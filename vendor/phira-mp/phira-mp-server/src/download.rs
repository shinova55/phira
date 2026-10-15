use super::User;
use anyhow::{anyhow, ensure, Result};
use phira_mp_common::{ChartSendWindow, ClientCommand, LocalChartInfo, ServerCommand, LOCAL_CHART_STREAM_CHUNK_BYTES, LOCAL_CHART_TRANSFER_TIMEOUT};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{mpsc, Notify},
    task::JoinHandle,
    time::{self, Instant},
};

struct Active {
    id: u32,
    total: u32,
    window: Arc<Mutex<ChartSendWindow>>,
    notify: Arc<Notify>,
    task: JoinHandle<()>,
}

impl Drop for Active {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Owned by the connection receiver, not by its producer task.
pub(super) struct Downloads {
    last_id: u32,
    active: Option<Active>,
    chunk_bytes: usize,
}

impl Default for Downloads {
    fn default() -> Self {
        Self {
            last_id: 0,
            active: None,
            chunk_bytes: LOCAL_CHART_STREAM_CHUNK_BYTES,
        }
    }
}

async fn selected(user: &User) -> Result<(Arc<crate::Room>, LocalChartInfo, Arc<Vec<u8>>)> {
    let room = user.room.read().await.clone().ok_or_else(|| anyhow!("no room"))?;
    ensure!(room.chart.read().await.as_ref().is_some_and(|c| c.id == -1), "no local chart selected");
    let guard = room.local_chart.read().await;
    let chart = guard.as_ref().ok_or_else(|| anyhow!("room chart missing"))?;
    let mut info = LocalChartInfo {
        size: chart.bytes.len() as u32,
        sha256: chart.sha256.clone(),
    };
    info.validate()?;
    let bytes = Arc::clone(&chart.bytes);
    drop(guard);
    Ok((room, info, bytes))
}

impl Downloads {
    #[cfg(test)]
    pub fn with_chunk_size(chunk_bytes: usize) -> Self {
        assert!(chunk_bytes == 256 * 1024 || chunk_bytes == 512 * 1024);
        Self {
            chunk_bytes,
            ..Self::default()
        }
    }
    pub fn cancel(&mut self) {
        self.active = None;
    }
    pub async fn handle(&mut self, user: &Arc<User>, sender: Arc<mpsc::Sender<ServerCommand>>, cmd: ClientCommand) -> Option<ServerCommand> {
        if self.active.as_ref().is_some_and(|a| a.task.is_finished()) {
            self.active = None;
        }
        match cmd {
            ClientCommand::GetLocalChartInfo { request_id } => Some(ServerCommand::LocalChartInfo {
                request_id,
                result: selected(user).await.map(|(_, info, _)| info).map_err(|e| e.to_string()),
            }),
            ClientCommand::CancelLocalChartDownload { transfer_id } => {
                if self.active.as_ref().is_some_and(|a| a.id == transfer_id) {
                    self.active = None;
                }
                None
            }
            ClientCommand::LocalChartDownloadAck { transfer_id, next_offset } => {
                let active = self.active.as_ref().filter(|a| a.id == transfer_id)?;
                let result = active.window.lock().unwrap().acknowledge(next_offset, active.total);
                if let Err(error) = result {
                    self.active = None;
                    return Some(ServerCommand::LocalChartDownloadError {
                        transfer_id,
                        message: error.to_string(),
                    });
                }
                active.notify.notify_one();
                None
            }
            ClientCommand::StartLocalChartDownload {
                transfer_id,
                expected_sha256,
            } => {
                let result: Result<()> = async {
                    ensure!(transfer_id > self.last_id, "stale transfer ID");
                    self.last_id = transfer_id;
                    ensure!(self.active.is_none(), "download already active");
                    let (room, info, bytes) = selected(user).await?;
                    ensure!(expected_sha256.eq_ignore_ascii_case(&info.sha256), "selected chart changed");
                    let window = Arc::new(Mutex::new(ChartSendWindow::default()));
                    let notify = Arc::new(Notify::new());
                    let chunk_bytes = self.chunk_bytes;
                    let task = tokio::spawn({
                        let window = Arc::clone(&window);
                        let notify = Arc::clone(&notify);
                        let user = Arc::downgrade(user);
                        let room = Arc::downgrade(&room);
                        async move {
                            let started = Instant::now();
                            let mut progress = Instant::now();
                            let mut previous_ack = 0;
                            let mut packets = 0u32;
                            let mut peak = 0;
                            let result: Result<()> = async {
                                loop {
                                    let user = user.upgrade().ok_or_else(|| anyhow!("disconnected"))?;
                                    let room = room.upgrade().ok_or_else(|| anyhow!("room destroyed"))?;
                                    ensure!(user.room.read().await.as_ref().is_some_and(|r| Arc::ptr_eq(r, &room)), "left room");
                                    ensure!(room.chart.read().await.as_ref().is_some_and(|c| c.id == -1), "chart changed");
                                    ensure!(room.local_chart.read().await.as_ref().is_some_and(|c| Arc::ptr_eq(&c.bytes, &bytes)), "chart changed");
                                    drop(room);
                                    drop(user);
                                    let (offset, ack, end) = {
                                        let w = window.lock().unwrap();
                                        (w.sent, w.acknowledged, w.next_end_with_chunk_size(info.size, chunk_bytes))
                                    };
                                    if ack != previous_ack {
                                        progress = Instant::now();
                                        previous_ack = ack;
                                    }
                                    ensure!(progress.elapsed() < LOCAL_CHART_TRANSFER_TIMEOUT, "chart download stalled");
                                    if ack == info.size {
                                        time::timeout(
                                            LOCAL_CHART_TRANSFER_TIMEOUT,
                                            sender.send(ServerCommand::LocalChartDownloadFinished { transfer_id }),
                                        )
                                        .await??;
                                        break;
                                    }
                                    if let Some(end) = end {
                                        // Reserve before committing the offset; no mutex spans backpressure.
                                        let permit = time::timeout(LOCAL_CHART_TRANSFER_TIMEOUT, sender.reserve()).await??;
                                        window.lock().unwrap().sent = end;
                                        peak = peak.max(end - ack);
                                        permit.send(ServerCommand::LocalChartDownloadChunk {
                                            transfer_id,
                                            offset,
                                            data: bytes[offset as usize..end as usize].to_vec(),
                                        });
                                        packets += 1;
                                        tokio::task::yield_now().await;
                                    } else {
                                        tokio::select! {
                                            _ = notify.notified() => {},
                                            _ = time::sleep(Duration::from_millis(250)) => {},
                                        }
                                    }
                                }
                                Ok(())
                            }
                            .await;
                            tracing::info!(
                                transfer_id,
                                packets,
                                peak_outstanding_bytes = peak,
                                seconds = started.elapsed().as_secs_f64(),
                                "chart stream ended"
                            );
                            if let Err(error) = result {
                                let _ = time::timeout(
                                    LOCAL_CHART_TRANSFER_TIMEOUT,
                                    sender.send(ServerCommand::LocalChartDownloadError {
                                        transfer_id,
                                        message: error.to_string(),
                                    }),
                                )
                                .await;
                            }
                        }
                    });
                    self.active = Some(Active {
                        id: transfer_id,
                        total: info.size,
                        window,
                        notify,
                        task,
                    });
                    Ok(())
                }
                .await;
                result.err().map(|e| ServerCommand::LocalChartDownloadError {
                    transfer_id,
                    message: e.to_string(),
                })
            }
            _ => unreachable!("not a download command"),
        }
    }
}
