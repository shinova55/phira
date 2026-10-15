use super::User;
use crate::{Chart, InternalRoomState, LocalChartData, LocalChartUpload, Room};
use anyhow::{anyhow, ensure, Result};
use phira_mp_common::{
    ClientCommand, LocalChartInfo, Message, ServerCommand, LOCAL_CHART_STREAM_CHUNK_BYTES, LOCAL_CHART_STREAM_WINDOW_BYTES,
    LOCAL_CHART_TRANSFER_TIMEOUT,
};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex, Weak};
use tokio::{
    task::JoinHandle,
    time::{self, Instant},
};

struct Active {
    id: u32,
    room: Weak<Room>,
    upload: LocalChartUpload,
    acknowledged: usize,
    progress: Instant,
}

/// Partial archives belong to the connection, so disconnect frees them even
/// when the room survives for reconnecting users.
pub(super) struct Uploads {
    last_id: u32,
    active: Arc<Mutex<Option<Active>>>,
    watchdog: JoinHandle<()>,
}

impl Default for Uploads {
    fn default() -> Self {
        let active = Arc::new(Mutex::new(None::<Active>));
        let weak = Arc::downgrade(&active);
        let watchdog = tokio::spawn(async move {
            loop {
                time::sleep(std::time::Duration::from_secs(1)).await;
                let Some(active) = weak.upgrade() else { break };
                let mut guard = active.lock().unwrap();
                if guard.as_ref().is_some_and(|a| a.progress.elapsed() >= LOCAL_CHART_TRANSFER_TIMEOUT) {
                    guard.take();
                }
            }
        });
        Self {
            last_id: 0,
            active,
            watchdog,
        }
    }
}

impl Drop for Uploads {
    fn drop(&mut self) {
        self.watchdog.abort();
    }
}

impl Uploads {
    pub fn cancel(&mut self) {
        self.active.lock().unwrap().take();
    }

    pub async fn handle(&mut self, user: &Arc<User>, cmd: ClientCommand) -> Option<ServerCommand> {
        let id = match &cmd {
            ClientCommand::StartLocalChartUpload { transfer_id, .. }
            | ClientCommand::LocalChartUploadData { transfer_id, .. }
            | ClientCommand::FinishLocalChartUpload { transfer_id }
            | ClientCommand::CancelLocalChartUpload { transfer_id } => *transfer_id,
            _ => unreachable!(),
        };
        if matches!(cmd, ClientCommand::CancelLocalChartUpload { .. }) {
            let mut guard = self.active.lock().unwrap();
            if guard.as_ref().is_some_and(|a| a.id == id) {
                guard.take();
            }
            return None;
        }
        let finishing = matches!(cmd, ClientCommand::FinishLocalChartUpload { .. });
        let result: Result<Option<u32>> = async {
            let room = user.room.read().await.clone().ok_or_else(|| anyhow!("no room"))?;
            ensure!(matches!(*room.state.read().await, InternalRoomState::SelectChart), "invalid state");
            room.check_host(user).await?;
            match cmd {
                ClientCommand::StartLocalChartUpload { name, size, sha256, .. } => {
                    ensure!(id > self.last_id, "stale upload ID");
                    self.last_id = id;
                    let mut info = LocalChartInfo { size, sha256 };
                    info.validate()?;
                    ensure!(!name.trim().is_empty() && name.len() <= 128, "invalid chart name");
                    let mut guard = self.active.lock().unwrap();
                    ensure!(guard.is_none(), "upload already active");
                    *guard = Some(Active {
                        id,
                        room: Arc::downgrade(&room),
                        acknowledged: 0,
                        progress: Instant::now(),
                        upload: LocalChartUpload {
                            name,
                            size,
                            sha256: info.sha256,
                            bytes: Vec::with_capacity(size as usize),
                        },
                    });
                    Ok(Some(0))
                }
                ClientCommand::LocalChartUploadData { offset, data, .. } => {
                    let mut guard = self.active.lock().unwrap();
                    let active = guard.as_mut().filter(|a| a.id == id).ok_or_else(|| anyhow!("no matching upload"))?;
                    ensure!(active.room.ptr_eq(&Arc::downgrade(&room)), "room changed");
                    ensure!(active.progress.elapsed() < LOCAL_CHART_TRANSFER_TIMEOUT, "upload stalled");
                    let upload = &mut active.upload;
                    ensure!(offset as usize == upload.bytes.len(), "invalid upload offset");
                    ensure!(
                        data.len() == (upload.size as usize - upload.bytes.len()).min(LOCAL_CHART_STREAM_CHUNK_BYTES) && !data.is_empty(),
                        "invalid upload chunk size"
                    );
                    upload.bytes.extend_from_slice(&data);
                    active.progress = Instant::now();
                    if upload.bytes.len() - active.acknowledged >= LOCAL_CHART_STREAM_WINDOW_BYTES / 2 || upload.bytes.len() == upload.size as usize {
                        active.acknowledged = upload.bytes.len();
                        Ok(Some(upload.bytes.len() as u32))
                    } else {
                        Ok(None)
                    }
                }
                ClientCommand::FinishLocalChartUpload { .. } => {
                    let active = {
                        let mut guard = self.active.lock().unwrap();
                        ensure!(guard.as_ref().is_some_and(|a| a.id == id), "no matching upload");
                        guard.take().unwrap()
                    };
                    ensure!(active.room.ptr_eq(&Arc::downgrade(&room)), "room changed");
                    let upload = active.upload;
                    ensure!(upload.bytes.len() == upload.size as usize, "incomplete upload");
                    let started = Instant::now();
                    ensure!(format!("{:x}", Sha256::digest(&upload.bytes)) == upload.sha256, "upload checksum mismatch");
                    tracing::info!(transfer_id = id, verify_ms = started.elapsed().as_millis(), "chart upload verified");
                    *room.local_chart.write().await = Some(LocalChartData {
                        sha256: upload.sha256,
                        bytes: Arc::new(upload.bytes),
                    });
                    *room.chart.write().await = Some(Chart {
                        id: -1,
                        name: upload.name.clone(),
                    });
                    room.send(Message::SelectLocalChart {
                        user: user.id,
                        name: upload.name,
                    })
                    .await;
                    room.on_state_change().await;
                    Ok(Some(0))
                }
                _ => unreachable!(),
            }
        }
        .await;
        if result.is_err() {
            let mut guard = self.active.lock().unwrap();
            if guard.as_ref().is_some_and(|a| a.id == id) {
                guard.take();
            }
        }
        if finishing {
            Some(ServerCommand::LocalChartUploadVerified {
                transfer_id: id,
                result: result.map(|_| ()).map_err(|e| e.to_string()),
            })
        } else {
            match result {
                Ok(None) => None,
                result => Some(ServerCommand::LocalChartUploadProgress {
                    transfer_id: id,
                    result: result.map(|v| v.unwrap()).map_err(|e| e.to_string()),
                }),
            }
        }
    }
}
