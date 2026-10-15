pub mod resolver;

use anyhow::{Context, Error, Result};
use dashmap::DashMap;
use phira_mp_common::{
    ChartSendWindow, ClientCommand, ClientRoomState, JoinRoomResponse, JudgeEvent, LocalChartChunk, LocalChartInfo, Message, RoomId, RoomState,
    ServerCommand, Stream, TouchFrame, UserInfo, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, LOCAL_CHART_STREAM_CHUNK_BYTES,
    LOCAL_CHART_STREAM_MAX_CHUNK_BYTES, LOCAL_CHART_STREAM_WINDOW_BYTES, LOCAL_CHART_TRANSFER_TIMEOUT, MAX_LOCAL_CHART_BYTES, PROTOCOL_VERSION,
};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex, Notify, RwLock},
    task::JoinHandle,
    time,
};
use tracing::{error, trace, warn};

type Callback<T> = Mutex<Option<oneshot::Sender<T>>>;
type RCallback<T, E = String> = Mutex<Option<oneshot::Sender<Result<T, E>>>>;

pub const TIMEOUT: Duration = Duration::from_secs(7);

pub struct LivePlayer {
    pub touch_frames: Mutex<Vec<TouchFrame>>,
    pub judge_events: Mutex<Vec<JudgeEvent>>,
}

impl Default for LivePlayer {
    fn default() -> Self {
        Self::new()
    }
}

impl LivePlayer {
    pub fn new() -> Self {
        Self {
            touch_frames: Mutex::default(),
            judge_events: Mutex::default(),
        }
    }
}

struct State {
    upload: std::sync::Mutex<Option<(u32, mpsc::Sender<ServerCommand>)>>,
    download: std::sync::Mutex<Option<(u32, mpsc::Sender<ServerCommand>)>>,
    next_transfer: AtomicU32,
    chart_epoch: AtomicU64,
    cb_local_chart_info: Mutex<Option<(u32, oneshot::Sender<Result<LocalChartInfo, String>>)>>,
    delay: Mutex<Option<Duration>>,
    ping_notify: Notify,

    me: RwLock<Option<UserInfo>>,
    room: RwLock<Option<ClientRoomState>>,

    cb_authenticate: RCallback<(UserInfo, Option<ClientRoomState>)>,
    cb_chat: RCallback<()>,
    cb_create_room: RCallback<()>,
    cb_join_room: RCallback<JoinRoomResponse>,
    cb_leave_room: RCallback<()>,
    cb_lock_room: RCallback<()>,
    cb_cycle_room: RCallback<()>,
    cb_select_chart: RCallback<()>,
    cb_request_start: RCallback<()>,
    cb_ready: RCallback<()>,
    cb_cancel_ready: RCallback<()>,
    cb_played: RCallback<()>,
    cb_abort: RCallback<()>,
    cb_start_local_upload: RCallback<()>,
    cb_local_upload_chunk: RCallback<()>,
    cb_finish_local_upload: RCallback<()>,
    cb_fetch_local_chunk: RCallback<LocalChartChunk>,
    cb_played_local: RCallback<()>,

    live_players: DashMap<i32, Arc<LivePlayer>>,
    messages: Mutex<Vec<Message>>,
}

impl State {
    pub fn live_player(&self, player: i32) -> Arc<LivePlayer> {
        Arc::clone(&self.live_players.entry(player).or_insert_with(|| Arc::new(LivePlayer::new())))
    }
}

pub struct Client {
    state: Arc<State>,

    stream: Arc<Stream<ClientCommand, ServerCommand>>,

    ping_fail_count: Arc<AtomicU8>,
    ping_task_handle: JoinHandle<()>,
}

impl Client {
    pub async fn new(stream: TcpStream) -> Result<Self> {
        stream.set_nodelay(true)?;

        let state = Arc::new(State {
            upload: std::sync::Mutex::new(None),
            download: std::sync::Mutex::new(None),
            next_transfer: AtomicU32::new(1),
            chart_epoch: AtomicU64::new(0),
            cb_local_chart_info: Mutex::default(),
            delay: Mutex::default(),
            ping_notify: Notify::new(),

            me: RwLock::default(),
            room: RwLock::default(),

            cb_authenticate: Callback::default(),
            cb_chat: Callback::default(),
            cb_create_room: Callback::default(),
            cb_join_room: Callback::default(),
            cb_leave_room: Callback::default(),
            cb_lock_room: Callback::default(),
            cb_cycle_room: Callback::default(),
            cb_select_chart: Callback::default(),
            cb_request_start: Callback::default(),
            cb_ready: Callback::default(),
            cb_cancel_ready: Callback::default(),
            cb_played: Callback::default(),
            cb_abort: Callback::default(),
            cb_start_local_upload: Callback::default(),
            cb_local_upload_chunk: Callback::default(),
            cb_finish_local_upload: Callback::default(),
            cb_fetch_local_chunk: Callback::default(),
            cb_played_local: Callback::default(),

            live_players: DashMap::new(),
            messages: Mutex::default(),
        });
        let stream = Arc::new(
            Stream::new(
                Some(PROTOCOL_VERSION),
                stream,
                Box::new({
                    let state = Arc::clone(&state);
                    move |_send_tx, cmd| process(Arc::clone(&state), cmd)
                }),
            )
            .await?,
        );

        let ping_fail_count = Arc::new(AtomicU8::default());
        let ping_task_handle = tokio::spawn({
            let ping_fail_count = Arc::clone(&ping_fail_count);
            let state = Arc::clone(&state);
            let stream = Arc::clone(&stream);
            async move {
                loop {
                    time::sleep(HEARTBEAT_INTERVAL).await;

                    let start = Instant::now();
                    if let Err(err) = stream.send(ClientCommand::Ping).await {
                        error!("failed to send heartbeat: {err:?}");
                    } else if time::timeout(HEARTBEAT_TIMEOUT, state.ping_notify.notified()).await.is_err() {
                        warn!("heartbeat timeout");
                        ping_fail_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        ping_fail_count.store(0, Ordering::SeqCst);
                    }
                    let delay = start.elapsed();
                    *state.delay.lock().await = Some(delay);
                    trace!("sent heartbeat, delay: {delay:?}");
                }
            }
        });

        Ok(Self {
            state,

            stream,

            ping_fail_count,
            ping_task_handle,
        })
    }

    pub async fn from_address(addr: &str) -> Result<Self> {
        let auth: http::uri::Authority = addr.parse().context("Invalid server address")?;
        let resolved_addr = resolver::resolve(&auth).await?;
        let stream = TcpStream::connect(resolved_addr).await?;
        Self::new(stream).await
    }

    pub fn me(&self) -> Option<UserInfo> {
        self.state.me.blocking_read().clone()
    }

    pub fn user_name(&self, id: i32) -> String {
        self.user_name_opt(id).unwrap_or_else(|| "?".to_owned())
    }

    pub fn user_name_opt(&self, id: i32) -> Option<String> {
        self.state
            .room
            .blocking_read()
            .as_ref()
            .and_then(|it| it.users.get(&id).map(|it| it.name.clone()))
    }

    pub fn blocking_take_messages(&self) -> Vec<Message> {
        self.state.messages.blocking_lock().drain(..).collect()
    }

    pub fn blocking_state(&self) -> Option<ClientRoomState> {
        self.state.room.blocking_read().clone()
    }

    pub fn blocking_room_id(&self) -> Option<RoomId> {
        self.state.room.blocking_read().as_ref().map(|it| it.id.clone())
    }

    pub fn blocking_room_state(&self) -> Option<RoomState> {
        self.state.room.blocking_read().as_ref().map(|it| it.state)
    }

    pub async fn room_state(&self) -> Option<RoomState> {
        self.state.room.read().await.as_ref().map(|it| it.state)
    }

    pub fn blocking_is_host(&self) -> Option<bool> {
        self.state.room.blocking_read().as_ref().map(|it| it.is_host)
    }

    pub fn blocking_is_ready(&self) -> Option<bool> {
        self.state.room.blocking_read().as_ref().map(|it| it.is_ready)
    }

    pub async fn ping(&self) -> Result<Duration> {
        let start = Instant::now();
        self.stream.send(ClientCommand::Ping).await?;
        time::timeout(HEARTBEAT_TIMEOUT, self.state.ping_notify.notified())
            .await
            .context("heartbeat timeout")?;
        let delay = start.elapsed();
        *self.state.delay.lock().await = Some(delay);
        Ok(delay)
    }

    pub fn delay(&self) -> Option<Duration> {
        *self.state.delay.blocking_lock()
    }

    async fn rcall<R>(&self, payload: ClientCommand, cb: &RCallback<R>) -> Result<R> {
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = cb.lock().await;
            anyhow::ensure!(guard.as_ref().is_none_or(|sender| sender.is_closed()), "RPC already pending");
            *guard = Some(tx);
        }
        self.stream.send(payload).await?;
        time::timeout(TIMEOUT, rx).await.context("timeout")??.map_err(Error::msg)
    }

    #[inline]
    pub async fn authenticate(&self, token: impl Into<String>) -> Result<()> {
        let (me, room) = self
            .rcall(
                ClientCommand::Authenticate {
                    token: token.into().try_into()?,
                },
                &self.state.cb_authenticate,
            )
            .await?;
        *self.state.me.write().await = Some(me);
        *self.state.room.write().await = room;
        Ok(())
    }

    #[inline]
    pub async fn chat(&self, message: String) -> Result<()> {
        self.rcall(
            ClientCommand::Chat {
                message: message.try_into()?,
            },
            &self.state.cb_chat,
        )
        .await
    }

    #[inline]
    pub async fn create_room(&self, id: RoomId) -> Result<()> {
        self.rcall(ClientCommand::CreateRoom { id: id.clone() }, &self.state.cb_create_room)
            .await?;
        let me = self.state.me.read().await.clone().unwrap();
        *self.state.room.write().await = Some(ClientRoomState {
            id,
            state: RoomState::default(),
            live: false,
            locked: false,
            cycle: false,
            is_host: true,
            is_ready: false,
            users: std::iter::once((me.id, me)).collect(),
        });
        Ok(())
    }

    #[inline]
    pub async fn join_room(&self, id: RoomId, monitor: bool) -> Result<()> {
        let resp = self
            .rcall(ClientCommand::JoinRoom { id: id.clone(), monitor }, &self.state.cb_join_room)
            .await?;
        *self.state.room.write().await = Some(ClientRoomState {
            id,
            state: resp.state,
            live: resp.live,
            locked: false,
            cycle: false,
            is_host: false,
            is_ready: false,
            users: resp.users.into_iter().map(|it| (it.id, it)).collect(),
        });
        Ok(())
    }

    #[inline]
    pub async fn leave_room(&self) -> Result<()> {
        self.state.chart_epoch.fetch_add(1, Ordering::SeqCst);
        self.state.download.lock().unwrap().take();
        self.state.upload.lock().unwrap().take();
        self.rcall(ClientCommand::LeaveRoom, &self.state.cb_leave_room).await?;
        *self.state.room.write().await = None;
        Ok(())
    }

    #[inline]
    pub async fn lock_room(&self, lock: bool) -> Result<()> {
        self.rcall(ClientCommand::LockRoom { lock }, &self.state.cb_lock_room).await
    }

    #[inline]
    pub async fn cycle_room(&self, cycle: bool) -> Result<()> {
        self.rcall(ClientCommand::CycleRoom { cycle }, &self.state.cb_cycle_room).await
    }

    #[inline]
    pub async fn select_chart(&self, id: i32) -> Result<()> {
        self.rcall(ClientCommand::SelectChart { id }, &self.state.cb_select_chart).await
    }

    /// Upload a private chart ZIP to the current self-hosted multiplayer room.
    /// The server only accepts this from the room host while selecting charts.
    pub async fn upload_local_chart(&self, name: String, bytes: Vec<u8>) -> Result<()> {
        anyhow::ensure!(!bytes.is_empty() && bytes.len() <= MAX_LOCAL_CHART_BYTES, "local chart ZIP exceeds 32 MiB");
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        let id = self
            .state
            .next_transfer
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_add(1))
            .map_err(|_| Error::msg("transfer IDs exhausted; reconnect"))?;
        let (tx, mut rx) = mpsc::channel(8);
        {
            let mut active = self.state.upload.lock().unwrap();
            anyhow::ensure!(active.is_none(), "local chart upload already active");
            *active = Some((id, tx));
        }
        struct Cleanup<'a>(&'a Client, u32);
        impl Drop for Cleanup<'_> {
            fn drop(&mut self) {
                let mut active = self.0.state.upload.lock().unwrap();
                if active.as_ref().is_some_and(|(id, _)| *id == self.1) {
                    active.take();
                }
                let _ = self.0.stream.try_send(ClientCommand::CancelLocalChartUpload { transfer_id: self.1 });
            }
        }
        let _cleanup = Cleanup(self, id);
        self.stream
            .send(ClientCommand::StartLocalChartUpload {
                transfer_id: id,
                name,
                size: bytes.len() as u32,
                sha256,
            })
            .await?;
        let started = Instant::now();
        let mut progress = Instant::now();
        let mut window = ChartSendWindow::default();
        let mut accepted = false;
        let mut finishing = false;
        let mut peak = 0;
        loop {
            anyhow::ensure!(!self.stream.is_closed(), "connection closed during chart upload");
            anyhow::ensure!(progress.elapsed() < if accepted { LOCAL_CHART_TRANSFER_TIMEOUT } else { TIMEOUT }, "chart upload stalled");
            if accepted && !finishing {
                if let Some(end) = window.next_end(bytes.len() as u32) {
                    time::timeout(
                        LOCAL_CHART_TRANSFER_TIMEOUT,
                        self.stream.send(ClientCommand::LocalChartUploadData {
                            transfer_id: id,
                            offset: window.sent,
                            data: bytes[window.sent as usize..end as usize].to_vec(),
                        }),
                    )
                    .await??;
                    window.sent = end;
                    peak = peak.max(window.sent - window.acknowledged);
                    continue;
                }
                if window.acknowledged == bytes.len() as u32 {
                    self.stream.send(ClientCommand::FinishLocalChartUpload { transfer_id: id }).await?;
                    finishing = true;
                }
            }
            let packet = tokio::select! {
                packet = rx.recv() => packet.context("chart upload cancelled")?,
                _ = time::sleep(Duration::from_millis(250)) => continue,
            };
            match packet {
                ServerCommand::LocalChartUploadProgress { result, .. } => {
                    let offset = result.map_err(Error::msg)?;
                    if !accepted {
                        anyhow::ensure!(offset == 0, "invalid upload acceptance");
                        accepted = true;
                        progress = Instant::now();
                    } else {
                        let old = window.acknowledged;
                        window.acknowledge(offset, bytes.len() as u32)?;
                        if old != offset {
                            progress = Instant::now();
                        }
                    }
                }
                ServerCommand::LocalChartUploadVerified { result, .. } => {
                    anyhow::ensure!(finishing, "premature upload completion");
                    result.map_err(Error::msg)?;
                    tracing::info!(seconds = started.elapsed().as_secs_f64(), peak_outstanding_bytes = peak, "chart upload complete");
                    return Ok(());
                }
                _ => anyhow::bail!("unexpected upload packet"),
            }
        }
    }

    pub fn chart_epoch(&self) -> u64 {
        self.state.chart_epoch.load(Ordering::SeqCst)
    }

    pub async fn local_chart_info(&self) -> Result<LocalChartInfo> {
        let started = Instant::now();
        let epoch = self.chart_epoch();
        let id = self
            .state
            .next_transfer
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_add(1))
            .map_err(|_| Error::msg("transfer IDs exhausted; reconnect"))?;
        let (tx, rx) = oneshot::channel();
        {
            let mut callback = self.state.cb_local_chart_info.lock().await;
            anyhow::ensure!(callback.as_ref().is_none_or(|(_, sender)| sender.is_closed()), "metadata request already pending");
            *callback = Some((id, tx));
        }
        self.stream.send(ClientCommand::GetLocalChartInfo { request_id: id }).await?;
        let mut info = time::timeout(TIMEOUT, rx).await.context("chart metadata timeout")??.map_err(Error::msg)?;
        anyhow::ensure!(self.chart_epoch() == epoch, "chart changed during metadata request");
        info.validate()?;
        tracing::info!(milliseconds = started.elapsed().as_millis(), size = info.size, "chart metadata received");
        Ok(info)
    }

    pub async fn download_local_chart(&self) -> Result<(Vec<u8>, String)> {
        self.download_local_chart_with_info(self.local_chart_info().await?).await
    }

    /// One bounded, unsolicited packet stream per connection, independent of RPC callbacks.
    pub async fn download_local_chart_with_info(&self, mut info: LocalChartInfo) -> Result<(Vec<u8>, String)> {
        info.validate()?;
        let id = self
            .state
            .next_transfer
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_add(1))
            .map_err(|_| Error::msg("transfer IDs exhausted; reconnect"))?;
        let (tx, mut rx) = mpsc::channel(LOCAL_CHART_STREAM_WINDOW_BYTES / LOCAL_CHART_STREAM_CHUNK_BYTES + 1);
        {
            let mut active = self.state.download.lock().unwrap();
            anyhow::ensure!(active.is_none(), "local chart download already active");
            *active = Some((id, tx));
        }
        struct Cleanup<'a>(&'a Client, u32);
        impl Drop for Cleanup<'_> {
            fn drop(&mut self) {
                let mut active = self.0.state.download.lock().unwrap();
                if active.as_ref().is_some_and(|(id, _)| *id == self.1) {
                    active.take();
                }
                let _ = self.0.stream.try_send(ClientCommand::CancelLocalChartDownload { transfer_id: self.1 });
            }
        }
        let _cleanup = Cleanup(self, id);
        let epoch = self.chart_epoch();
        self.stream
            .send(ClientCommand::StartLocalChartDownload {
                transfer_id: id,
                expected_sha256: info.sha256.clone(),
            })
            .await?;
        let started = Instant::now();
        let mut progress = Instant::now();
        let mut bytes = Vec::with_capacity(info.size as usize);
        let mut acknowledged = 0usize;
        loop {
            anyhow::ensure!(self.chart_epoch() == epoch, "selected chart or room changed");
            anyhow::ensure!(!self.stream.is_closed(), "multiplayer connection closed during chart transfer");
            anyhow::ensure!(
                progress.elapsed() < if bytes.is_empty() { TIMEOUT } else { LOCAL_CHART_TRANSFER_TIMEOUT },
                "local chart download stalled"
            );
            let packet = tokio::select! {
                packet = rx.recv() => packet.context("local chart transfer cancelled")?,
                _ = time::sleep(Duration::from_millis(250)) => continue,
            };
            match packet {
                ServerCommand::LocalChartDownloadChunk { offset, data, .. } => {
                    anyhow::ensure!(offset as usize == bytes.len(), "invalid local chart offset");
                    let remaining = info.size as usize - bytes.len();
                    anyhow::ensure!(
                        !data.is_empty()
                            && (data.len() == remaining.min(LOCAL_CHART_STREAM_CHUNK_BYTES)
                                || data.len() == remaining.min(LOCAL_CHART_STREAM_MAX_CHUNK_BYTES)),
                        "invalid local chart chunk size"
                    );
                    if bytes.is_empty() {
                        tracing::info!(milliseconds = started.elapsed().as_millis(), "chart stream started");
                    }
                    bytes.extend_from_slice(&data);
                    progress = Instant::now();
                    if bytes.len() - acknowledged >= LOCAL_CHART_STREAM_WINDOW_BYTES / 2 || bytes.len() == info.size as usize {
                        self.stream
                            .send(ClientCommand::LocalChartDownloadAck {
                                transfer_id: id,
                                next_offset: bytes.len() as u32,
                            })
                            .await?;
                        acknowledged = bytes.len();
                    }
                }
                ServerCommand::LocalChartDownloadFinished { .. } => {
                    anyhow::ensure!(bytes.len() == info.size as usize, "incomplete local chart download");
                    let verify = Instant::now();
                    let digest = format!("{:x}", Sha256::digest(&bytes));
                    anyhow::ensure!(digest == info.sha256, "local chart SHA-256 mismatch");
                    tracing::info!(
                        seconds = started.elapsed().as_secs_f64(),
                        mib_per_second = bytes.len() as f64 / 1048576.0 / started.elapsed().as_secs_f64(),
                        verify_ms = verify.elapsed().as_millis(),
                        "chart download verified"
                    );
                    return Ok((bytes, digest));
                }
                ServerCommand::LocalChartDownloadError { message, .. } => anyhow::bail!(message),
                _ => anyhow::bail!("unexpected local chart packet"),
            }
        }
    }

    pub async fn played_local(&self, score: i32, accuracy: f32, full_combo: bool) -> Result<()> {
        self.rcall(ClientCommand::PlayedLocal { score, accuracy, full_combo }, &self.state.cb_played_local)
            .await
    }

    #[inline]
    pub async fn request_start(&self) -> Result<()> {
        self.rcall(ClientCommand::RequestStart, &self.state.cb_request_start).await?;
        self.state.room.write().await.as_mut().unwrap().is_ready = true;
        Ok(())
    }

    pub async fn request_start_local_chart(&self, expected_sha256: String) -> Result<()> {
        self.rcall(ClientCommand::RequestStartLocalChart { expected_sha256 }, &self.state.cb_request_start)
            .await?;
        if let Some(room) = self.state.room.write().await.as_mut() {
            room.is_ready = true;
        }
        Ok(())
    }

    pub async fn ready_local_chart(&self, expected_sha256: String) -> Result<()> {
        self.rcall(ClientCommand::ReadyLocalChart { expected_sha256 }, &self.state.cb_ready)
            .await?;
        if let Some(room) = self.state.room.write().await.as_mut() {
            room.is_ready = true;
        }
        Ok(())
    }

    #[inline]
    pub async fn ready(&self) -> Result<()> {
        self.rcall(ClientCommand::Ready, &self.state.cb_ready).await?;
        self.state.room.write().await.as_mut().unwrap().is_ready = true;
        Ok(())
    }

    #[inline]
    pub async fn cancel_ready(&self) -> Result<()> {
        self.rcall(ClientCommand::CancelReady, &self.state.cb_cancel_ready).await?;
        self.state.room.write().await.as_mut().unwrap().is_ready = false;
        Ok(())
    }

    #[inline]
    pub async fn played(&self, id: i32) -> Result<()> {
        self.rcall(ClientCommand::Played { id }, &self.state.cb_played).await
    }

    #[inline]
    pub async fn abort(&self) -> Result<()> {
        self.rcall(ClientCommand::Abort, &self.state.cb_abort).await
    }

    pub fn ping_fail_count(&self) -> u8 {
        self.ping_fail_count.load(Ordering::Relaxed)
    }

    pub async fn send(&self, payload: ClientCommand) -> Result<()> {
        self.stream.send(payload).await
    }

    pub fn blocking_send(&self, payload: ClientCommand) -> Result<()> {
        self.stream.blocking_send(payload)
    }

    #[inline]
    pub fn live_player(&self, player: i32) -> Arc<LivePlayer> {
        self.state.live_player(player)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.ping_task_handle.abort();
    }
}

async fn process(state: Arc<State>, cmd: ServerCommand) {
    async fn cb<T>(cb: &Callback<T>, res: T) {
        if let Some(tx) = cb.lock().await.take() {
            let _ = tx.send(res);
        }
    }
    match cmd {
        packet @ (ServerCommand::LocalChartUploadProgress { .. } | ServerCommand::LocalChartUploadVerified { .. }) => {
            let id = match &packet {
                ServerCommand::LocalChartUploadProgress { transfer_id, .. } | ServerCommand::LocalChartUploadVerified { transfer_id, .. } => {
                    *transfer_id
                }
                _ => unreachable!(),
            };
            let mut active = state.upload.lock().unwrap();
            if let Some((active_id, tx)) = active.as_ref() {
                if id == *active_id && tx.try_send(packet).is_err() {
                    active.take();
                }
            }
        }
        ServerCommand::LocalChartInfo { request_id, result } => {
            let mut callback = state.cb_local_chart_info.lock().await;
            if callback.as_ref().is_some_and(|(id, _)| *id == request_id) {
                if let Some((_, tx)) = callback.take() {
                    let _ = tx.send(result);
                }
            }
        }
        packet @ (ServerCommand::LocalChartDownloadChunk { .. }
        | ServerCommand::LocalChartDownloadFinished { .. }
        | ServerCommand::LocalChartDownloadError { .. }) => {
            let id = match &packet {
                ServerCommand::LocalChartDownloadChunk { transfer_id, .. }
                | ServerCommand::LocalChartDownloadFinished { transfer_id }
                | ServerCommand::LocalChartDownloadError { transfer_id, .. } => *transfer_id,
                _ => unreachable!(),
            };
            let mut active = state.download.lock().unwrap();
            if let Some((active_id, tx)) = active.as_ref() {
                if id == *active_id && tx.try_send(packet).is_err() {
                    active.take();
                }
            }
        }
        ServerCommand::Pong => {
            state.ping_notify.notify_one();
        }
        ServerCommand::Authenticate(res) => {
            cb(&state.cb_authenticate, res).await;
        }
        ServerCommand::Chat(res) => {
            cb(&state.cb_chat, res).await;
        }
        ServerCommand::Touches { player, frames } => {
            state.live_player(player).touch_frames.lock().await.extend(frames.iter().cloned());
        }
        ServerCommand::Judges { player, judges } => {
            state.live_player(player).judge_events.lock().await.extend(judges.iter().cloned());
        }
        ServerCommand::Message(msg) => {
            if matches!(msg, Message::SelectChart { .. } | Message::SelectLocalChart { .. }) {
                state.chart_epoch.fetch_add(1, Ordering::SeqCst);
                state.download.lock().unwrap().take();
            }
            match msg {
                Message::LockRoom { lock } => {
                    state.room.write().await.as_mut().unwrap().locked = lock;
                }
                Message::CycleRoom { cycle } => {
                    state.room.write().await.as_mut().unwrap().cycle = cycle;
                }
                Message::LeaveRoom { user, .. } => {
                    state.room.write().await.as_mut().unwrap().users.remove(&user);
                }
                _ => {}
            }
            state.messages.lock().await.push(msg);
        }
        ServerCommand::ChangeState(room) => {
            state.live_players.clear();
            let mut guard = state.room.write().await;
            let state = guard.as_mut().unwrap();
            state.state = room;
            state.is_ready = state.is_host;
        }
        ServerCommand::ChangeHost(me_is_host) => {
            state.room.write().await.as_mut().unwrap().is_host = me_is_host;
        }

        ServerCommand::CreateRoom(res) => {
            cb(&state.cb_create_room, res).await;
        }
        ServerCommand::JoinRoom(res) => {
            cb(&state.cb_join_room, res).await;
        }
        ServerCommand::OnJoinRoom(user) => {
            if let Some(room) = state.room.write().await.as_mut() {
                room.live |= user.monitor;
                room.users.insert(user.id, user);
            }
        }
        ServerCommand::LeaveRoom(res) => {
            cb(&state.cb_leave_room, res).await;
        }
        ServerCommand::LockRoom(res) => {
            cb(&state.cb_lock_room, res).await;
        }
        ServerCommand::CycleRoom(res) => {
            cb(&state.cb_cycle_room, res).await;
        }
        ServerCommand::SelectChart(res) => {
            cb(&state.cb_select_chart, res).await;
        }
        ServerCommand::RequestStart(res) => {
            cb(&state.cb_request_start, res).await;
        }
        ServerCommand::Ready(res) => {
            cb(&state.cb_ready, res).await;
        }
        ServerCommand::CancelReady(res) => {
            cb(&state.cb_cancel_ready, res).await;
        }
        ServerCommand::Played(res) => {
            cb(&state.cb_played, res).await;
        }
        ServerCommand::Abort(res) => {
            cb(&state.cb_abort, res).await;
        }
        ServerCommand::StartLocalUpload(res) => cb(&state.cb_start_local_upload, res).await,
        ServerCommand::LocalUploadChunk(res) => cb(&state.cb_local_upload_chunk, res).await,
        ServerCommand::FinishLocalUpload(res) => cb(&state.cb_finish_local_upload, res).await,
        ServerCommand::FetchLocalChunk(res) => cb(&state.cb_fetch_local_chunk, res).await,
        ServerCommand::PlayedLocal(res) => cb(&state.cb_played_local, res).await,
    }
}
