use super::*;
use crate::LocalChartData;
use phira_mp_client::Client;
use phira_mp_common::{LocalChartInfo, LOCAL_CHART_STREAM_CHUNK_BYTES, LOCAL_CHART_STREAM_WINDOW_BYTES, PROTOCOL_VERSION};
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, sync::mpsc};

struct LatencyProxy(Vec<JoinHandle<()>>);
impl Drop for LatencyProxy {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

// A bounded, pipelined delay line in each direction. Unlike sleeping per
// packet, it delays bytes without serializing their propagation latency.
async fn delay_bytes(mut read: tokio::net::tcp::OwnedReadHalf, mut write: tokio::net::tcp::OwnedWriteHalf, delay: Duration) {
    use std::collections::VecDeque;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut pending = VecDeque::<(time::Instant, Vec<u8>)>::new();
    let mut buffer = vec![0; 64 * 1024];
    let mut eof = false;
    loop {
        if eof && pending.is_empty() {
            let _ = write.shutdown().await;
            break;
        }
        let deadline = pending
            .front()
            .map(|p| p.0)
            .unwrap_or_else(|| time::Instant::now() + Duration::from_secs(3600));
        tokio::select! {
            result = read.read(&mut buffer), if !eof && pending.len() < 128 => {
                match result {
                    Ok(0) => eof = true,
                    Ok(n) => pending.push_back((time::Instant::now() + delay, buffer[..n].to_vec())),
                    Err(_) => break,
                }
            }
            _ = time::sleep_until(deadline), if !pending.is_empty() => {
                let (_, bytes) = pending.pop_front().unwrap();
                if write.write_all(&bytes).await.is_err() { break; }
            }
        }
    }
}

async fn sockets(rtt: Duration) -> (TcpStream, TcpStream, LatencyProxy) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    if rtt.is_zero() {
        return (socket, server, LatencyProxy(Vec::new()));
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
    let (proxy, _) = listener.accept().await.unwrap();
    proxy.set_nodelay(true).unwrap();
    socket.set_nodelay(true).unwrap();
    let (ar, aw) = proxy.into_split();
    let (br, bw) = socket.into_split();
    let tasks = vec![tokio::spawn(delay_bytes(ar, bw, rtt / 2)), tokio::spawn(delay_bytes(br, aw, rtt / 2))];
    (client, server, LatencyProxy(tasks))
}

fn fixture(bytes: Vec<u8>) -> (Arc<User>, Arc<Room>) {
    let (lost_con_tx, _) = mpsc::channel(8);
    let server = Arc::new(ServerState {
        config: crate::ServerConfig::default(),
        sessions: Default::default(),
        users: Default::default(),
        rooms: Default::default(),
        lost_con_tx,
    });
    let user = Arc::new(User::new(1, "test".into(), Language::default(), server));
    let room = Arc::new(Room::new("test".to_string().try_into().unwrap(), Arc::downgrade(&user)));
    *user.room.try_write().unwrap() = Some(Arc::clone(&room));
    *room.chart.try_write().unwrap() = Some(Chart {
        id: -1,
        name: "chart".into(),
    });
    *room.local_chart.try_write().unwrap() = Some(LocalChartData {
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        bytes: Arc::new(bytes),
    });
    (user, room)
}

/// Authentication is deliberately replaced by a pre-authenticated fixture;
/// framing, serializers, client state and server transfer handlers are real.
async fn connection(user: Arc<User>) -> (Arc<Client>, Stream<ServerCommand, ClientCommand>) {
    let (client, server, _) = connection_with_rtt(user, Duration::ZERO).await;
    (client, server)
}

async fn connection_with_rtt(user: Arc<User>, rtt: Duration) -> (Arc<Client>, Stream<ServerCommand, ClientCommand>, LatencyProxy) {
    let (socket, server, proxy) = sockets(rtt).await;
    let chunk_kib = std::env::var("PHIRA_STREAM_BENCH_CHUNK_KIB")
        .ok()
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(256);
    let handlers = Arc::new(Mutex::new((download::Downloads::with_chunk_size(chunk_kib * 1024), upload::Uploads::default())));
    let server = Stream::new(
        None,
        server,
        Box::new(move |sender, command| {
            let user = Arc::clone(&user);
            let handlers = Arc::clone(&handlers);
            async move {
                let mut handlers = handlers.lock().await;
                let response = match command {
                    ClientCommand::Ping => Some(ServerCommand::Pong),
                    ClientCommand::StartLocalChartUpload { .. }
                    | ClientCommand::LocalChartUploadData { .. }
                    | ClientCommand::FinishLocalChartUpload { .. }
                    | ClientCommand::CancelLocalChartUpload { .. } => handlers.1.handle(&user, command).await,
                    _ => handlers.0.handle(&user, Arc::clone(&sender), command).await,
                };
                drop(handlers);
                if let Some(response) = response {
                    let _ = sender.send(response).await;
                }
            }
        }),
    );
    let (server, client) = tokio::join!(server, Client::new(socket));
    (Arc::new(client.unwrap()), server.unwrap(), proxy)
}

#[tokio::test]
async fn real_tcp_download_boundaries_and_upload() {
    for size in [1, LOCAL_CHART_STREAM_CHUNK_BYTES, LOCAL_CHART_STREAM_CHUNK_BYTES + 7, 8 * 1024 * 1024] {
        let bytes = vec![42; size];
        let (user, room) = fixture(bytes.clone());
        let (client, _server) = connection(user).await;
        let (received, _) = client.download_local_chart().await.unwrap();
        assert_eq!(received, bytes);
        let upload = vec![13; size];
        client.upload_local_chart("uploaded".into(), upload.clone()).await.unwrap();
        assert_eq!(*room.local_chart.read().await.as_ref().unwrap().bytes, upload);
        assert_eq!(client.download_local_chart().await.unwrap().0, upload);
    }
}

#[tokio::test]
async fn concurrent_clients_and_heartbeats() {
    let bytes = vec![9; 8 * 1024 * 1024];
    let (user, _room) = fixture(bytes.clone());
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let (client, server) = connection(Arc::clone(&user)).await;
        tasks.push(tokio::spawn(async move {
            let done = AtomicBool::new(false);
            let download = async {
                let result = client.download_local_chart().await;
                done.store(true, Ordering::SeqCst);
                result
            };
            let control = async {
                while !done.load(Ordering::SeqCst) {
                    assert!(client.ping().await.unwrap() < Duration::from_secs(2));
                    time::sleep(Duration::from_millis(10)).await;
                }
            };
            let (result, _) = tokio::join!(download, control);
            let _keep_alive = server;
            result.unwrap().0
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), bytes);
    }
}

#[tokio::test]
async fn bounded_window_invalid_ack_and_stale_ids() {
    let (user, _room) = fixture(vec![1; 4 * 1024 * 1024]);
    let (sender, mut packets) = mpsc::channel(32);
    let sender = Arc::new(sender);
    let mut downloads = download::Downloads::default();
    let hash = user
        .room
        .read()
        .await
        .as_ref()
        .unwrap()
        .local_chart
        .read()
        .await
        .as_ref()
        .unwrap()
        .sha256
        .clone();
    assert!(downloads
        .handle(
            &user,
            Arc::clone(&sender),
            ClientCommand::StartLocalChartDownload {
                transfer_id: 1,
                expected_sha256: hash.clone()
            }
        )
        .await
        .is_none());
    for offset in (0..LOCAL_CHART_STREAM_WINDOW_BYTES).step_by(LOCAL_CHART_STREAM_CHUNK_BYTES) {
        match time::timeout(Duration::from_secs(2), packets.recv()).await.unwrap().unwrap() {
            ServerCommand::LocalChartDownloadChunk { offset: actual, .. } => assert_eq!(actual as usize, offset),
            _ => panic!("expected chunk"),
        }
    }
    assert!(time::timeout(Duration::from_millis(30), packets.recv()).await.is_err());
    // A stale ACK cannot release this transfer's credit.
    downloads
        .handle(
            &user,
            Arc::clone(&sender),
            ClientCommand::LocalChartDownloadAck {
                transfer_id: 99,
                next_offset: 1048576,
            },
        )
        .await;
    assert!(time::timeout(Duration::from_millis(30), packets.recv()).await.is_err());
    assert!(matches!(
        downloads
            .handle(
                &user,
                Arc::clone(&sender),
                ClientCommand::LocalChartDownloadAck {
                    transfer_id: 1,
                    next_offset: 4194304
                }
            )
            .await,
        Some(ServerCommand::LocalChartDownloadError { .. })
    ));
    assert!(matches!(
        downloads
            .handle(
                &user,
                Arc::clone(&sender),
                ClientCommand::StartLocalChartDownload {
                    transfer_id: 1,
                    expected_sha256: hash
                }
            )
            .await,
        Some(ServerCommand::LocalChartDownloadError { .. })
    ));
}

#[tokio::test]
async fn cancellation_chart_change_and_disconnect() {
    let (user, room) = fixture(vec![1; 4 * 1024 * 1024]);
    let (sender, mut packets) = mpsc::channel(32);
    let sender = Arc::new(sender);
    let hash = room.local_chart.read().await.as_ref().unwrap().sha256.clone();
    let mut downloads = download::Downloads::default();
    downloads
        .handle(
            &user,
            Arc::clone(&sender),
            ClientCommand::StartLocalChartDownload {
                transfer_id: 1,
                expected_sha256: hash.clone(),
            },
        )
        .await;
    packets.recv().await.unwrap();
    downloads
        .handle(&user, Arc::clone(&sender), ClientCommand::CancelLocalChartDownload { transfer_id: 1 })
        .await;
    while packets.try_recv().is_ok() {}
    assert!(time::timeout(Duration::from_millis(30), packets.recv()).await.is_err());
    downloads
        .handle(
            &user,
            Arc::clone(&sender),
            ClientCommand::StartLocalChartDownload {
                transfer_id: 2,
                expected_sha256: hash,
            },
        )
        .await;
    *room.local_chart.write().await = None;
    loop {
        if matches!(
            time::timeout(Duration::from_secs(2), packets.recv()).await.unwrap().unwrap(),
            ServerCommand::LocalChartDownloadError { transfer_id: 2, .. }
        ) {
            break;
        }
    }
    let (user, _room) = fixture(vec![1; 4 * 1024 * 1024]);
    let (client, server) = connection(user).await;
    let info = client.local_chart_info().await.unwrap();
    drop(server);
    assert!(time::timeout(Duration::from_secs(2), client.download_local_chart_with_info(info))
        .await
        .unwrap()
        .is_err());
}

#[tokio::test]
async fn upload_rejects_offsets_and_correlates_completion() {
    let (user, _room) = fixture(vec![1]);
    let mut uploads = upload::Uploads::default();
    let start = ClientCommand::StartLocalChartUpload {
        transfer_id: 7,
        name: "test".into(),
        size: 1,
        sha256: "a".repeat(64),
    };
    assert!(matches!(
        uploads.handle(&user, start).await,
        Some(ServerCommand::LocalChartUploadProgress {
            transfer_id: 7,
            result: Ok(0)
        })
    ));
    assert!(matches!(
        uploads.handle(&user, ClientCommand::FinishLocalChartUpload { transfer_id: 8 }).await,
        Some(ServerCommand::LocalChartUploadVerified {
            transfer_id: 8,
            result: Err(_)
        })
    ));
    assert!(matches!(
        uploads
            .handle(
                &user,
                ClientCommand::LocalChartUploadData {
                    transfer_id: 7,
                    offset: 1,
                    data: vec![1]
                }
            )
            .await,
        Some(ServerCommand::LocalChartUploadProgress {
            transfer_id: 7,
            result: Err(_)
        })
    ));
}

#[tokio::test]
async fn client_rejects_corruption_and_premature_completion() {
    for scenario in 0..4 {
        let premature = scenario == 0;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let server = Stream::<ServerCommand, ClientCommand>::new(
            None,
            server,
            Box::new(move |sender, command| async move {
                match command {
                    ClientCommand::GetLocalChartInfo { request_id } => {
                        sender
                            .send(ServerCommand::LocalChartInfo {
                                request_id: request_id + 1,
                                result: Err("stale metadata reply".into()),
                            })
                            .await
                            .unwrap();
                        sender
                            .send(ServerCommand::LocalChartInfo {
                                request_id,
                                result: Ok(LocalChartInfo {
                                    size: 1,
                                    sha256: "a".repeat(64),
                                }),
                            })
                            .await
                            .unwrap();
                    }
                    ClientCommand::StartLocalChartDownload { transfer_id, .. } => {
                        // Delayed packets from unrelated transfers must be ignored.
                        sender
                            .send(ServerCommand::LocalChartDownloadFinished {
                                transfer_id: transfer_id + 1,
                            })
                            .await
                            .unwrap();
                        if !premature {
                            sender
                                .send(ServerCommand::LocalChartDownloadChunk {
                                    transfer_id,
                                    offset: if scenario == 2 { 1 } else { 0 },
                                    data: vec![
                                        1;
                                        if scenario == 3 {
                                            phira_mp_common::LOCAL_CHART_STREAM_MAX_CHUNK_BYTES + 1
                                        } else {
                                            1
                                        }
                                    ],
                                })
                                .await
                                .unwrap();
                        }
                        if premature {
                            sender.send(ServerCommand::LocalChartDownloadFinished { transfer_id }).await.unwrap();
                        }
                    }
                    ClientCommand::LocalChartDownloadAck { transfer_id, .. } => {
                        sender.send(ServerCommand::LocalChartDownloadFinished { transfer_id }).await.unwrap();
                    }
                    ClientCommand::Ping => {
                        sender.send(ServerCommand::Pong).await.unwrap();
                    }
                    _ => {}
                }
            }),
        );
        let (server, client) = tokio::join!(server, Client::new(socket));
        let _server = server.unwrap();
        let client = client.unwrap();
        let error = client.download_local_chart().await.unwrap_err().to_string();
        assert!(error.contains(["incomplete", "SHA-256", "offset", "chunk size"][scenario]), "{error}");
        client.ping().await.unwrap();
    }
}

#[tokio::test]
async fn matching_protocol_is_v2() {
    let (user, _room) = fixture(vec![1]);
    let (_client, server) = connection(user).await;
    assert_eq!(server.version(), PROTOCOL_VERSION);
}

#[tokio::test(start_paused = true)]
async fn stalled_transfers_expire_and_release_archive_references() {
    let (user, room) = fixture(vec![1; 4 * 1024 * 1024]);
    let bytes = Arc::clone(&room.local_chart.read().await.as_ref().unwrap().bytes);
    let hash = room.local_chart.read().await.as_ref().unwrap().sha256.clone();
    let (sender, mut packets) = mpsc::channel(16);
    let sender = Arc::new(sender);
    let mut downloads = download::Downloads::default();
    downloads
        .handle(
            &user,
            Arc::clone(&sender),
            ClientCommand::StartLocalChartDownload {
                transfer_id: 1,
                expected_sha256: hash,
            },
        )
        .await;
    for _ in 0..8 {
        packets.recv().await.unwrap();
    }
    time::advance(Duration::from_secs(31)).await;
    assert!(matches!(packets.recv().await.unwrap(), ServerCommand::LocalChartDownloadError { transfer_id: 1, .. }));
    tokio::task::yield_now().await;
    assert_eq!(Arc::strong_count(&bytes), 2);
    let mut uploads = upload::Uploads::default();
    uploads
        .handle(
            &user,
            ClientCommand::StartLocalChartUpload {
                transfer_id: 2,
                name: "test".into(),
                size: 1,
                sha256: "a".repeat(64),
            },
        )
        .await;
    tokio::task::yield_now().await;
    time::advance(Duration::from_secs(31)).await;
    tokio::task::yield_now().await;
    assert!(matches!(
        uploads.handle(&user, ClientCommand::FinishLocalChartUpload { transfer_id: 2 }).await,
        Some(ServerCommand::LocalChartUploadVerified { result: Err(_), .. })
    ));
}

#[tokio::test]
async fn overlapping_downloads_are_rejected_without_overwriting_state() {
    let (user, _room) = fixture(vec![1; 4 * 1024 * 1024]);
    let (client, _server) = connection(user).await;
    let info = client.local_chart_info().await.unwrap();
    let (first, second) = tokio::join!(client.download_local_chart_with_info(info.clone()), client.download_local_chart_with_info(info));
    assert!(first.is_ok());
    assert!(second.unwrap_err().to_string().contains("already active"));
}

#[tokio::test]
async fn obsolete_cached_content_cannot_start_or_ready_a_new_selection() {
    let (user, room) = fixture(vec![1]);
    let wrong = "a".repeat(64);
    assert!(matches!(
        super::process(
            Arc::clone(&user),
            ClientCommand::RequestStartLocalChart {
                expected_sha256: wrong.clone()
            }
        )
        .await,
        Some(ServerCommand::RequestStart(Err(_)))
    ));
    assert!(matches!(*room.state.read().await, InternalRoomState::SelectChart));
    *room.state.write().await = InternalRoomState::WaitForReady { started: HashSet::new() };
    assert!(matches!(super::process(user, ClientCommand::ReadyLocalChart { expected_sha256: wrong }).await, Some(ServerCommand::Ready(Err(_)))));
    match &*room.state.read().await {
        InternalRoomState::WaitForReady { started } => assert!(started.is_empty()),
        _ => panic!("unexpected state"),
    }
}

async fn legacy_download(user: Arc<User>, rtt: Duration) -> Duration {
    let (socket, server, _proxy) = sockets(rtt).await;
    let (tx, mut responses) = mpsc::channel(2);
    let server = Stream::<ServerCommand, ClientCommand>::new(
        None,
        server,
        Box::new(move |sender, command| {
            let user = Arc::clone(&user);
            async move {
                if let Some(response) = super::process(user, command).await {
                    let _ = sender.send(response).await;
                }
            }
        }),
    );
    let client = Stream::<ClientCommand, ServerCommand>::new(
        Some(PROTOCOL_VERSION),
        socket,
        Box::new(move |_, response| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(response).await;
            }
        }),
    );
    let (server, client) = tokio::join!(server, client);
    let (_server, client) = (server.unwrap(), client.unwrap());
    let started = Instant::now();
    let mut bytes = Vec::new();
    loop {
        client.send(ClientCommand::FetchLocalChunk { offset: bytes.len() as u32 }).await.unwrap();
        let ServerCommand::FetchLocalChunk(Ok(chunk)) = responses.recv().await.unwrap() else {
            panic!("legacy response")
        };
        bytes.extend_from_slice(&chunk.data);
        if bytes.len() == chunk.total_size as usize {
            assert_eq!(format!("{:x}", Sha256::digest(&bytes)), chunk.sha256);
            break;
        }
    }
    started.elapsed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "controlled latency benchmark; run explicitly with --ignored --nocapture"]
async fn benchmark_streaming_matrix() {
    let full = std::env::var_os("PHIRA_STREAM_BENCH_FULL").is_some();
    let sizes = if full { vec![1, 8, 16, 32] } else { vec![16] };
    let rtts = if full { vec![0, 50, 100, 200] } else { vec![120] };
    let concurrency = if full { vec![1, 2, 4, 8] } else { vec![1] };
    for size in sizes {
        let (user, _room) = fixture(vec![42; size * 1024 * 1024]);
        for &rtt_ms in &rtts {
            let rtt = Duration::from_millis(rtt_ms);
            if (!full || std::env::var_os("PHIRA_STREAM_BENCH_LEGACY").is_some()) && std::env::var_os("PHIRA_STREAM_BENCH_SKIP_LEGACY").is_none() {
                let old = legacy_download(Arc::clone(&user), rtt).await;
                println!("legacy,size_mib={size},rtt_ms={rtt_ms},clients=1,seconds={:.3}", old.as_secs_f64());
            }
            for &clients in &concurrency {
                let mut tasks = Vec::new();
                for _ in 0..clients {
                    let (client, server, proxy) = connection_with_rtt(Arc::clone(&user), rtt).await;
                    tasks.push(tokio::spawn(async move {
                        let started = Instant::now();
                        let download = client.download_local_chart();
                        let pings = async {
                            let mut peak = Duration::ZERO;
                            for _ in 0..4 {
                                peak = peak.max(client.ping().await.unwrap());
                            }
                            peak
                        };
                        let (result, peak_ping) = tokio::join!(download, pings);
                        assert_eq!(result.unwrap().0.len(), size * 1024 * 1024);
                        let elapsed = started.elapsed();
                        drop((server, proxy));
                        (elapsed, peak_ping)
                    }));
                }
                let mut elapsed = Duration::ZERO;
                let mut ping = Duration::ZERO;
                for task in tasks {
                    let (duration, peak) = task.await.unwrap();
                    elapsed = elapsed.max(duration);
                    ping = ping.max(peak);
                }
                println!(
                    "stream,size_mib={size},rtt_ms={rtt_ms},clients={clients},max_seconds={:.3},min_mib_s={:.2},peak_ping_ms={:.1}",
                    elapsed.as_secs_f64(),
                    size as f64 / elapsed.as_secs_f64(),
                    ping.as_secs_f64() * 1000.0
                );
            }
        }
    }
}
