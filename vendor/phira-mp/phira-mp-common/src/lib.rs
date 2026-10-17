mod bin;
pub use bin::*;

mod command;
pub use command::*;

use anyhow::{bail, Error, Result};
use std::{collections::VecDeque, future::Future, marker::PhantomData, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
};
use tracing::{error, trace, warn};

/// Room-local ZIP transfer limits; chunks stay below the TCP packet cap.
pub const MAX_LOCAL_CHART_BYTES: usize = 32 * 1024 * 1024;
pub const LOCAL_CHART_CHUNK_BYTES: usize = 64 * 1024;
pub const PROTOCOL_VERSION: u8 = 2;
pub const LOCAL_CHART_STREAM_CHUNK_BYTES: usize = 256 * 1024;
/// Receivers also accept the 512 KiB alternative; senders default to 256 KiB.
pub const LOCAL_CHART_STREAM_MAX_CHUNK_BYTES: usize = 512 * 1024;
pub const LOCAL_CHART_STREAM_WINDOW_BYTES: usize = 2 * 1024 * 1024;
pub const LOCAL_CHART_TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);
// Includes space for one bulk window and control traffic. Also bounds stale
// frames left behind by rapidly cancelled transfers from a hostile peer.
const OUTGOING_PACKET_CAPACITY: usize = 16;

/// Counts scheduled as well as written bytes. Only cumulative chunk-boundary ACKs
/// can release credit; duplicate ACKs are harmless.
#[derive(Debug, Default)]
pub struct ChartSendWindow {
    pub sent: u32,
    pub acknowledged: u32,
}

impl ChartSendWindow {
    pub fn acknowledge(&mut self, offset: u32, total: u32) -> Result<()> {
        if offset < self.acknowledged
            || offset > self.sent
            || offset > total
            || (offset != total && !(offset as usize).is_multiple_of(LOCAL_CHART_STREAM_CHUNK_BYTES))
        {
            bail!("invalid chart acknowledgement");
        }
        self.acknowledged = offset;
        Ok(())
    }

    pub fn next_end(&self, total: u32) -> Option<u32> {
        self.next_end_with_chunk_size(total, LOCAL_CHART_STREAM_CHUNK_BYTES)
    }

    pub fn next_end_with_chunk_size(&self, total: u32, chunk_bytes: usize) -> Option<u32> {
        let chunk_bytes = u32::try_from(chunk_bytes).ok()?;
        if chunk_bytes == 0 || chunk_bytes as usize > LOCAL_CHART_STREAM_MAX_CHUNK_BYTES {
            return None;
        }
        let end = self.sent.checked_add(chunk_bytes)?.min(total);
        (end > self.sent && end.checked_sub(self.acknowledged)? as usize <= LOCAL_CHART_STREAM_WINDOW_BYTES).then_some(end)
    }
}

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3);
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(2);
pub const HEARTBEAT_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub fn encode_packet(payload: &impl BinaryData, vec: &mut Vec<u8>) {
    BinaryWriter::new(vec).write(payload).unwrap();
}

pub fn decode_packet<T>(data: &[u8]) -> Result<T>
where
    T: BinaryData,
{
    BinaryReader::new(data).read()
}

pub struct Stream<S, R> {
    version: u8,

    send_tx: Arc<mpsc::Sender<S>>,

    send_task_handle: JoinHandle<()>,
    recv_task_handle: JoinHandle<Result<()>>,

    _marker: PhantomData<(S, R)>,
}

pub trait PacketScheduling {
    fn is_bulk(&self) -> bool;
}

impl PacketScheduling for ClientCommand {
    fn is_bulk(&self) -> bool {
        matches!(self, Self::LocalChartUploadData { .. })
    }
}

impl PacketScheduling for ServerCommand {
    fn is_bulk(&self) -> bool {
        matches!(self, Self::LocalChartDownloadChunk { .. })
    }
}

impl<S, R> Stream<S, R>
where
    S: BinaryData + PacketScheduling + std::fmt::Debug + Send + Sync + 'static,
    R: BinaryData + std::fmt::Debug + Send + 'static,
{
    pub async fn new<F>(
        version: Option<u8>,
        stream: TcpStream,
        mut handler: Box<dyn FnMut(Arc<mpsc::Sender<S>>, R) -> F + Send + Sync>,
    ) -> Result<Self>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        stream.set_nodelay(true)?;
        let (mut read, mut write) = stream.into_split();
        let version = if let Some(version) = version {
            write.write_u8(version).await?;
            version
        } else {
            read.read_u8().await?
        };
        if version != PROTOCOL_VERSION {
            bail!("unsupported multiplayer protocol {version}; expected {PROTOCOL_VERSION}");
        }

        let (send_tx, mut send_rx) = mpsc::channel::<S>(OUTGOING_PACKET_CAPACITY);
        let send_tx = Arc::new(send_tx);
        let send_task_handle = tokio::spawn({
            async move {
                let mut buffer = Vec::new();
                let mut len_buf = [0u8; 5];
                let mut control = VecDeque::new();
                let mut bulk = VecDeque::new();
                let mut control_burst = 0;
                loop {
                    // Bounded look-ahead gives control priority without a second
                    // writer or changing senders. FIFO is preserved within each
                    // class. Completion is sent only after all data is ACKed.
                    if control.is_empty() && bulk.is_empty() {
                        let Some(packet) = send_rx.recv().await else { break };
                        if packet.is_bulk() {
                            bulk.push_back(packet);
                        } else {
                            control.push_back(packet);
                        }
                    }
                    while control.len() + bulk.len() < OUTGOING_PACKET_CAPACITY {
                        let Ok(packet) = send_rx.try_recv() else { break };
                        if packet.is_bulk() {
                            bulk.push_back(packet);
                        } else {
                            control.push_back(packet);
                        }
                    }
                    let payload = if !control.is_empty() && (control_burst < 8 || bulk.is_empty()) {
                        control_burst += 1;
                        control.pop_front().unwrap()
                    } else {
                        control_burst = 0;
                        bulk.pop_front().unwrap()
                    };
                    buffer.clear();
                    encode_packet(&payload, &mut buffer);
                    trace!(
                        bytes = buffer.len(),
                        queued_control_packets = control.len(),
                        queued_bulk_packets = bulk.len(),
                        channel_packets = send_rx.len(),
                        "sending packet"
                    );
                    if buffer.len() > 2 * 1024 * 1024 {
                        error!("outgoing packet too large");
                        break;
                    }

                    let mut x = buffer.len() as u32;
                    let mut n = 0;
                    loop {
                        len_buf[n] = (x & 0x7f) as u8;
                        n += 1;
                        x >>= 7;
                        if x == 0 {
                            break;
                        } else {
                            len_buf[n - 1] |= 0x80;
                        }
                    }

                    if let Err(err) = async {
                        write.write_all(&len_buf[..n]).await?;
                        write.write_all(&buffer).await?;
                        Ok::<_, Error>(())
                    }
                    .await
                    {
                        error!("failed to send: {err:?}");
                        break;
                    }
                }
            }
        });

        let recv_task_handle = tokio::spawn({
            let send_tx = Arc::clone(&send_tx);
            #[allow(clippy::read_zero_byte_vec)]
            async move {
                let mut buffer = Vec::new();
                loop {
                    let mut len = 0u32;
                    let mut pos = 0;
                    loop {
                        let byte = read.read_u8().await?;
                        if pos == 28 && byte & 0xf0 != 0 {
                            bail!("invalid length");
                        }
                        len |= ((byte & 0x7f) as u32) << pos;
                        pos += 7;
                        if byte & 0x80 == 0 {
                            break;
                        }
                        if pos > 32 {
                            bail!("invalid length");
                        }
                    }
                    if len > 2 * 1024 * 1024 {
                        bail!("data packet too large");
                    }
                    let len = len as usize;

                    buffer.resize(len, 0);
                    read.read_exact(&mut buffer).await?;
                    trace!(bytes = buffer.len(), "received packet");

                    let payload: R = match decode_packet(&buffer) {
                        Ok(val) => val,
                        Err(err) => {
                            warn!("invalid packet: {err:?}");
                            break;
                        }
                    };
                    handler(Arc::clone(&send_tx), payload).await;
                }
                Ok(())
            }
        });

        Ok(Self {
            version,

            send_tx,

            send_task_handle,
            recv_task_handle,

            _marker: PhantomData,
        })
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn is_closed(&self) -> bool {
        self.recv_task_handle.is_finished() || self.send_task_handle.is_finished()
    }

    pub async fn send(&self, payload: S) -> Result<()> {
        self.send_tx.send(payload).await?;
        Ok(())
    }

    pub fn try_send(&self, payload: S) -> Result<()> {
        self.send_tx.try_send(payload)?;
        Ok(())
    }

    pub fn blocking_send(&self, payload: S) -> Result<()> {
        self.send_tx.blocking_send(payload)?;
        Ok(())
    }
}

impl<S, R> Drop for Stream<S, R> {
    fn drop(&mut self) {
        self.send_task_handle.abort();
        self.recv_task_handle.abort();
    }
}

#[cfg(test)]
mod local_chart_protocol_tests {
    use super::*;

    #[test]
    fn streaming_packets_and_frame_budget() {
        let packet = ServerCommand::LocalChartDownloadChunk {
            transfer_id: 42,
            offset: 256 * 1024,
            data: vec![7; LOCAL_CHART_STREAM_CHUNK_BYTES],
        };
        let mut bytes = Vec::new();
        encode_packet(&packet, &mut bytes);
        assert!(bytes.len() + 5 < 2 * 1024 * 1024);
        match decode_packet::<ServerCommand>(&bytes).unwrap() {
            ServerCommand::LocalChartDownloadChunk { transfer_id, offset, data } => {
                assert_eq!(transfer_id, 42);
                assert_eq!(offset, 256 * 1024);
                assert_eq!(data, vec![7; LOCAL_CHART_STREAM_CHUNK_BYTES]);
            }
            _ => panic!("wrong packet"),
        }
        for command in [
            ClientCommand::StartLocalChartDownload {
                transfer_id: 1,
                expected_sha256: "a".repeat(64),
            },
            ClientCommand::LocalChartDownloadAck {
                transfer_id: 1,
                next_offset: 1048576,
            },
            ClientCommand::CancelLocalChartDownload { transfer_id: 1 },
            ClientCommand::StartLocalChartUpload {
                transfer_id: 2,
                name: "test".into(),
                size: 1,
                sha256: "b".repeat(64),
            },
            ClientCommand::LocalChartUploadData {
                transfer_id: 2,
                offset: 0,
                data: vec![1],
            },
            ClientCommand::FinishLocalChartUpload { transfer_id: 2 },
        ] {
            bytes.clear();
            encode_packet(&command, &mut bytes);
            let decoded = decode_packet::<ClientCommand>(&bytes).unwrap();
            let mut again = Vec::new();
            encode_packet(&decoded, &mut again);
            assert_eq!(bytes, again);
        }
    }

    #[test]
    fn metadata_bounds_and_normalization() {
        for size in [1, MAX_LOCAL_CHART_BYTES as u32] {
            let mut info = LocalChartInfo {
                size,
                sha256: "AB".repeat(32),
            };
            info.validate().unwrap();
            assert_eq!(info.sha256, "ab".repeat(32));
        }
        for size in [0, MAX_LOCAL_CHART_BYTES as u32 + 1] {
            assert!(LocalChartInfo {
                size,
                sha256: "a".repeat(64)
            }
            .validate()
            .is_err());
        }
        for sha256 in ["".into(), "g".repeat(64), "a".repeat(65), "../".repeat(21)] {
            assert!(LocalChartInfo { size: 1, sha256 }.validate().is_err());
        }
    }

    #[test]
    fn window_and_cumulative_ack_validation() {
        for total in [
            1,
            LOCAL_CHART_STREAM_CHUNK_BYTES as u32,
            LOCAL_CHART_STREAM_CHUNK_BYTES as u32 + 1,
            MAX_LOCAL_CHART_BYTES as u32,
        ] {
            let mut w = ChartSendWindow::default();
            while w.acknowledged < total {
                while let Some(end) = w.next_end(total) {
                    w.sent = end;
                    assert!(w.sent - w.acknowledged <= LOCAL_CHART_STREAM_WINDOW_BYTES as u32);
                }
                assert!(w.acknowledge(w.sent + 1, total).is_err());
                let sent = w.sent;
                w.acknowledge(sent, total).unwrap();
                w.acknowledge(sent, total).unwrap();
                assert!(w.acknowledge(sent - 1, total).is_err());
            }
        }
        let mut w = ChartSendWindow {
            sent: 1048576,
            acknowledged: 0,
        };
        assert!(w.acknowledge(123, 2097152).is_err());
        assert_eq!(w.acknowledged, 0);
    }

    #[test]
    fn malformed_lengths_return_errors() {
        assert!(decode_packet::<String>(&[255; 20]).is_err());
        assert!(BinaryReader::new(&[]).take(usize::MAX).is_err());
    }

    #[tokio::test]
    async fn unsupported_protocol_is_rejected_before_packets() {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        client.write_u8(1).await.unwrap();
        let result = Stream::<ServerCommand, ClientCommand>::new(None, server, Box::new(|_, _| async {})).await;
        assert!(result.err().unwrap().to_string().contains("unsupported multiplayer protocol"));
    }

    #[test]
    fn chunk_roundtrip() {
        let packet = ClientCommand::LocalUploadChunk {
            offset: 64 * 1024,
            data: vec![1, 2, 3, 4, 5],
        };
        let mut bytes = Vec::new();
        encode_packet(&packet, &mut bytes);
        match decode_packet::<ClientCommand>(&bytes).unwrap() {
            ClientCommand::LocalUploadChunk { offset, data } => {
                assert_eq!(offset, 64 * 1024);
                assert_eq!(data, vec![1, 2, 3, 4, 5]);
            }
            _ => panic!("wrong client command"),
        }
        assert_eq!(bytes[0], 17, "legacy upload discriminant changed");
    }

    #[test]
    fn room_chart_response_roundtrip() {
        let packet = ServerCommand::FetchLocalChunk(Ok(LocalChartChunk {
            offset: 0,
            total_size: 8,
            sha256: "a".repeat(64),
            data: vec![0, 1, 2, 3],
        }));
        let mut bytes = Vec::new();
        encode_packet(&packet, &mut bytes);
        match decode_packet::<ServerCommand>(&bytes).unwrap() {
            ServerCommand::FetchLocalChunk(Ok(chunk)) => {
                assert_eq!(chunk.offset, 0);
                assert_eq!(chunk.total_size, 8);
                assert_eq!(chunk.data, vec![0, 1, 2, 3]);
                assert_eq!(chunk.sha256.len(), 64);
            }
            _ => panic!("wrong server response"),
        }
        assert_eq!(bytes[0], 23, "legacy fetch discriminant changed");
    }
}
