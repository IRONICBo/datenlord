use std::{
    cell::UnsafeCell,
    fmt::Debug,
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    task::{Context, Poll},
    u8,
};

use bytes::BytesMut;
use futures::{pin_mut, Future};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, Interval},
};
use tracing::debug;

use crate::{async_fuse::util::usize_to_u64, read_exact_timeout, write_all_timeout};

use super::{
    common::ClientTimeoutOptions,
    error::RpcError,
    message::{ReqType, RespType},
    packet::{Decode, Encode, Packet, PacketsKeeper, ReqHeader, RespHeader, REQ_HEADER_SIZE},
    utils::u64_to_usize,
};

/// TODO: combine `RpcClientConnectionInner` and `RpcClientConnection`
struct RpcClientConnectionInner<P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    /// The TCP stream for the connection.
    stream: UnsafeCell<TcpStream>,
    /// Options for the timeout of the connection
    timeout_options: ClientTimeoutOptions,
    /// Stream auto increment sequence number, used to mark the request and response
    seq: AtomicU64,
    /// The instant of the connection, used to calculate the keep alive timeout
    clock_instant: Instant,
    /// Received keep alive timestamp, will mark the valid keep alive response.
    /// If the keep alive response is not received in right mestamp, the connection will be closed,
    /// If we receive other response, we will update the received_keepalive_timestamp too, just treat it as a keep alive response.
    received_keepalive_timestamp: AtomicU64,
    /// Send packet task
    packets_keeper: UnsafeCell<PacketsKeeper<P>>,
    /// Response buffer, try to reuse same buffer to reduce memory allocation
    /// In case of the response is too large, we need to consider the buffer size
    /// Init size is 4MB
    /// Besides, we want to reduce memory copy, and read/write the response to the same data buffer
    resp_buf: UnsafeCell<BytesMut>,
    /// The Client ID for the connection
    client_id: u64,
}

// TODO: Add markable id for this client
impl<P> Debug for RpcClientConnectionInner<P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcClientConnectionInner")
            .field("client id", &self.client_id)
            .field("timeout options", &self.timeout_options)
            .finish_non_exhaustive()
    }
}

impl<P> RpcClientConnectionInner<P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    /// Create a new connection.
    pub fn new(stream: TcpStream, timeout_options: &ClientTimeoutOptions, client_id: u64) -> Self {
        Self {
            stream: UnsafeCell::new(stream),
            timeout_options: timeout_options.clone(),
            seq: AtomicU64::new(0),
            clock_instant: Instant::now(),
            received_keepalive_timestamp: AtomicU64::new(0),
            packets_keeper: UnsafeCell::new(PacketsKeeper::new(
                timeout_options.clone().idle_timeout.as_secs(),
            )),
            resp_buf: UnsafeCell::new(BytesMut::with_capacity(8 * 1024 * 1024)),
            client_id,
        }
    }

    /// Recv request header from the stream
    pub async fn recv_header(&self) -> Result<RespHeader, RpcError<String>> {
        // Try to read to buffer
        match self.recv_len(REQ_HEADER_SIZE).await {
            Ok(()) => {}
            Err(err) => {
                debug!("Failed to receive request header: {:?}", err);
                return Err(err);
            }
        }

        let buffer: &mut BytesMut = unsafe { &mut *self.resp_buf.get() };
        let req_header = RespHeader::decode(buffer)?;
        debug!("Received response header: {:?}", req_header);

        Ok(req_header)
    }

    /// Receive request body from the server.
    pub async fn recv_len(&self, len: u64) -> Result<(), RpcError<String>> {
        let mut req_buffer: &mut BytesMut = unsafe { &mut *self.resp_buf.get() };
        req_buffer.resize(u64_to_usize(len), 0);
        let reader = self.get_stream_mut();
        match read_exact_timeout!(reader, &mut req_buffer, self.timeout_options.read_timeout).await
        {
            Ok(size) => {
                debug!("{:?} Received response body len: {:?}", self, size);
                Ok(())
            }
            Err(err) => {
                debug!("{:?} Failed to receive response header: {:?}", self, err);
                Err(RpcError::InternalError(err.to_string()))
            }
        }
    }

    /// Send a request to the server.
    pub async fn send_data(&self, data: &[u8]) -> Result<(), RpcError<String>> {
        let writer = self.get_stream_mut();
        match write_all_timeout!(writer, data, self.timeout_options.write_timeout).await {
            Ok(()) => {
                debug!("{:?} Sent data with length: {:?}", self, data.len());
                Ok(())
            }
            Err(err) => {
                debug!("{:?} Failed to send data: {:?}", self, err);
                Err(RpcError::InternalError(err.to_string()))
            }
        }
    }

    /// Get the next sequence number.
    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, std::sync::atomic::Ordering::AcqRel)
    }

    /// Send keep alive message to the server
    pub async fn ping(&self) -> Result<(), RpcError<String>> {
        // Send keep alive message
        let current_seq = self.next_seq();
        let current_timestamp = self.clock_instant.elapsed().as_secs();
        // Check keepalive is valid
        let received_keepalive_timestamp = self
            .received_keepalive_timestamp
            .load(std::sync::atomic::Ordering::Acquire);
        if current_timestamp - received_keepalive_timestamp
            > self.timeout_options.keep_alive_timeout.as_secs()
        {
            debug!("{:?} keep alive timeout, close the connection", self);
            return Err(RpcError::InternalError("Keep alive timeout".to_owned()));
        }

        // Set to packet task
        let keep_alive_msg = ReqHeader {
            seq: current_seq,
            op: ReqType::KeepAliveRequest.to_u8(),
            len: 0,
        }
        .encode();

        if let Ok(()) = self.send_data(&keep_alive_msg).await {
            debug!("{:?} Success to sent keep alive message", self);
            Ok(())
        } else {
            debug!("{:?} Failed to send keep alive message", self);
            Err(RpcError::InternalError(
                "Failed to send keep alive message".to_owned(),
            ))
        }
    }

    /// Send packet by the client.
    pub async fn send_packet(&self, req_packet: &mut P) -> Result<(), RpcError<String>> {
        let current_seq = self.next_seq();
        req_packet.set_seq(current_seq);
        debug!("{:?} Try to send request: {:?}", self, current_seq);

        // Send may be used in different threads, so we need to create local buffer to store the request data
        if let Ok(req_buffer) = req_packet.get_req_data() {
            let mut req_header = ReqHeader {
                seq: req_packet.seq(),
                op: req_packet.op(),
                len: usize_to_u64(req_buffer.len()),
            }
            .encode();

            // concate req_header and req_buffer
            req_header.extend_from_slice(&req_buffer);

            if let Ok(()) = self.send_data(&req_header).await {
                debug!("{:?} Sent request success: {:?}", self, req_packet.seq());
                // We have set a copy to keeper and manage the status for the packets keeper
                // Set to packet task with clone
                self.get_packets_keeper_mut().add_task(req_packet.clone())?;

                Ok(())
            } else {
                debug!("{:?} Failed to send request: {:?}", self, req_packet.seq());
                Err(RpcError::InternalError("Failed to send request".to_owned()))
            }
        } else {
            debug!(
                "{:?} Failed to serialize request: {:?}",
                self,
                req_packet.seq()
            );
            Err(RpcError::InternalError(
                "Failed to serialize request".to_owned(),
            ))
        }
    }

    /// Receive packet by the client
    pub fn recv_packet(&self, resp_packet: &P) -> Option<P> {
        // Try to receive the response from innner keeper
        let packets_keeper: &mut PacketsKeeper<P> = self.get_packets_keeper_mut();
        packets_keeper.consume_task(resp_packet.seq())
    }

    /// Receive loop for the client.
    pub async fn recv_loop(&self) {
        // Check and clean keeper timeout unused task
        // The timeout data will be cleaned by the get function
        // We don't need to clean the timeout data radically
        let mut tickers = Box::pin(tokio::time::interval(
            self.timeout_options.idle_timeout * 100,
        ));
        loop {
            debug!("{:?} Start to receive loop", self);
            // Clean timeout tasks, will block here
            let recv_header_f = self.recv_header();
            pin_mut!(recv_header_f);
            let selector = ReceiveHeaderFuture::new(self, &mut tickers, &mut recv_header_f);
            match selector.await {
                Ok(header) => {
                    // Try to receive the response from the server
                    let header_seq = header.seq;
                    debug!("{:?} Received keep alive response or other response.", self);
                    let current_timestamp = self.clock_instant.elapsed().as_secs();
                    self.received_keepalive_timestamp
                        .store(current_timestamp, std::sync::atomic::Ordering::Release);
                    // Update the received keep alive seq
                    if let Ok(resp_type) = RespType::from_u8(header.op) {
                        if let RespType::FileBlockResponse = resp_type {
                            debug!("{:?} Received response header: {:?}", self, header);
                            // Try to read to buffer
                            match self.recv_len(header.len).await {
                                Ok(()) => {}
                                Err(err) => {
                                    debug!(
                                        "{:?} Failed to receive request header: {:?}",
                                        self, err
                                    );
                                    break;
                                }
                            }

                            // Take the packet task and recv the response
                            let packets_keeper: &mut PacketsKeeper<P> =
                                self.get_packets_keeper_mut();
                            let resp_buffer: &mut BytesMut = unsafe { &mut *self.resp_buf.get() };
                            match packets_keeper.update_task_mut(header_seq, resp_buffer) {
                                Ok(()) => {
                                    debug!("{:?} Received response: {:?}", self, header_seq);
                                }
                                Err(err) => {
                                    debug!("{:?} Failed to update task: {:?}", self, err);
                                }
                            }
                        }
                    } else {
                        debug!("{:?} Invalid response type: {:?}", self, header.op);
                        break;
                    }
                }
                Err(err) => {
                    debug!("{:?} Failed to receive request header: {:?}", self, err);
                    break;
                }
            }
        }
    }

    /// Get stream with mutable reference
    #[allow(clippy::mut_from_ref)]
    fn get_stream_mut(&self) -> &mut TcpStream {
        // Current implementation is safe because the stream is only accessed by one thread
        unsafe { &mut *self.stream.get() }
    }

    /// Get packet task with mutable reference
    #[allow(clippy::mut_from_ref)]
    fn get_packets_keeper_mut(&self) -> &mut PacketsKeeper<P> {
        // Current implementation is safe because the packet task is only accessed by one thread
        unsafe { &mut *self.packets_keeper.get() }
    }
}

unsafe impl<P> Send for RpcClientConnectionInner<P> where P: Packet + Clone + Send + Sync + 'static {}

unsafe impl<P> Sync for RpcClientConnectionInner<P> where P: Packet + Clone + Send + Sync + 'static {}

/// The RPC client definition.
#[derive(Debug)]
pub struct RpcClient<P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    /// The timeout options for the client.
    /// TODO:
    #[allow(dead_code)]
    timeout_options: ClientTimeoutOptions,
    /// Inner connection for the client.
    inner_connection: Arc<RpcClientConnectionInner<P>>,
    /// Client ID for the connection
    /// TODO:
    #[allow(dead_code)]
    client_id: AtomicU64,
}

impl<P> RpcClient<P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    /// Create a new RPC client.
    ///
    /// We don't manage the stream is dead or clean
    /// The client will be closed if the keep alive message is not received in 100 times
    /// When the stream is broken, the client will be closed, and we need to recreate a new client
    pub fn new(stream: TcpStream, timeout_options: &ClientTimeoutOptions) -> Self {
        let client_id = AtomicU64::new(0);
        let inner_connection = RpcClientConnectionInner::new(
            stream,
            timeout_options,
            client_id.load(std::sync::atomic::Ordering::Acquire),
        );

        Self {
            timeout_options: timeout_options.clone(),
            inner_connection: Arc::new(inner_connection),
            client_id,
        }
    }

    /// Start client receiver
    pub fn start_recv(&self) {
        // Create a receiver loop
        let inner_connection_clone = Arc::clone(&self.inner_connection);
        tokio::spawn(async move {
            inner_connection_clone.recv_loop().await;
        });
    }

    /// Send a request to the server.
    /// Try to send data to channel, if the channel is full, return an error.
    /// Contains the rezquest header and body.
    pub async fn send_request(&self, req: &mut P) -> Result<(), RpcError<String>> {
        self.inner_connection
            .send_packet(req)
            .await
            .map_err(|err| RpcError::InternalError(err.to_string()))
    }

    /// Get the response from the server.
    pub fn recv_response(&self, resp: &mut P) -> Result<(), RpcError<String>> {
        // Try to receive the response from innner keeper
        debug!("{:?} Try to receive response: {:?}", self, resp.seq());
        if let Some(resp_packet) = self.inner_connection.recv_packet(resp) {
            debug!("{:?} Received response seq: {:?}", self, resp_packet.seq());
            resp.set_resp_data(&resp_packet.get_resp_data()?)?;
            return Ok(());
        }

        Err(RpcError::InternalError(
            "Failed to receive response".to_owned(),
        ))
    }

    /// Manually send ping by the send request
    /// The inner can not start two loop, because the stream is unsafe
    /// we need to start the loop in the client or higher level manually
    pub async fn ping(&self) -> Result<(), RpcError<String>> {
        self.inner_connection.ping().await
    }
}

/// The receive stream for the client.
/// The stream will be used to receive the response from the server,
/// and clean outdated tasks when the task is timeout.
struct ReceiveHeaderFuture<'a, P, F>
where
    P: Packet + Clone + Send + Sync + 'static,
    F: Future<Output = Result<RespHeader, RpcError<String>>> + Unpin,
{
    /// The inner connection for the client.
    client_inner: &'a RpcClientConnectionInner<P>,
    /// The interval for period task.
    interval: &'a mut Pin<Box<Interval>>,
    /// The recv_future in the client.
    recv_future: Pin<&'a mut F>,
}

impl<'a, P, F> ReceiveHeaderFuture<'a, P, F>
where
    P: Packet + Clone + Send + Sync + 'static,
    F: Future<Output = Result<RespHeader, RpcError<String>>> + Unpin,
{
    /// Create a new receive header stream.
    fn new(
        client_inner: &'a RpcClientConnectionInner<P>,
        interval: &'a mut Pin<Box<Interval>>,
        recv_future: &'a mut F,
    ) -> Self {
        Self {
            client_inner,
            interval,
            recv_future: Pin::new(recv_future),
        }
    }
}

impl<'a, P, F> Future for ReceiveHeaderFuture<'a, P, F>
where
    P: Packet + Clone + Send + Sync + 'static,
    F: Future<Output = Result<RespHeader, RpcError<String>>> + Unpin,
{
    type Output = Result<RespHeader, RpcError<String>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let self_mut: &mut ReceiveHeaderFuture<'a, P, F> = self.get_mut();
        let client_inner = self_mut.client_inner;

        // clean timeout tasks
        let mut tick_ready = false;
        while self_mut.interval.as_mut().poll_tick(cx).is_ready() {
            // consume all ticks in once call
            tick_ready = true;
        }
        if tick_ready {
            let packets_keeper: &mut PacketsKeeper<P> = client_inner.get_packets_keeper_mut();
            packets_keeper.clean_timeout_tasks();
        }

        // recv header data
        self_mut.recv_future.as_mut().poll(cx)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use tokio::net::TcpListener;

    use crate::storage::cache::rpc::packet::PacketStatus;
    use crate::storage::cache::rpc::setup;

    use super::*;
    use std::time::Duration;

    #[derive(Debug, Clone)]
    pub struct TestPacket {
        pub seq: u64,
        pub op: u8,
        pub status: PacketStatus,
    }

    impl Packet for TestPacket {
        fn seq(&self) -> u64 {
            self.seq
        }

        fn set_seq(&mut self, seq: u64) {
            self.seq = seq;
        }

        fn op(&self) -> u8 {
            self.op
        }

        fn set_op(&mut self, op: u8) {
            self.op = op;
        }

        fn set_req_data(&mut self, _data: &[u8]) -> Result<(), RpcError<String>> {
            Ok(())
        }

        fn get_req_data(&self) -> Result<Vec<u8>, RpcError<String>> {
            Ok(Vec::new())
        }

        fn set_resp_data(&mut self, _data: &[u8]) -> Result<(), RpcError<String>> {
            Ok(())
        }

        fn get_resp_data(&self) -> Result<Vec<u8>, RpcError<String>> {
            Ok(Vec::new())
        }

        fn status(&self) -> PacketStatus {
            self.status
        }

        fn set_status(&mut self, status: PacketStatus) {
            self.status = status;
        }
    }

    // Helper function to setup a mock server
    async fn setup_mock_server(addr: String, response: Vec<u8>) -> String {
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                socket.write_all(&response).await.unwrap(); // Send a predefined response
                socket.flush().await.unwrap();
            }
        });
        addr
    }

    #[tokio::test]
    async fn test_new_connection() {
        setup();
        let local_addr = setup_mock_server("127.0.0.1:50050".to_owned(), vec![]).await;
        let stream = TcpStream::connect(local_addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let client_id = 123;
        let connection =
            RpcClientConnectionInner::<TestPacket>::new(stream, &timeout_options, client_id);
        assert_eq!(connection.client_id, client_id);
    }

    #[tokio::test]
    async fn test_recv_header() {
        setup();
        let response = RespHeader {
            seq: 1,
            op: RespType::KeepAliveResponse.to_u8(),
            len: 0,
        }
        .encode();
        let addr = setup_mock_server("127.0.0.1:50052".to_owned(), response).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let connection = RpcClientConnectionInner::<TestPacket>::new(stream, &timeout_options, 123);
        let header = connection.recv_header().await.unwrap();
        assert_eq!(header.seq, 1);
        assert_eq!(header.op, RespType::KeepAliveResponse.to_u8());
    }

    #[tokio::test]
    async fn test_recv_len() {
        setup();
        // Create a 10 len vector
        let response = vec![0; 10];
        let addr = setup_mock_server("127.0.0.1:50053".to_owned(), response).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let connection = RpcClientConnectionInner::<TestPacket>::new(stream, &timeout_options, 123);
        connection.recv_len(10).await.unwrap();
    }

    #[tokio::test]
    async fn test_send_data() {
        setup();
        let addr = setup_mock_server("127.0.0.1:50054".to_owned(), vec![]).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let connection = RpcClientConnectionInner::<TestPacket>::new(stream, &timeout_options, 123);
        let data = b"Hello, world!";
        connection.send_data(data).await.unwrap();
    }

    #[tokio::test]
    async fn test_next_seq() {
        setup();
        let addr = setup_mock_server("127.0.0.1:50055".to_owned(), vec![]).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let client_id = 123;
        let connection =
            RpcClientConnectionInner::<TestPacket>::new(stream, &timeout_options, client_id);
        let seq1 = connection.next_seq();
        let seq2 = connection.next_seq();
        assert_eq!(seq1 + 1, seq2);
    }

    #[tokio::test]
    async fn test_ping() {
        setup();
        let response = RespHeader {
            seq: 1,
            op: RespType::KeepAliveResponse.to_u8(),
            len: 0,
        }
        .encode();
        let addr = setup_mock_server("127.0.0.1:50056".to_owned(), response).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let connection = RpcClientConnectionInner::<TestPacket>::new(stream, &timeout_options, 123);
        connection.ping().await.unwrap();
    }
}