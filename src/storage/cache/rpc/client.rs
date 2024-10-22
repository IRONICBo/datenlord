use std::{
    cell::UnsafeCell,
    fmt::Debug,
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    task::{Context, Poll},
};

use bytes::BytesMut;
use futures::{pin_mut, Future};
use smol::future::FutureExt;
use tokio::{
    io::{AsyncRead, AsyncWriteExt, ReadBuf},
    net::TcpStream,
    time::{Instant, Interval},
};
use tracing::debug;

use crate::write_all_timeout;

use super::{
    common::{self, ClientTimeoutOptions},
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
    packets_keeper: PacketsKeeper<P>,
    /// Response buffer, try to reuse same buffer to reduce memory allocation
    /// In case of the response is too large, we need to consider the buffer size
    /// Init size is 4MB
    /// Besides, we want to reduce memory copy, and read/write the response to the same data buffer
    resp_buf: UnsafeCell<BytesMut>,
    /// Request encode buffer
    req_buf: UnsafeCell<BytesMut>,
    /// The Client ID for the connection
    client_id: u64,
    /// The receiver for the closed signal.
    is_closed_receiver: flume::Receiver<()>,
}

// TODO: Add markable id for this client
impl<P> Debug for RpcClientConnectionInner<P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcClientConnectionInner")
            .field("client id", &self.client_id)
            .finish_non_exhaustive()
    }
}

impl<P> RpcClientConnectionInner<P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    /// Create a new connection.
    pub fn new(
        stream: TcpStream,
        timeout_options: ClientTimeoutOptions,
        client_id: u64,
        is_closed_receiver: flume::Receiver<()>,
    ) -> Self {
        let task_timeout = timeout_options.task_timeout.as_secs();
        Self {
            stream: UnsafeCell::new(stream),
            timeout_options,
            seq: AtomicU64::new(0),
            clock_instant: Instant::now(),
            received_keepalive_timestamp: AtomicU64::new(0),
            packets_keeper: PacketsKeeper::new(task_timeout),
            resp_buf: UnsafeCell::new(BytesMut::with_capacity(
                common::DEFAULT_TCP_RESPONSE_BUFFER_SIZE,
            )),
            req_buf: UnsafeCell::new(BytesMut::with_capacity(
                common::DEFAULT_TCP_REQUEST_BUFFER_SIZE,
            )),
            client_id,
            is_closed_receiver,
        }
    }

    /// Recv request header from the stream
    async fn recv_header(&self) -> Result<RespHeader, RpcError> {
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

    /// Receive response body from the server.
    async fn recv_len(&self, len: u64) -> Result<(), RpcError> {
        let req_buffer: &mut BytesMut = unsafe { &mut *self.resp_buf.get() };
        req_buffer.resize(u64_to_usize(len), 0);
        // let reader = self.get_stream_mut();

        let recv_len_future =
            RecvLenFuture::new(self, len, req_buffer, self.is_closed_receiver.recv_async());

        // Block here until the client is dropped or stream error.
        match recv_len_future.await {
            Ok(size) => {
                debug!("Received response body size: {:?}", size);
                Ok(())
            }
            Err(err) => {
                debug!("Failed to receive response header: {:?}", err);
                Err(RpcError::InternalError(err.to_string()))
            }
        }
    }

    /// Send a request to the server.
    /// We need to make sure
    /// Current send packet need to be in one task, so if we need to send ping and packet
    /// we need to make sure only one operation is running, like select.
    async fn send_data(&self, req_header: &dyn Encode, packet: Option<P>) -> Result<(), RpcError> {
        let buf = unsafe { &mut *self.req_buf.get() };
        buf.clear();
        // encode just need to append to buffer, do not clear buffer
        req_header.encode(buf);
        // Append the body to the buffer
        if let Some(body) = packet {
            // encode just need to append to buffer, do not clear buffer
            body.encode(buf);
        }
        debug!("{:?} Sent data with length: {:?}", self, buf.len());
        let writer = self.get_stream_mut();
        match write_all_timeout!(writer, buf, self.timeout_options.write_timeout).await {
            Ok(()) => Ok(()),
            Err(err) => {
                debug!("{:?} Failed to send data: {:?}", self, err);
                Err(RpcError::InternalError(err.to_string()))
            }
        }
    }

    /// Get the next sequence number.
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, std::sync::atomic::Ordering::AcqRel)
    }

    /// Send keep alive message to the server
    async fn ping(&self) -> Result<(), RpcError> {
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
            op: ReqType::KeepAliveRequest.into(),
            len: 0,
        };

        if let Ok(()) = self.send_data(&keep_alive_msg, None).await {
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
    async fn send_packet(&self, mut req_packet: P) -> Result<(), RpcError> {
        let current_seq = self.next_seq();
        req_packet.set_seq(current_seq);
        debug!("{:?} Try to send request: {:?}", self, current_seq);

        // Send may be used in different threads, so we need to create local buffer to store the request data
        let req_len = req_packet.get_req_len(); // Try to get request buffer
        let req_header = ReqHeader {
            seq: req_packet.seq(),
            op: req_packet.op(),
            len: req_len,
        };

        // concate req_header and req_buffer
        let req_seq = req_packet.seq();
        if let Ok(()) = self.send_data(&req_header, Some(req_packet)).await {
            debug!("{:?} Sent request success: {:?}", self, req_seq);
            Ok(())
        } else {
            debug!("{:?} Failed to send request: {:?}", self, req_seq);
            Err(RpcError::InternalError("Failed to send request".to_owned()))
        }
    }

    /// Receive loop for the client.
    async fn recv_loop(&self) {
        // Check and clean keeper timeout unused task
        // The timeout data will be cleaned by the get function
        // We don't need to clean the timeout data radically
        let mut tickers = Box::pin(tokio::time::interval(
            self.timeout_options.task_timeout.div_f32(2.0),
        ));
        loop {
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
                    if let Ok(resp_type) = RespType::try_from(header.op) {
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
                            let resp_buffer: &mut BytesMut = unsafe { &mut *self.resp_buf.get() };
                            match self.packets_keeper.take_task(header_seq, resp_buffer).await {
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

        // Purge all the tasks
        self.packets_keeper.purge_outdated_tasks().await;
    }

    /// Get stream with mutable reference
    #[allow(clippy::mut_from_ref)]
    fn get_stream_mut(&self) -> &mut TcpStream {
        // Current implementation is safe because the stream is only accessed by one thread
        unsafe { &mut *self.stream.get() }
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
    /// Is closed flag sender, this struct is associated with the inner_connection,
    /// if this client is dropped, the inner_connection receiver will receive the closed signal.
    #[allow(dead_code)]
    is_closed_sender: flume::Sender<()>,
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
    pub fn new(stream: TcpStream, timeout_options: ClientTimeoutOptions) -> Self {
        let client_id = AtomicU64::new(0);
        let (is_closed_sender, is_closed_receiver) = flume::unbounded::<()>();
        let inner_connection = RpcClientConnectionInner::new(
            stream,
            timeout_options.clone(),
            client_id.load(std::sync::atomic::Ordering::Acquire),
            is_closed_receiver,
        );

        Self {
            timeout_options,
            inner_connection: Arc::new(inner_connection),
            client_id,
            is_closed_sender,
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
    /// WARN: this function does not support concurrent call
    pub async fn send_request(&self, req: P) -> Result<(), RpcError> {
        self.inner_connection
            .send_packet(req)
            .await
            .map_err(|err| RpcError::InternalError(err.to_string()))
    }

    /// Manually send ping by the send request
    /// The inner can not start two loop, because the stream is unsafe
    /// we need to start the loop in the client or higher level manually
    /// WARN: this function does not support concurrent call
    pub async fn ping(&self) -> Result<(), RpcError> {
        self.inner_connection.ping().await
    }
}

/// `RecvLenFuture` TCP Stream reader future for receive data
/// We need to keep block the stream until the data is received
/// or the stream is closed or client is dropped
/// The future will be used to receive the response from the server
struct RecvLenFuture<'a, P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    /// The inner connection for the client.
    client: &'a RpcClientConnectionInner<P>,
    /// The length of the data to receive.
    len: u64,
    /// The buffer to store the received data.
    req_buffer: &'a mut BytesMut,
    /// The receiver for the closed signal.
    is_closed: flume::r#async::RecvFut<'a, ()>,
}

impl<'a, P> RecvLenFuture<'a, P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    /// Create a new receive length future.
    fn new(
        client: &'a RpcClientConnectionInner<P>,
        len: u64,
        req_buffer: &'a mut BytesMut,
        is_closed: flume::r#async::RecvFut<'a, ()>,
    ) -> Self {
        Self {
            client,
            len,
            req_buffer,
            is_closed,
        }
    }
}

impl<P> Future for RecvLenFuture<'_, P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    type Output = Result<(), RpcError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Resize the buffer to accommodate the incoming data
        this.req_buffer.resize(u64_to_usize(this.len), 0);

        let mut reader = this.client.get_stream_mut();
        let mut read_buf = ReadBuf::new(this.req_buffer);

        let buf_poll_status = match Pin::new(&mut reader).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(size)) => {
                debug!("Received response body size: {:?}", size);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => {
                debug!("Failed to receive response header: {:?}", err);
                Poll::Ready(Err(RpcError::InternalError(err.to_string())))
            }
            Poll::Pending => Poll::Pending,
        };

        // Recv closed data from the client channel
        match this.is_closed.poll(cx) {
            Poll::Ready(Ok(())) => {
                debug!("Client is closed, stop the receive loop");
                Poll::Ready(Err(RpcError::InternalError("Client is closed".to_owned())))
            }
            Poll::Ready(Err(err)) => {
                debug!("Failed to receive closed signal: {:?}", err);
                Poll::Ready(Err(RpcError::InternalError(err.to_string())))
            }
            Poll::Pending => buf_poll_status,
        }
    }
}

/// The receive stream for the client.
/// The stream will be used to receive the response from the server,
/// and clean outdated tasks when the task is timeout.
struct ReceiveHeaderFuture<'a, P, F>
where
    P: Packet + Clone + Send + Sync + 'static,
    F: Future<Output = Result<RespHeader, RpcError>> + Unpin,
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
    F: Future<Output = Result<RespHeader, RpcError>> + Unpin,
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
    F: Future<Output = Result<RespHeader, RpcError>> + Unpin,
{
    type Output = Result<RespHeader, RpcError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let self_mut: &mut ReceiveHeaderFuture<'a, P, F> = self.get_mut();
        let client_inner = self_mut.client_inner;

        // clean timeout tasks
        let mut tick_ready = false;
        while self_mut.interval.as_mut().poll_tick(cx).is_ready() {
            // consume all ticks in once call
            tick_ready = true;
        }
        while tick_ready {
            let clean_future = client_inner.packets_keeper.clean_timeout_tasks();
            pin_mut!(clean_future);
            match clean_future.poll(cx) {
                Poll::Ready(()) => tick_ready = false,
                Poll::Pending => return Poll::Pending,
            }
        }

        // recv header data
        self_mut.recv_future.as_mut().poll(cx)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use async_trait::async_trait;
    use bytes::BufMut;
    use tokio::net::TcpListener;

    use crate::{async_fuse::util::usize_to_u64, storage::cache::rpc::utils::get_u64_from_buf};

    use super::*;
    use std::{mem, time::Duration};

    /// Request struct for test
    #[derive(Debug, Default, Clone)]
    pub struct TestRequest {
        pub id: u64,
    }

    impl Encode for TestRequest {
        fn encode(&self, buf: &mut BytesMut) {
            buf.put_u64(self.id.to_le());
        }
    }

    impl Decode for TestRequest {
        fn decode(buf: &[u8]) -> Result<Self, RpcError> {
            if buf.len() < 8 {
                return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
            }
            let id = get_u64_from_buf(buf, 0)?;
            Ok(TestRequest { id })
        }
    }

    #[derive(Debug, Clone)]
    pub struct TestPacket {
        pub seq: u64,
        pub op: u8,
        pub request: TestRequest,
        pub timestamp: u64,
    }

    impl Encode for TestPacket {
        fn encode(&self, buf: &mut BytesMut) {
            self.request.encode(buf);
        }
    }

    #[async_trait]
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

        fn set_timestamp(&mut self, timestamp: u64) {
            self.timestamp = timestamp;
        }

        fn get_timestamp(&self) -> u64 {
            self.timestamp
        }

        fn set_resp_data(&mut self, _data: &[u8]) -> Result<(), RpcError> {
            Ok(())
        }

        async fn set_result(self, _status: Result<(), RpcError>) {}

        fn get_req_len(&self) -> u64 {
            usize_to_u64(mem::size_of_val(&self.request))
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
        // setup();
        let local_addr = setup_mock_server("127.0.0.1:50050".to_owned(), vec![]).await;
        let stream = TcpStream::connect(local_addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let client_id = 123;
        let (sender, receiver) = flume::unbounded::<()>();
        let connection = RpcClientConnectionInner::<TestPacket>::new(
            stream,
            timeout_options,
            client_id,
            receiver,
        );
        assert_eq!(connection.client_id, client_id);

        sender.send(()).unwrap();
    }

    #[tokio::test]
    async fn test_recv_header() {
        // setup();
        let mut buf = BytesMut::new();
        RespHeader {
            seq: 1,
            op: RespType::KeepAliveResponse.into(),
            len: 0,
        }
        .encode(&mut buf);
        let addr = setup_mock_server("127.0.0.1:50052".to_owned(), buf.to_vec()).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let (sender, receiver) = flume::unbounded::<()>();
        let connection =
            RpcClientConnectionInner::<TestPacket>::new(stream, timeout_options, 123, receiver);
        let header = connection.recv_header().await.unwrap();
        assert_eq!(header.seq, 1);
        let resp_op: u8 = RespType::KeepAliveResponse.into();
        assert_eq!(header.op, resp_op);
        sender.send(()).unwrap();
    }

    #[tokio::test]
    async fn test_recv_len() {
        // setup();
        // Create a 10 len vector
        let response = vec![0; 10];
        let addr = setup_mock_server("127.0.0.1:50053".to_owned(), response).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let (sender, receiver) = flume::unbounded::<()>();
        let connection =
            RpcClientConnectionInner::<TestPacket>::new(stream, timeout_options, 123, receiver);
        connection.recv_len(10).await.unwrap();

        sender.send(()).unwrap();
    }

    struct TestData;
    impl Encode for TestData {
        fn encode(&self, buf: &mut BytesMut) {
            let data = b"Hello, world!";
            buf.extend_from_slice(data);
        }
    }

    #[tokio::test]
    async fn test_send_data() {
        // setup();
        let addr = setup_mock_server("127.0.0.1:50054".to_owned(), vec![]).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let (sender, receiver) = flume::unbounded::<()>();
        let connection =
            RpcClientConnectionInner::<TestPacket>::new(stream, timeout_options, 123, receiver);
        let req_header = ReqHeader {
            seq: 1,
            op: ReqType::KeepAliveRequest.into(),
            len: 0,
        };

        connection.send_data(&req_header, None).await.unwrap();

        sender.send(()).unwrap();
    }

    #[tokio::test]
    async fn test_next_seq() {
        // setup();
        let addr = setup_mock_server("127.0.0.1:50055".to_owned(), vec![]).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let client_id = 123;
        let (sender, receiver) = flume::unbounded::<()>();
        let connection = RpcClientConnectionInner::<TestPacket>::new(
            stream,
            timeout_options,
            client_id,
            receiver,
        );
        let seq1 = connection.next_seq();
        let seq2 = connection.next_seq();
        assert_eq!(seq1 + 1, seq2);

        sender.send(()).unwrap();
    }

    #[tokio::test]
    async fn test_ping() {
        // setup();
        let mut buf = BytesMut::new();
        RespHeader {
            seq: 1,
            op: RespType::KeepAliveResponse.into(),
            len: 0,
        }
        .encode(&mut buf);
        let addr = setup_mock_server("127.0.0.1:50056".to_owned(), buf.to_vec()).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let (sender, receiver) = flume::unbounded::<()>();
        let connection =
            RpcClientConnectionInner::<TestPacket>::new(stream, timeout_options, 123, receiver);
        connection.ping().await.unwrap();

        sender.send(()).unwrap();
    }
}
