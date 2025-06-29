use std::sync::Once;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    filter, fmt::layer, layer::SubscriberExt, util::SubscriberInitExt, Layer,
};

/// Use for unit test to setup tracing
#[allow(dead_code)]
static INIT: Once = Once::new();

/// Set up once for tracing
#[allow(dead_code)]
fn setup() {
    // init tracing once
    INIT.call_once(|| {
        // Set the tracing log level to debug
        let filter =
            filter::Targets::new().with_target("datenlord::storage::cache", LevelFilter::DEBUG);
        tracing_subscriber::registry()
            .with(layer().with_filter(filter))
            .init();
    });
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::distribute_kv_cache::rpc::client::RpcClient;
    use crate::distribute_kv_cache::rpc::common::{ClientTimeoutOptions, ServerTimeoutOptions};
    use crate::distribute_kv_cache::rpc::message::{
        FileBlockRequest, FileBlockRequestTask, FileBlockResponse, ReqType, RespType, StatusCode,
    };
    use crate::distribute_kv_cache::rpc::packet::{Encode, ReqHeader, RespHeader};
    use crate::distribute_kv_cache::rpc::server::{RpcServer, RpcServerConnectionHandler};
    use crate::distribute_kv_cache::rpc::utils::u64_to_usize;
    use crate::distribute_kv_cache::rpc::workerpool::{Job, WorkerPool};
    use crate::{connect_timeout, encode_to_buf};
    use async_trait::async_trait;
    use bytes::{Buf, BufMut, BytesMut};
    use tokio::sync::mpsc;
    use tokio::time;
    use tracing::{debug, error};

    use crate::async_fuse::util::usize_to_u64;

    /// The handler for the RPC file block request.
    #[derive(Debug)]
    pub struct FileBlockHandler {
        /// The request header.
        header: ReqHeader,
        /// The file block request.
        request: FileBlockRequest,
        /// The channel for sending the response.
        done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
    }

    impl FileBlockHandler {
        /// Create a new file block handler.
        #[must_use]
        pub fn new(
            header: ReqHeader,
            request: FileBlockRequest,
            done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
        ) -> Self {
            Self {
                header,
                request,
                done_tx,
            }
        }
    }

    #[async_trait]
    impl Job for FileBlockHandler {
        async fn run(&self) {
            // Mock: serve block request and send response
            let size = self.request.block_size;

            // Prepare response body
            // Mock: response body is all zeros
            let file_block_resp = FileBlockResponse {
                file_id: self.request.file_id,
                block_id: self.request.block_id,
                block_size: size,
                block_version: self.request.block_version,
                hash_ring_version: self.request.hash_ring_version,
                status: StatusCode::Success,
                data: vec![0_u8; u64_to_usize(size)],
            };
            let resp_body_buffer = BytesMut::new();
            encode_to_buf!(resp_body_buffer.clone(), &file_block_resp);
            // Prepare response header
            let resp_header = RespHeader {
                seq: self.header.seq,
                op: RespType::FileBlockResponse.into(),
                len: usize_to_u64(resp_body_buffer.len()),
            };
            let mut resp_header_buffer = BytesMut::new();
            resp_header.encode(&mut resp_header_buffer);
            // Combine response header and body
            resp_header_buffer.extend_from_slice(&resp_body_buffer);

            // Send response to the done channel
            match self.done_tx.send(vec![resp_header_buffer.freeze()]).await {
                Ok(()) => {
                    debug!("Sent response to done channel");
                }
                Err(err) => {
                    error!("Failed to send response to done channel: {:?}", err);
                }
            }
        }
    }

    /// The file block handler for the RPC server.
    #[derive(Clone, Debug)]
    pub struct FileBlockRpcServerHandler {
        /// The worker pool for the RPC server.
        worker_pool: Arc<WorkerPool>,
    }

    impl FileBlockRpcServerHandler {
        /// Create a new file block RPC server handler.
        #[must_use]
        pub fn new(worker_pool: Arc<WorkerPool>) -> Self {
            Self { worker_pool }
        }
    }

    #[async_trait]
    impl RpcServerConnectionHandler for FileBlockRpcServerHandler {
        async fn dispatch(
            &self,
            req_header: ReqHeader,
            req_buffer: BytesMut,
            done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
        ) {
            // Dispatch the handler for the connection
            if let Ok(req_type) = ReqType::try_from(req_header.op) {
                if let ReqType::FileBlockRequest = req_type {
                    // Try to read the request body
                    // Decode the request body
                    let reader = req_buffer.reader();
                    let req_body: FileBlockRequest = match bincode::deserialize_from(reader) {
                        Ok(req) => req,
                        Err(err) => {
                            debug!("Failed to decode file block request: {:?}", err);
                            return;
                        }
                    };

                    debug!(
                        "FileBlockRpcServerHandler: Received file block request: {:?}",
                        req_body
                    );

                    // File block request
                    // Submit the handler to the worker pool
                    // When the handler is done, send the response to the done channel
                    // Response need to contain the response header and body
                    let handler = FileBlockHandler::new(req_header, req_body, done_tx.clone());
                    if let Ok(()) = self
                        .worker_pool
                        .submit_job(Box::new(handler))
                        .map_err(|err| {
                            debug!("Failed to submit job: {:?}", err);
                        })
                    {
                        debug!("Submitted job to worker pool");
                    }
                } else {
                    debug!(
                        "FileBlockRpcServerHandler: Inner request type is not matched: {:?}",
                        req_header.op
                    );
                }
            }
        }
    }

    /// Check if the port is in use
    async fn is_port_in_use(addr: &str) -> bool {
        if let Ok(stream) = tokio::net::TcpStream::connect(addr).await {
            // Port is in use
            drop(stream);
            true
        } else {
            // Port is not in use
            false
        }
    }

    #[tokio::test]
    async fn test_send_and_recv_packet() {
        // setup();
        // Setup server
        let addr = "127.0.0.1:2788";
        let pool = Arc::new(WorkerPool::new(1000, 1000));
        let handler = FileBlockRpcServerHandler::new(Arc::clone(&pool));
        let mut server = RpcServer::new(&ServerTimeoutOptions::default(), 1000, 1000, handler);
        server.listen(addr).await.unwrap();
        time::sleep(Duration::from_secs(1)).await;
        assert!(is_port_in_use(addr).await);
        time::sleep(Duration::from_secs(1)).await;

        // Create a client
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(100),
            write_timeout: Duration::from_secs(100),
            task_timeout: Duration::from_secs(100),
            keep_alive_timeout: Duration::from_secs(20),
        };
        let connect_stream = connect_timeout!(addr, timeout_options.read_timeout)
            .await
            .unwrap();

        let rpc_client = RpcClient::<FileBlockRequestTask>::new(connect_stream, &timeout_options);
        rpc_client.start_recv();

        time::sleep(Duration::from_secs(1)).await;

        // Send ping
        unsafe {
            rpc_client.ping().await.unwrap();
        }

        // benchmark for 400 block with 512k size
        let block_size = 512 * 1024;
        let block_count = 400;

        for i in 0..block_count {
            let (tx, rx) = flume::unbounded::<Result<FileBlockResponse, FileBlockRequest>>();
            // Send file block request
            let block_request = FileBlockRequest {
                block_id: i,
                block_size,
                file_id: 0,
                block_version: 0,
                hash_ring_version: 1,
            };
            let packet = FileBlockRequestTask::new(&block_request, tx.clone());
            rpc_client.send_request(packet).await.unwrap();

            loop {
                match rx.recv_async().await {
                    Ok(resp) => {
                        let resp = resp.unwrap();
                        debug!("Received response ok with len: {:?}", resp.data.len());

                        assert_eq!(resp.file_id, 0);
                        assert_eq!(resp.block_id, i);
                        assert_eq!(resp.block_size, block_size);
                        assert_eq!(resp.status, StatusCode::Success);
                        assert_eq!(resp.data.len(), u64_to_usize(block_size));
                        debug!("Received response ok with len: {:?}", resp.data.len());
                        break;
                    }
                    Err(err) => {
                        error!("Failed to receive response: {:?}", err);
                        // time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }

        let (tx, rx) = flume::unbounded::<Result<FileBlockResponse, FileBlockRequest>>();
        // Send file block request
        let block_request = FileBlockRequest {
            block_id: 0,
            block_size,
            file_id: 0,
            block_version: 0,
            hash_ring_version: 1,
        };
        let packet = FileBlockRequestTask::new(&block_request, tx.clone());
        rpc_client.send_request(packet).await.unwrap();

        loop {
            match rx.recv_async().await {
                Ok(resp) => {
                    let resp = resp.unwrap();
                    debug!("Received response ok with len: {:?}", resp.data.len());

                    assert_eq!(resp.file_id, 0);
                    assert_eq!(resp.block_id, 0);
                    assert_eq!(resp.block_size, 4096);
                    assert_eq!(resp.status, StatusCode::Success);
                    assert_eq!(resp.data.len(), 4096);
                    debug!("Received response ok with len: {:?}", resp.data.len());
                    break;
                }
                Err(err) => {
                    error!("Failed to receive response: {:?}", err);
                    time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        server.stop();
    }

    #[tokio::test]
    async fn test_request_timeout() {
        // setup();
        // Setup server
        let addr = "127.0.0.1:2791";
        let pool = Arc::new(WorkerPool::new(4, 100));
        let handler = FileBlockRpcServerHandler::new(Arc::clone(&pool));
        let mut server = RpcServer::new(&ServerTimeoutOptions::default(), 4, 100, handler);
        server.listen(addr).await.unwrap();
        time::sleep(Duration::from_secs(1)).await;
        assert!(is_port_in_use(addr).await);
        time::sleep(Duration::from_secs(1)).await;

        // Create a client with short timeout
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_secs(1),
            task_timeout: Duration::from_secs(1),
            keep_alive_timeout: Duration::from_secs(1),
        };
        let connect_stream = connect_timeout!(addr, timeout_options.read_timeout)
            .await
            .unwrap();

        let rpc_client = RpcClient::<FileBlockRequestTask>::new(connect_stream, &timeout_options);
        rpc_client.start_recv();

        time::sleep(Duration::from_secs(5)).await;

        // Send ping, and current response is error
        unsafe {
            assert!(rpc_client.ping().await.is_err());
        }

        server.stop();
    }
}
