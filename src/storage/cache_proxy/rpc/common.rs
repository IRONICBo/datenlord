use std::{
    cell::UnsafeCell,
    fmt,
    future::Future,
    mem::transmute,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use bytes::BytesMut;
use crossfire::mpsc;
use futures::{future::FutureExt, pin_mut};
use hydro_common::{
    buffer::alloc_buf,
    error::ErrorStatic,
    logger::*,
    net::{UnifyBufStream, UnifyStream},
    sync::wait_group::WaitGroupExecutor,
    time::get_delay_now,
};
use nix::errno::Errno;
use tokio::time::{interval_at, sleep, Duration, Instant, Interval};
use zerocopy::AsBytes;

use super::{common::*, notifier::*, throttler::*};
use crate::errors::ERR_RPC_COMM;

macro_rules! retry_with_err {
    ($self:expr, $t:expr, $err:expr) => {
        if let Some(retry_task_sender) = $self.retry_task_sender.as_ref() {
            let retry_task = RetryTaskInfo { task: $t, task_err: $err };
            if let Err(mpsc::SendError(rt)) = retry_task_sender.send(Some(retry_task)) {
                rt.unwrap().task.set_result(Err(Errno::EIO));
            }
        }
    };
}

pub struct RpcClient<T: RpcTask + Send + Unpin + 'static> {
    close_h: Option<mpsc::TxUnbounded<()>>,
    inner: Arc<RpcClientInner<T>>,
}

impl<T> RpcClient<T>
where
    T: RpcTask + Send + Unpin + 'static,
{
    /// timeout_setting: only use read_timeout/write_timeout
    pub fn new(
        remote_host_id: u32, remote_dev_id: u32, local_client_id: u32, stream: UnifyStream,
        thresholds: usize, task_timeout: usize, timeout_setting: TimeoutSetting,
        retry_tx: Option<mpsc::TxUnbounded<Option<RetryTaskInfo<T>>>>,
        last_resp_ts: Option<Arc<AtomicU64>>, logger: Option<Arc<SubLogger>>,
    ) -> Self {
        let (_close_tx, _close_rx) = mpsc::unbounded_future::<()>();
        Self {
            close_h: Some(_close_tx),
            inner: Arc::new(RpcClientInner::new(
                remote_host_id,
                remote_dev_id,
                local_client_id,
                stream,
                retry_tx,
                _close_rx,
                thresholds,
                task_timeout,
                timeout_setting,
                last_resp_ts,
                logger,
            )),
        }
    }

    pub fn start_receiver(&self) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            inner.receive_loop().await;
        });
    }

    /// Should be call in sender thread
    #[inline(always)]
    pub async fn ping(&self) -> Result<(), ErrorStatic> {
        self.inner.send_ping_req().await
    }

    #[inline(always)]
    pub fn get_last_resp_ts(&self) -> u64 {
        if let Some(ts) = self.inner.last_resp_ts.as_ref() {
            ts.load(Ordering::Acquire)
        } else {
            0
        }
    }

    /// Since sender and receiver are two threads, might be close on either side
    #[inline(always)]
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Force the receiver to exit
    pub async fn set_error_and_exit(&self) {
        self.inner.has_err.store(true, Ordering::Release);
        let stream = self.inner.get_stream_mut();
        let _ = stream.close().await; // stream close is just shutdown on sending, receiver might not be notified on peer dead
        if let Some(close_h) = self.close_h.as_ref() {
            let _ = close_h.send(()); // This equals to RpcClient::drop
        }
    }
}

impl<T> RpcClient<T>
where
    T: RpcTask + Send + Unpin,
{
    #[inline(always)]
    pub async fn send_task(&self, task: T, need_flush: bool) -> Result<(), ErrorStatic> {
        self.inner.send_task(task, need_flush).await
    }

    #[inline(always)]
    pub async fn flush_req(&self) -> Result<(), ErrorStatic> {
        self.inner.flush_req().await
    }

    #[inline]
    pub fn will_block(&self) -> bool {
        if let Some(t) = self.inner.throttler.as_ref() {
            t.nearly_full()
        } else {
            false
        }
    }

    #[inline(always)]
    pub async fn throttle(&self) -> bool {
        if self.inner.closed.load(Ordering::Acquire) {
            return false;
        }
        if let Some(t) = self.inner.throttler.as_ref() {
            return t.throttle().await;
        } else {
            false
        }
    }
}

impl<T> Drop for RpcClient<T>
where
    T: RpcTask + Send + Unpin + 'static,
{
    fn drop(&mut self) {
        self.close_h.take();
        let notifier = self.inner.get_notifier_mut();
        notifier.stop_reg_task();
        self.inner.closed.store(true, Ordering::Release);
    }
}

impl<T> fmt::Debug for RpcClient<T>
where
    T: RpcTask + Send + Unpin + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

pub struct RpcClientInner<T>
where
    T: RpcTask + Send + Unpin + 'static,
{
    remote_host_id: u32,
    remote_dev_id: u32,
    local_client_id: u32,
    stream: UnsafeCell<UnifyBufStream>,
    timeout: TimeoutSetting,
    seq: AtomicU64,
    retry_task_sender: Option<mpsc::TxUnbounded<Option<RetryTaskInfo<T>>>>,
    close_h: mpsc::RxUnbounded<()>, // When RpcClient(sender) dropped, receiver will be notifier
    closed: AtomicBool,             // flag set by either sender or receive on there exit
    notifier: UnsafeCell<RpcTaskNotifier<T>>,
    has_err: AtomicBool,
    resp_buf: UnsafeCell<BytesMut>,
    throttler: Option<Throttler>,
    last_resp_ts: Option<Arc<AtomicU64>>,
    logger: Arc<SubLogger>,
}

unsafe impl<T> Send for RpcClientInner<T> where T: RpcTask + Send + Unpin + 'static {}

unsafe impl<T> Sync for RpcClientInner<T> where T: RpcTask + Send + Unpin + 'static {}

impl<T> fmt::Debug for RpcClientInner<T>
where
    T: RpcTask + Send + Unpin + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "rpc client {}:{}", self.remote_host_id, self.remote_dev_id)
    }
}

impl<T> RpcClientInner<T>
where
    T: RpcTask + Send + Unpin + 'static,
{
    pub fn new(
        remote_host_id: u32, remote_dev_id: u32, local_client_id: u32, stream: UnifyStream,
        retry_tx: Option<mpsc::TxUnbounded<Option<RetryTaskInfo<T>>>>,
        close_h: mpsc::RxUnbounded<()>, thresholds: usize, task_timeout: usize,
        timeout_setting: TimeoutSetting, last_resp_ts: Option<Arc<AtomicU64>>,
        mut logger: Option<Arc<SubLogger>>,
    ) -> Self {
        // XXX it seams local_client_id has no meaning
        if let None = logger {
            logger = Some(Arc::new(SubLogger::new(0)));
            logger.as_mut().unwrap().set_level(log::Level::Trace);
        }

        let mut client_inner = Self {
            remote_host_id,
            remote_dev_id,
            local_client_id,
            stream: UnsafeCell::new(UnifyBufStream::with_capacity(33 * 1024, 33 * 1024, stream)),
            retry_task_sender: retry_tx,
            close_h,
            closed: AtomicBool::new(false),
            seq: AtomicU64::new(1),
            timeout: timeout_setting,
            notifier: UnsafeCell::new(RpcTaskNotifier::new(
                remote_host_id,
                local_client_id,
                task_timeout,
                thresholds,
            )),
            resp_buf: UnsafeCell::new(BytesMut::with_capacity(512)),
            throttler: None,
            last_resp_ts,
            has_err: AtomicBool::new(false),
            logger: logger.unwrap(),
        };

        if thresholds > 0 {
            logger_trace!(
                client_inner.logger,
                "thresholds of throttler is set to {} (client {}:{})",
                thresholds,
                remote_host_id,
                local_client_id
            );
            client_inner.throttler = Some(Throttler::new(thresholds));
        } else {
            logger_trace!(
                client_inner.logger,
                "throttler is disabled (client {}:{})",
                remote_host_id,
                local_client_id
            );
        }

        client_inner
    }

    #[inline(always)]
    fn get_stream_mut(&self) -> &mut UnifyBufStream {
        unsafe { std::mem::transmute(self.stream.get()) }
    }

    #[inline(always)]
    fn get_notifier_mut(&self) -> &mut RpcTaskNotifier<T> {
        unsafe { std::mem::transmute(self.notifier.get()) }
    }

    #[inline(always)]
    fn should_close(&self, e: Errno) -> bool {
        e == Errno::EAGAIN || e == Errno::EHOSTDOWN
    }

    pub fn set_timeout(&mut self, read_timeout: Duration, write_timeout: Duration) {
        self.timeout.read_timeout = read_timeout;
        self.timeout.write_timeout = write_timeout;
    }

    /// Directly work on the socket steam, when failed
    pub async fn send_task(&self, mut task: T, need_flush: bool) -> Result<(), ErrorStatic> {
        let notifier = self.get_notifier_mut();
        notifier.pending_task_count_ref().fetch_add(1, Ordering::SeqCst);
        if self.closed.load(Ordering::Acquire) {
            logger_warn!(
                self.logger,
                "{:?} sending task {} failed: {}",
                self,
                task,
                ERR_RPC_CLOSED
            );
            retry_with_err!(self, task, TaskError::RpcErr(ErrorStatic(ERR_RPC_CLOSED)));
            notifier.pending_task_count_ref().fetch_sub(1, Ordering::SeqCst); // rollback
            return Err(ERR_RPC_COMM);
        }

        match self.send_request(&mut task, need_flush).await {
            Err(e) => {
                logger_warn!(self.logger, "{:?} sending task {} failed: {}", self, task, e);
                notifier.pending_task_count_ref().fetch_sub(1, Ordering::SeqCst); // rollback
                retry_with_err!(self, task, TaskError::RpcErr(e));
                self.closed.store(true, Ordering::Release);
                self.has_err.store(true, Ordering::Release);
                notifier.stop_reg_task();
                return Err(e);
            }
            Ok(_) => {
                logger_trace!(self.logger, "{:?} send task {} success", self, task);
                // register task to norifier
                let mut wg_executor: Option<WaitGroupExecutor> = None;
                if let Some(throttler) = self.throttler.as_ref() {
                    wg_executor = Some(throttler.add_task());
                }
                notifier.reg_task(task, wg_executor).await;
                return Ok(());
            }
        }
    }

    #[inline(always)]
    pub async fn flush_req(&self) -> Result<(), ErrorStatic> {
        let writer = self.get_stream_mut();
        let r = writer.flush_timeout(self.timeout.write_timeout).await;
        if r.is_err() {
            logger_warn!(self.logger, "{:?} flush_req flush err: {:?}", self, r);
            self.closed.store(true, Ordering::Release);
            self.has_err.store(true, Ordering::Release);
            let notifier = self.get_notifier_mut();
            notifier.stop_reg_task();

            return Err(ERR_RPC_COMM);
        }
        Ok(())
    }

    #[inline(always)]
    async fn send_request(&self, task: &mut T, need_flush: bool) -> Result<(), ErrorStatic> {
        let msg_buf = task.get_msg_buf().unwrap();
        let ext_len =
            if let Some(ref ext_buf) = task.get_ext_buf_ref() { ext_buf.len() } else { 0 };

        let seq = self.seq_update();
        task.set_seq(seq);
        // encode response header
        let header = ReqHead {
            magic: RPC_MAGIC,
            seq,
            client_id: self.local_client_id as u8,
            ver: 1,
            format: RpcEncoding::MSGPack as u8,
            action: task.action(),
            msg_len: msg_buf.len() as u32,
            ext_len: ext_len as u32,
            ..Default::default()
        };

        let writer = self.get_stream_mut();
        let r = writer.write_timeout(header.as_bytes(), self.timeout.write_timeout).await;
        if r.is_err() {
            logger_warn!(self.logger, "{:?} send_req write req header err: {:?}", self, r);
            return Err(ERR_RPC_COMM);
        }
        logger_debug!(self.logger, "{:?} rpc client send request {}", self, header);

        let r = writer.write_timeout(&msg_buf, self.timeout.write_timeout).await;
        if r.is_err() {
            logger_warn!(self.logger, "{:?} send_req write req msg err: {:?}", self, r);
            return Err(ERR_RPC_COMM);
        }

        if ext_len > 0 {
            let ext_buf = task.get_ext_buf_ref().unwrap();
            let r = writer.write_timeout(&ext_buf, self.timeout.write_timeout).await;
            if r.is_err() {
                logger_warn!(self.logger, "{:?} send_req write req ext err: {:?}", self, r);
                return Err(ERR_RPC_COMM);
            }
        }
        if need_flush || ext_len >= 32 * 1024 {
            let r = writer.flush_timeout(self.timeout.write_timeout).await;
            if r.is_err() {
                warn!("{:?} send_req flush req err: {:?}", self, r);
                return Err(ERR_RPC_COMM);
            }
        }

        return Ok(());
    }

    // return Ok(false) when close_h has close and nothing more pending resp to receive
    async fn recv_some(&self) -> Result<bool, ErrorClientSide> {
        for _ in 0i32..20 {
            // Underlayer rpc socket is buffered, might not yeal to runtime
            // return if recv_one_resp runs too long, allow timer to be fire at each second
            match self.recv_one_resp().await {
                Err(e) => {
                    if e == ErrorClientSide::Static(ERR_RPC_CLOSED) {
                        return Ok(false);
                    } else {
                        return Err(e);
                    }
                }
                Ok(_) => {
                    if let Some(last_resp_ts) = self.last_resp_ts.as_ref() {
                        last_resp_ts.store(get_delay_now(), Ordering::Release);
                    }
                }
            }
        }
        Ok(true)
    }

    async fn recv_one_resp(&self) -> Result<(), ErrorClientSide> {
        let rpc_head: RespHead;

        let mut resp_head_buf = [0u8; RPC_RESPONSE_HEADER_LEN];
        let reader = self.get_stream_mut();
        let read_timeout = self.timeout.read_timeout;

        'HeaderLoop: loop {
            if self.closed.load(Ordering::Acquire) {
                let notifier = self.get_notifier_mut();
                // ensure task receive on normal exit
                if notifier.check_pending_tasks_empty() || self.has_err.load(Ordering::Acquire) {
                    return Err(ErrorClientSide::Static(ERR_RPC_CLOSED));
                }

                if let Err(_e) = reader.read_exact_timeout(&mut resp_head_buf, read_timeout).await {
                    logger_debug!(
                        self.logger,
                        "{:?} rpc client read resp head when closing err: {:?}",
                        self,
                        _e
                    );
                    return Err(ErrorClientSide::Static(ERR_COMM));
                }
                break;
            } else {
                let close_f = self.close_h.recv().fuse();
                pin_mut!(close_f);
                let read_header_f = reader.read_exact(&mut resp_head_buf).fuse();
                pin_mut!(read_header_f);
                futures::select! {
                    r = read_header_f => {
                        match r {
                            Err(_e) => {
                                logger_debug!(self.logger, "{:?} rpc client read resp head err: {:?}", self, _e);
                                return Err(ErrorClientSide::Static(ERR_COMM));
                            }
                            Ok(_) => {
                                break 'HeaderLoop;
                            },
                        }
                    },
                    _ = close_f => {
                        self.closed.store(true, Ordering::Release);
                        continue
                    }
                }
            }
        }
        match decode_response_header(&resp_head_buf) {
            Err(_e) => {
                logger_debug!(
                    self.logger,
                    "{:?} rpc client decode_response_header err: {:?}",
                    self,
                    _e
                );
                return Err(ErrorClientSide::Static(ERR_COMM));
            }
            Ok(head) => {
                rpc_head = head;
                logger_trace!(self.logger, "{:?} rpc client read head response {}", self, rpc_head);
            }
        }

        // detect if seq is vaild
        /*
        if rpc_head.seq > self.seq {
            return Err(ErrorClientSide::Static(ERR_COMM));
        }
        */

        if rpc_head.action == PING_ACTION {
            if rpc_head.err_no != 0 {
                let err_no = Errno::from_i32(rpc_head.err_no as i32);
                logger_trace!(self.logger, "{:?} got ping respone with err_no {}", self, err_no);
                if self.should_close(err_no) {
                    self.closed.store(true, Ordering::Release);
                }
            }
            return Ok(());
        }

        let mut read_buf: &mut BytesMut = unsafe { transmute(self.resp_buf.get()) };
        // let mut msg: Option<&[u8]> = None;
        if rpc_head.msg_len > 0 {
            read_buf.resize(rpc_head.msg_len as usize, 0);
            match reader.read_exact_timeout(&mut read_buf, read_timeout).await {
                Err(_) => {
                    return Err(ErrorClientSide::Static(ERR_COMM));
                }
                Ok(_) => {
                    // msg = Some(&read_buf);
                }
            }
        }

        let notifier = self.get_notifier_mut();

        let ext_len = rpc_head.ext_len;
        let seq = rpc_head.seq;
        if let Some(mut task_item) = notifier.take_task(seq).await {
            let mut task = task_item.task.take().unwrap();
            if ext_len > 0 {
                match task.get_ext_buf_ref_mut() {
                    None => {
                        match alloc_buf(rpc_head.ext_len as usize) {
                            Err(e) => {
                                logger_error!(
                                    self.logger,
                                    "{:?} rpc client failed to alloc buf for ext_buf, err: {:?}",
                                    self,
                                    e
                                );
                                retry_with_err!(self, task, TaskError::RpcErr(ERR_RPC_COMM));
                                return Err(ErrorClientSide::Static(ERR_COMM));
                            }
                            Ok(mut ext_buf) => {
                                match reader.read_exact_timeout(&mut ext_buf, read_timeout).await {
                                    Err(e) => {
                                        logger_debug!(
                                            self.logger,
                                            "{:?} rpc client reader read ext_buf err: {:?}",
                                            self,
                                            e
                                        );
                                        retry_with_err!(
                                            self,
                                            task,
                                            TaskError::RpcErr(ERR_RPC_COMM)
                                        );
                                        return Err(ErrorClientSide::Static(ERR_COMM));
                                    }
                                    Ok(_) => {
                                        // extension = Some(ext_buf);
                                        task.set_ext_buf(ext_buf);
                                    }
                                }
                            }
                        }
                    }
                    Some(ext_buf_ref) => {
                        // read_exact_timeout will read the exact number of bytes, so the buffer
                        // size should not be larger than the extension data len of 'ext_len'
                        let len: usize = if ext_buf_ref.len() < ext_len as usize {
                            logger_warn!(
                                self.logger,
                                "{:?} unexpect: ext_buf_len={} is smaller than ext_len={}",
                                self,
                                ext_buf_ref.len(),
                                ext_len
                            );
                            ext_buf_ref.len()
                        } else {
                            ext_len as usize
                        };
                        match reader.read_exact_timeout(&mut ext_buf_ref[..len], read_timeout).await
                        {
                            Err(e) => {
                                logger_debug!(
                                    self.logger,
                                    "{:?} rpc client reader read ext_buf err: {:?}",
                                    self,
                                    e
                                );
                                retry_with_err!(self, task, TaskError::RpcErr(ERR_RPC_COMM));
                                return Err(ErrorClientSide::Static(ERR_COMM));
                            }
                            Ok(_) => {}
                        }
                    }
                }
            }

            if rpc_head.err_no != 0 {
                let err_no = Errno::from_i32(rpc_head.err_no as i32);
                if self.should_close(err_no) {
                    self.closed.store(true, Ordering::Release);
                }
                retry_with_err!(self, task, TaskError::PosixErrno(err_no));
            } else {
                // set result of task, and notify task completed
                match task.fill_task(&read_buf) {
                    Ok(_) => {
                        logger_debug!(self.logger, "{:?} recv task {} ok", self, task);
                        task.set_result(Ok(()));
                    }
                    Err(e) => {
                        logger_warn!(self.logger, "{:?} recv task {} failed: {:?}", self, task, e);
                        retry_with_err!(self, task, e);
                    }
                }
            }
        } else {
            // regarded as timeout response, drop the extend content

            logger_debug!(self.logger, "{:?} notifier take_task(seq={}) return None", self, seq);
            if ext_len > 0 {
                match alloc_buf(ext_len as usize) {
                    Err(e) => return Err(ErrorClientSide::Static(e.0)),
                    Ok(mut ext_buf) => {
                        match reader.read_exact_timeout(&mut ext_buf, read_timeout).await {
                            Err(_) => {
                                return Err(ErrorClientSide::Static(ERR_COMM));
                            }
                            Ok(_) => {
                                // drop the buf
                                // extension = Some(ext_buf);
                            }
                        }
                    }
                }
            }
        }
        return Ok(());
    }

    pub async fn receive_loop(&self) {
        let later = Instant::now() + Duration::from_secs(1);
        let mut tick = Box::pin(interval_at(later, Duration::from_secs(1)));
        //let mut reciver = Box::pin(self.recv_resp());

        loop {
            let f = self.recv_some();
            pin_mut!(f);
            let selector = ReciverTimerFuture::new(self, &mut tick, &mut f);
            match selector.await {
                Ok(true) => {}
                Ok(false) => return, // We are done
                Err(e) => {
                    logger_debug!(self.logger, "{:?} receive_loop error: {:?}", self, e);
                    self.closed.store(true, Ordering::Release);
                    let notifier = self.get_notifier_mut();
                    notifier.clean_pending_tasks(self.retry_task_sender.as_ref());
                    // If pending_task_count > 0 means some tasks may still remain in the pending chan
                    while notifier.pending_task_count_ref().load(Ordering::SeqCst) > 0 {
                        // After the 'closed' flag has taken effect,
                        // pending_task_count will not keep growing,
                        // so there is no need to sleep here.
                        notifier.clean_pending_tasks(self.retry_task_sender.as_ref());
                        sleep(Duration::from_secs(1)).await;
                    }
                    return;
                }
            }
        }
    }

    // Adjust the waiting queue
    fn time_reach(&self) {
        if let Some(throttler) = self.throttler.as_ref() {
            logger_trace!(
                self.logger,
                "{:?} has {} pending_tasks",
                self,
                throttler.get_inflight_tasks_count()
            );
        }
        let notifier = self.get_notifier_mut();
        notifier.adjust_task_queue(self.retry_task_sender.as_ref());
        return;
    }

    #[inline(always)]
    fn seq_update(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    #[inline(always)]
    pub async fn send_ping_req(&self) -> Result<(), ErrorStatic> {
        if self.closed.load(Ordering::Acquire) {
            logger_warn!(self.logger, "{:?} send_ping_req skip as conn closed", self);
            return Err(ErrorStatic(ERR_RPC_CLOSED));
        }

        // 我们可能自定义的头部信息
        // encode response header
        let header = ReqHead {
            magic: RPC_MAGIC,
            seq: self.seq_update(),
            client_id: self.local_client_id as u8,
            ver: 1,
            format: RpcEncoding::MSGPack as u8,
            action: PING_ACTION,
            msg_len: 0 as u32,
            ext_len: 0 as u32,
            ..Default::default()
        };

        let writer = self.get_stream_mut();
        let r = writer.write_timeout(header.as_bytes(), self.timeout.write_timeout).await;
        if r.is_err() {
            logger_warn!(self.logger, "{:?} send_ping_req write head {:?}", self, r);
            self.closed.store(true, Ordering::Release);
            return Err(ERR_RPC_COMM);
        }

        let r = writer.flush_timeout(self.timeout.write_timeout).await;
        if r.is_err() {
            logger_warn!(self.logger, "{:?} send_ping_req flush req err: {:?}", self, r);
            self.closed.store(true, Ordering::Release);
            return Err(ERR_RPC_COMM);
        }

        logger_trace!(self.logger, "{:?} rpc client send ping request: {}", self, header);
        return Ok(());
    }
}

impl<T> Drop for RpcClientInner<T>
where
    T: RpcTask + Send + Unpin + 'static,
{
    fn drop(&mut self) {
        let notifier = self.get_notifier_mut();
        notifier.clean_pending_tasks(self.retry_task_sender.as_ref());
    }
}

struct ReciverTimerFuture<'a, T, F>
where
    T: RpcTask + Send + Unpin + 'static,
    F: Future<Output = Result<bool, ErrorClientSide>> + Unpin,
{
    client: &'a RpcClientInner<T>,
    inv: &'a mut Pin<Box<Interval>>,
    recv_future: Pin<&'a mut F>,
}

impl<'a, T, F> ReciverTimerFuture<'a, T, F>
where
    T: RpcTask + Send + Unpin + 'static,
    F: Future<Output = Result<bool, ErrorClientSide>> + Unpin,
{
    fn new(
        client: &'a RpcClientInner<T>, inv: &'a mut Pin<Box<Interval>>, recv_future: &'a mut F,
    ) -> Self {
        Self { inv, client, recv_future: Pin::new(recv_future) }
    }
}

// Return Ok(true) to indicate Ok
// Return Ok(false) when client sender has close normally
// Err(e) when connection error
impl<'a, T, F> Future for ReciverTimerFuture<'a, T, F>
where
    T: RpcTask + Send + Unpin,
    F: Future<Output = Result<bool, ErrorClientSide>> + Unpin,
{
    type Output = Result<bool, ErrorClientSide>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        // In case ticker not fire, and ensure ticker schedule after ready
        while let Poll::Ready(_) = _self.inv.as_mut().poll_tick(ctx) {
            _self.client.time_reach();
        }
        if _self.client.has_err.load(Ordering::Acquire) {
            // When sentinel detect peer unreachable, recv_some mighe blocked, at least inv will
            // wait us, just exit
            return Poll::Ready(Err(ErrorClientSide::Static(ERR_RPC_CLOSED)));
        }
        _self.client.get_notifier_mut().poll_sent_task(ctx);
        // Even if receive future has block, we should poll_sent_task in order to detect timeout event
        if let Poll::Ready(r) = _self.recv_future.as_mut().poll(ctx) {
            return Poll::Ready(r);
        }
        return Poll::Pending;
    }
}
