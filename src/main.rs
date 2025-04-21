use rs_matching_engine::{
    error::EgineError,
    tcp_server::{TcpOrderServer, WorkerBootstrapConfig, start_order_workers},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), EgineError> {
    let worker_runtime = start_order_workers(WorkerBootstrapConfig {
        worker_threads: 4,
        channel_cap: 1 << 12,
        trade_channel_cap: 1024,
        min_price: 1,
        max_price: 1_000_000,
        tick_size: 1,
        order_pool_cap: 1_000_000,
    })?;

    let server = TcpOrderServer::bind(
        "127.0.0.1:8080",
        worker_runtime.senders,
        worker_runtime.trade_tx,
    )
    .await?;

    let _worker_handles = worker_runtime.handles;
    server.run().await
}
