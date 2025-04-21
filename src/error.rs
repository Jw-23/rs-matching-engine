use thiserror::Error;

#[derive(Debug, Error)]
pub enum EgineError {
    /// 生产环境中务必直接panic
    #[error("缓冲区满了")]
    Full,
    #[error("缓冲区为空")]
    Empty,
    #[error("未知的订单Id")]
    UnkownOrderId(String),
    /// 生产环境中务必直接panic
    #[error("致命错误: {0}")]
    Fatal(String),
    #[error("绑定监听地址失败: {0}")]
    BindFailed(String),
    #[error("接收连接失败: {0}")]
    AcceptFailed(String),
    #[error("网络IO失败: {0}")]
    Io(String),
}
