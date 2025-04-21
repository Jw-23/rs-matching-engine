use std::{net::SocketAddr, sync::Arc};

use async_trait::async_trait;

use crate::error::EgineError;

#[cfg(all(feature = "std-net", feature = "tokio-net"))]
compile_error!("features std-net and tokio-net cannot be enabled at the same time");

#[cfg(not(any(feature = "std-net", feature = "tokio-net")))]
compile_error!("one of features std-net or tokio-net must be enabled");

#[async_trait]
pub trait TcpConnection: Send {
	async fn read(&mut self, buf: &mut [u8]) -> Result<usize, EgineError>;
	async fn write_all(&mut self, buf: &[u8]) -> Result<(), EgineError>;
	async fn flush(&mut self) -> Result<(), EgineError>;
	async fn shutdown(&mut self) -> Result<(), EgineError>;
	fn peer_addr(&self) -> Result<SocketAddr, EgineError>;
}

#[async_trait]
pub trait TcpListener: Send + Sync {
	type Connection: TcpConnection + Send + 'static;

	async fn bind(addr: &str) -> Result<Self, EgineError>
	where
		Self: Sized;

	async fn accept(&self) -> Result<(Self::Connection, SocketAddr), EgineError>;
	fn local_addr(&self) -> Result<SocketAddr, EgineError>;
}

#[async_trait]
pub trait ConnectionHandler<C>: Send + Sync + 'static
where
	C: TcpConnection + Send + 'static,
{
	async fn on_connect(&self, _peer: SocketAddr) -> Result<(), EgineError> {
		Ok(())
	}

	async fn handle_connection(&self, conn: C) -> Result<(), EgineError>;

	async fn on_disconnect(&self, _peer: SocketAddr) -> Result<(), EgineError> {
		Ok(())
	}
}

pub async fn run_server<L, H>(addr: &str, handler: Arc<H>) -> Result<(), EgineError>
where
	L: TcpListener + 'static,
	H: ConnectionHandler<L::Connection>,
{
	let listener = L::bind(addr).await?;
	loop {
		let (conn, peer) = listener.accept().await?;
		let handler = Arc::clone(&handler);
		tokio::spawn(async move {
			if let Err(err) = handler.on_connect(peer).await {
				eprintln!("connection start hook failed({peer}): {err}");
				return;
			}

			if let Err(err) = handler.handle_connection(conn).await {
				eprintln!("connection handling failed({peer}): {err}");
			}

			if let Err(err) = handler.on_disconnect(peer).await {
				eprintln!("connection end hook failed({peer}): {err}");
			}
		});
	}
}

#[cfg(feature = "std-net")]
pub struct StdTcpConnection {
	inner: std::net::TcpStream,
}

#[cfg(feature = "std-net")]
impl StdTcpConnection {
	pub fn new(inner: std::net::TcpStream) -> Self {
		Self { inner }
	}
}

#[cfg(feature = "std-net")]
#[async_trait]
impl TcpConnection for StdTcpConnection {
	async fn read(&mut self, buf: &mut [u8]) -> Result<usize, EgineError> {
		use std::io::Read;
		self.inner
			.read(buf)
			.map_err(|e| EgineError::Io(e.to_string()))
	}

	async fn write_all(&mut self, buf: &[u8]) -> Result<(), EgineError> {
		use std::io::Write;
		self.inner
			.write_all(buf)
			.map_err(|e| EgineError::Io(e.to_string()))
	}

	async fn flush(&mut self) -> Result<(), EgineError> {
		use std::io::Write;
		self.inner
			.flush()
			.map_err(|e| EgineError::Io(e.to_string()))
	}

	async fn shutdown(&mut self) -> Result<(), EgineError> {
		self.inner
			.shutdown(std::net::Shutdown::Both)
			.map_err(|e| EgineError::Io(e.to_string()))
	}

	fn peer_addr(&self) -> Result<SocketAddr, EgineError> {
		self.inner
			.peer_addr()
			.map_err(|e| EgineError::Io(e.to_string()))
	}
}

#[cfg(feature = "std-net")]
pub struct StdTcpListener {
	inner: std::net::TcpListener,
}

#[cfg(feature = "std-net")]
#[async_trait]
impl TcpListener for StdTcpListener {
	type Connection = StdTcpConnection;

	async fn bind(addr: &str) -> Result<Self, EgineError> {
		let listener = std::net::TcpListener::bind(addr)
			.map_err(|e| EgineError::BindFailed(e.to_string()))?;
		Ok(Self { inner: listener })
	}

	async fn accept(&self) -> Result<(Self::Connection, SocketAddr), EgineError> {
		let (stream, peer) = self
			.inner
			.accept()
			.map_err(|e| EgineError::AcceptFailed(e.to_string()))?;
		Ok((StdTcpConnection::new(stream), peer))
	}

	fn local_addr(&self) -> Result<SocketAddr, EgineError> {
		self.inner
			.local_addr()
			.map_err(|e| EgineError::BindFailed(e.to_string()))
	}
}

#[cfg(feature = "tokio-net")]
pub struct TokioTcpConnection {
	inner: tokio::net::TcpStream,
}

#[cfg(feature = "tokio-net")]
impl TokioTcpConnection {
	pub fn new(inner: tokio::net::TcpStream) -> Self {
		Self { inner }
	}
}

#[cfg(feature = "tokio-net")]
#[async_trait]
impl TcpConnection for TokioTcpConnection {
	async fn read(&mut self, buf: &mut [u8]) -> Result<usize, EgineError> {
		use tokio::io::AsyncReadExt;
		self.inner
			.read(buf)
			.await
			.map_err(|e| EgineError::Io(e.to_string()))
	}

	async fn write_all(&mut self, buf: &[u8]) -> Result<(), EgineError> {
		use tokio::io::AsyncWriteExt;
		self.inner
			.write_all(buf)
			.await
			.map_err(|e| EgineError::Io(e.to_string()))
	}

	async fn flush(&mut self) -> Result<(), EgineError> {
		use tokio::io::AsyncWriteExt;
		self.inner
			.flush()
			.await
			.map_err(|e| EgineError::Io(e.to_string()))
	}

	async fn shutdown(&mut self) -> Result<(), EgineError> {
		use tokio::io::AsyncWriteExt;
		self.inner
			.shutdown()
			.await
			.map_err(|e| EgineError::Io(e.to_string()))
	}

	fn peer_addr(&self) -> Result<SocketAddr, EgineError> {
		self.inner
			.peer_addr()
			.map_err(|e| EgineError::Io(e.to_string()))
	}
}

#[cfg(feature = "tokio-net")]
pub struct TokioTcpListener {
	inner: tokio::net::TcpListener,
}

#[cfg(feature = "tokio-net")]
#[async_trait]
impl TcpListener for TokioTcpListener {
	type Connection = TokioTcpConnection;

	async fn bind(addr: &str) -> Result<Self, EgineError> {
		let listener = tokio::net::TcpListener::bind(addr)
			.await
			.map_err(|e| EgineError::BindFailed(e.to_string()))?;
		Ok(Self { inner: listener })
	}

	async fn accept(&self) -> Result<(Self::Connection, SocketAddr), EgineError> {
		let (stream, peer) = self
			.inner
			.accept()
			.await
			.map_err(|e| EgineError::AcceptFailed(e.to_string()))?;
		Ok((TokioTcpConnection::new(stream), peer))
	}

	fn local_addr(&self) -> Result<SocketAddr, EgineError> {
		self.inner
			.local_addr()
			.map_err(|e| EgineError::BindFailed(e.to_string()))
	}
}

#[cfg(feature = "std-net")]
pub type ActiveTcpListener = StdTcpListener;

#[cfg(feature = "tokio-net")]
pub type ActiveTcpListener = TokioTcpListener;

#[cfg(test)]
mod tests {
	use super::{ActiveTcpListener, TcpConnection, TcpListener};

	#[cfg(feature = "tokio-net")]
	#[tokio::test]
	async fn listener_accept_and_read_smoke() {
		let listener = <ActiveTcpListener as TcpListener>::bind("127.0.0.1:0")
			.await
			.expect("bind listener");
		let addr = listener.local_addr().expect("read local addr");

		let client = tokio::spawn(async move {
			use tokio::io::AsyncWriteExt;
			let mut stream = tokio::net::TcpStream::connect(addr)
				.await
				.expect("connect server");
			stream.write_all(b"ping").await.expect("write ping");
		});

		let (mut conn, _) = listener.accept().await.expect("accept client");
		let mut buf = [0u8; 4];
		let read = conn.read(&mut buf).await.expect("read payload");

		assert_eq!(read, 4);
		assert_eq!(&buf, b"ping");
		client.await.expect("join client");
	}

	#[cfg(feature = "std-net")]
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn listener_accept_and_read_smoke() {
		use std::io::Write;

		let listener = <ActiveTcpListener as TcpListener>::bind("127.0.0.1:0")
			.await
			.expect("bind listener");
		let addr = listener.local_addr().expect("read local addr");

		let client = std::thread::spawn(move || {
			let mut stream = std::net::TcpStream::connect(addr).expect("connect server");
			stream.write_all(b"ping").expect("write ping");
		});

		let (mut conn, _) = listener.accept().await.expect("accept client");
		let mut buf = [0u8; 4];
		let read = conn.read(&mut buf).await.expect("read payload");

		assert_eq!(read, 4);
		assert_eq!(&buf, b"ping");
		client.join().expect("join client");
	}
}
