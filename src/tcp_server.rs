use std::{
	collections::{HashMap, HashSet},
	num::NonZeroU64,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	thread::JoinHandle,
};

use tokio::{
	io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
	net::{TcpListener, TcpStream},
	sync::{RwLock, broadcast, mpsc},
};

use crate::{
	error::EgineError,
	mpmc::{Consumer, Producer, Sender, channel},
	order::{Order, OrderId, OrderType, Price, Side, SymbolId},
	order_book::{FlatOrderBook, Trade},
	worker::{OrderWorker, Worker},
};

pub struct WorkerBootstrapConfig {
	pub worker_threads: usize,
	pub channel_cap: usize,
	pub trade_channel_cap: usize,
	pub min_price: u64,
	pub max_price: u64,
	pub tick_size: u64,
	pub order_pool_cap: usize,
}

impl Default for WorkerBootstrapConfig {
	fn default() -> Self {
		Self {
			worker_threads: 4,
			channel_cap: 1 << 12,
			trade_channel_cap: 1024,
			min_price: 1,
			max_price: 1_000_000,
			tick_size: 1,
			order_pool_cap: 1_000_000,
		}
	}
}

pub struct WorkerRuntime {
	pub senders: Vec<Sender<Order>>,
	pub trade_tx: broadcast::Sender<Vec<Trade>>,
	pub handles: Vec<JoinHandle<()>>,
}

pub fn start_order_workers(cfg: WorkerBootstrapConfig) -> Result<WorkerRuntime, EgineError> {
	if cfg.worker_threads == 0 {
		return Err(EgineError::Fatal("worker_threads must be > 0".to_string()));
	}
	if cfg.min_price == 0 || cfg.tick_size == 0 || cfg.max_price <= cfg.min_price {
		return Err(EgineError::Fatal("invalid order book price range config".to_string()));
	}

	let core_ids = core_affinity::get_core_ids()
		.ok_or_else(|| EgineError::Fatal("cannot fetch cpu core ids".to_string()))?;
	if core_ids.len() < cfg.worker_threads {
		return Err(EgineError::Fatal(format!(
			"insufficient cpu cores: need {}, got {}",
			cfg.worker_threads,
			core_ids.len()
		)));
	}

	let (trade_tx, _) = broadcast::channel(cfg.trade_channel_cap);
	let mut senders = Vec::with_capacity(cfg.worker_threads);
	let mut handles = Vec::with_capacity(cfg.worker_threads);

	for worker_idx in 0..cfg.worker_threads {
		let core_id = core_ids[worker_idx];
		let (tx, rx) = channel(cfg.channel_cap);
		let trade_tx_clone = trade_tx.clone();
		let min_price = cfg.min_price;
		let max_price = cfg.max_price;
		let tick_size = cfg.tick_size;
		let order_pool_cap = cfg.order_pool_cap;

		let handle = std::thread::Builder::new()
			.name(format!("order-worker-{worker_idx}"))
			.spawn(move || {
				core_affinity::set_for_current(core_id);
				let book = FlatOrderBook::new(
					worker_idx as u64 + 1,
					min_price,
					max_price,
					tick_size,
					order_pool_cap,
				);
				let mut worker = OrderWorker::new(book, trade_tx_clone);

				loop {
					match rx.take() {
						Some(order) => worker.handle(order),
						None => std::hint::spin_loop(),
					}
				}
			})
			.map_err(|e| EgineError::Fatal(format!("spawn worker failed: {e}")))?;

		senders.push(tx);
		handles.push(handle);
	}

	Ok(WorkerRuntime {
		senders,
		trade_tx,
		handles,
	})
}

#[derive(Clone)]
struct ClientSink {
	client_id: u64,
	tx: mpsc::UnboundedSender<String>,
}

#[derive(Debug, Clone, Copy)]
enum ClientCommand {
	Buy {
		symbol_id: SymbolId,
		quantity: u64,
		price: u64,
		client_order_id: OrderId,
	},
	Sell {
		symbol_id: SymbolId,
		quantity: u64,
		price: u64,
		client_order_id: OrderId,
	},
	Subscribe {
		symbol_id: SymbolId,
	},
}

pub struct TcpOrderServer {
	listener: TcpListener,
	order_senders: Arc<Vec<Sender<Order>>>,
	trade_tx: broadcast::Sender<Vec<Trade>>,
	subscribers: Arc<RwLock<HashMap<SymbolId, Vec<ClientSink>>>>,
	next_order_id: Arc<AtomicU64>,
	next_client_id: Arc<AtomicU64>,
}

impl TcpOrderServer {
	pub async fn bind(
		addr: &str,
		order_senders: Vec<Sender<Order>>,
		trade_tx: broadcast::Sender<Vec<Trade>>,
	) -> Result<Self, EgineError> {
		if order_senders.is_empty() {
			return Err(EgineError::Fatal("at least one worker sender is required".to_string()));
		}

		let listener = TcpListener::bind(addr)
			.await
			.map_err(|e| EgineError::BindFailed(e.to_string()))?;

		Ok(Self {
			listener,
			order_senders: Arc::new(order_senders),
			trade_tx,
			subscribers: Arc::new(RwLock::new(HashMap::new())),
			next_order_id: Arc::new(AtomicU64::new(1)),
			next_client_id: Arc::new(AtomicU64::new(1)),
		})
	}

	pub fn local_addr(&self) -> Result<std::net::SocketAddr, EgineError> {
		self.listener
			.local_addr()
			.map_err(|e| EgineError::BindFailed(e.to_string()))
	}

	pub async fn run(self) -> Result<(), EgineError> {
		let trade_rx = self.trade_tx.subscribe();
		let fanout_subscribers = Arc::clone(&self.subscribers);
		tokio::spawn(async move {
			if let Err(err) = Self::fanout_trades(trade_rx, fanout_subscribers).await {
				eprintln!("trade fanout stopped: {err}");
			}
		});

		loop {
			let (stream, _) = self
				.listener
				.accept()
				.await
				.map_err(|e| EgineError::AcceptFailed(e.to_string()))?;

			let order_senders = Arc::clone(&self.order_senders);
			let subscribers = Arc::clone(&self.subscribers);
			let next_order_id = Arc::clone(&self.next_order_id);
			let client_id = self.next_client_id.fetch_add(1, Ordering::Relaxed);

			tokio::spawn(async move {
				if let Err(err) = Self::handle_client(
					stream,
					client_id,
					order_senders,
					subscribers,
					next_order_id,
				)
				.await
				{
					eprintln!("client {client_id} disconnected with error: {err}");
				}
			});
		}
	}

	async fn fanout_trades(
		mut trade_rx: broadcast::Receiver<Vec<Trade>>,
		subscribers: Arc<RwLock<HashMap<SymbolId, Vec<ClientSink>>>>,
	) -> Result<(), EgineError> {
		loop {
			let trades = match trade_rx.recv().await {
				Ok(trades) => trades,
				Err(broadcast::error::RecvError::Lagged(_)) => continue,
				Err(broadcast::error::RecvError::Closed) => return Ok(()),
			};

			for trade in trades {
				let symbol_id = trade.symbol_id;
				let msg = format!(
					"TRADE {} {} {}\n",
					symbol_id,
					Self::price_to_u64(trade.price),
					trade.quantity
				);

				let mut map = subscribers.write().await;
				if let Some(client_sinks) = map.get_mut(&symbol_id) {
					client_sinks.retain(|sink| sink.tx.send(msg.clone()).is_ok());
				}
			}
		}
	}

	async fn handle_client(
		stream: TcpStream,
		client_id: u64,
		order_senders: Arc<Vec<Sender<Order>>>,
		subscribers: Arc<RwLock<HashMap<SymbolId, Vec<ClientSink>>>>,
		next_order_id: Arc<AtomicU64>,
	) -> Result<(), EgineError> {
		let (reader_half, mut writer_half) = stream.into_split();
		let mut reader = BufReader::new(reader_half);
		let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
		let mut subscribed_symbols = HashSet::<SymbolId>::new();

		let writer_task = tokio::spawn(async move {
			while let Some(msg) = out_rx.recv().await {
				if writer_half.write_all(msg.as_bytes()).await.is_err() {
					break;
				}
			}
		});

		let mut line = String::new();
		loop {
			line.clear();
			let n = reader
				.read_line(&mut line)
				.await
				.map_err(|e| EgineError::Io(e.to_string()))?;

			if n == 0 {
				break;
			}

			let cmd = match Self::parse_command(line.trim()) {
				Ok(cmd) => cmd,
				Err(msg) => {
					let _ = out_tx.send(format!("ERROR {}\n", msg));
					continue;
				}
			};

			let response = match cmd {
				ClientCommand::Buy {
					symbol_id,
					quantity,
					price,
					client_order_id,
				} => Self::submit_order(
					&order_senders,
					&next_order_id,
					symbol_id,
					quantity,
					price,
					client_order_id,
					Side::Buy,
				),
				ClientCommand::Sell {
					symbol_id,
					quantity,
					price,
					client_order_id,
				} => Self::submit_order(
					&order_senders,
					&next_order_id,
					symbol_id,
					quantity,
					price,
					client_order_id,
					Side::Sell,
				),
				ClientCommand::Subscribe { symbol_id } => {
					if subscribed_symbols.insert(symbol_id) {
						let mut map = subscribers.write().await;
						map.entry(symbol_id).or_default().push(ClientSink {
							client_id,
							tx: out_tx.clone(),
						});
					}
					format!("SUBSCRIBED {}\n", symbol_id)
				}
			};

			let _ = out_tx.send(response);
		}

		Self::remove_client(client_id, &subscribers, &subscribed_symbols).await;
		drop(out_tx);
		let _ = writer_task.await;
		Ok(())
	}

	async fn remove_client(
		client_id: u64,
		subscribers: &Arc<RwLock<HashMap<SymbolId, Vec<ClientSink>>>>,
		subscribed_symbols: &HashSet<SymbolId>,
	) {
		let mut map = subscribers.write().await;
		for symbol_id in subscribed_symbols {
			if let Some(client_sinks) = map.get_mut(symbol_id) {
				client_sinks.retain(|sink| sink.client_id != client_id);
			}
		}
	}

	fn submit_order(
		order_senders: &Arc<Vec<Sender<Order>>>,
		next_order_id: &Arc<AtomicU64>,
		symbol_id: SymbolId,
		quantity: u64,
		price: u64,
		client_order_id: OrderId,
		side: Side,
	) -> String {
		let order_id = next_order_id.fetch_add(1, Ordering::Relaxed);
		let shard = (symbol_id as usize) % order_senders.len();
		let order = Order {
			id: order_id,
			price: Price(Some(NonZeroU64::new(price).expect("price validated before submit"))),
			client_order_id,
			symbol_id,
			quantity,
			side,
			ty: OrderType::Limit,
			active: true,
		};

		order_senders[shard].push(order);
		format!("ORDER_ACCEPTED_ASYNC {}\n", order_id)
	}

	fn parse_command(input: &str) -> Result<ClientCommand, String> {
		let mut parts = input.split_whitespace();
		let cmd = parts
			.next()
			.ok_or_else(|| "empty command".to_string())?
			.to_ascii_uppercase();

		match cmd.as_str() {
			"BUY" | "SELL" => {
				let symbol_id = parts
					.next()
					.ok_or_else(|| "missing symbol_id".to_string())?
					.parse::<u64>()
					.map_err(|_| "invalid symbol_id".to_string())?;
				let quantity = parts
					.next()
					.ok_or_else(|| "missing quantity".to_string())?
					.parse::<u64>()
					.map_err(|_| "invalid quantity".to_string())?;
				let price = parts
					.next()
					.ok_or_else(|| "missing price".to_string())?
					.parse::<u64>()
					.map_err(|_| "invalid price".to_string())?;
				let client_order_id = parts
					.next()
					.map(|v| v.parse::<u64>().map_err(|_| "invalid client_order_id".to_string()))
					.transpose()?
					.unwrap_or(0);

				if symbol_id == 0 {
					return Err("symbol_id must be > 0".to_string());
				}
				if quantity == 0 {
					return Err("quantity must be > 0".to_string());
				}
				if price == 0 {
					return Err("price must be > 0".to_string());
				}

				if cmd == "BUY" {
					Ok(ClientCommand::Buy {
						symbol_id,
						quantity,
						price,
						client_order_id,
					})
				} else {
					Ok(ClientCommand::Sell {
						symbol_id,
						quantity,
						price,
						client_order_id,
					})
				}
			}
			"SUBSCRIBE" => {
				let symbol_id = parts
					.next()
					.ok_or_else(|| "missing symbol_id".to_string())?
					.parse::<u64>()
					.map_err(|_| "invalid symbol_id".to_string())?;
				if symbol_id == 0 {
					return Err("symbol_id must be > 0".to_string());
				}
				Ok(ClientCommand::Subscribe { symbol_id })
			}
			_ => Err("unknown command".to_string()),
		}
	}

	fn price_to_u64(price: Price) -> u64 {
		price.0.map_or(0, NonZeroU64::get)
	}
}

