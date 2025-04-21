use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static MACHINE_ID: std::sync::OnceLock<u8> = std::sync::OnceLock::new();

pub fn set_machine_id(id: u8) {
    MACHINE_ID.set(id).ok();
}

pub fn get_machine_id() -> u8 {
    *MACHINE_ID.get().unwrap_or(&0)
}

/// Returns seconds since midnight (0–86399)
fn seconds_today() -> u32 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let midnight = now - (now % 86400);
    (now - midnight) as u32
}

static DAILY_SEQ: AtomicU64 = AtomicU64::new(0);
static LAST_SECONDS_TODAY: AtomicU32 = AtomicU32::new(u32::MAX);

/// Generates a new order ID.
/// Layout: [machine(8) | seconds_of_day(17) | seq(39)]
pub fn next_order_id() -> u64 {
    let machine = get_machine_id() as u64;
    let today_secs = seconds_today();
    let last_secs = LAST_SECONDS_TODAY.load(Ordering::Relaxed);

    // Day changed → reset sequence
    if today_secs < last_secs {
        DAILY_SEQ.store(0, Ordering::Relaxed);
    }
    LAST_SECONDS_TODAY.store(today_secs, Ordering::Relaxed);

    let seq = DAILY_SEQ.fetch_add(1, Ordering::Relaxed);

    // Layout: [machine(8) | seconds(17) | seq(39)]
    (machine << 47) | ((today_secs as u64) << 30) | (seq & 0x7FFF_FFFF_FFFF)
}

/// Extract seconds-of-day from an order ID
pub fn id_to_seconds(id: u64) -> u32 {
    ((id >> 30) & 0x1FFFF) as u32
}
