use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use tokio::sync;

pub struct TableLocks {
    locks: sync::Mutex<HashMap<String, Arc<sync::Mutex<()>>>>,
}

impl TableLocks {
    fn new() -> Self {
        TableLocks {
            locks: sync::Mutex::new(HashMap::new()),
        }
    }

    pub async fn lock(&self, table: &str) -> sync::OwnedMutexGuard<()> {
        let mut map = self.locks.lock().await;
        let mutex = map
            .entry(table.to_string())
            .or_insert_with(|| Arc::new(sync::Mutex::new(())))
            .clone();
        drop(map);
        mutex.lock_owned().await
    }
}

static TABLE_LOCKS: OnceLock<TableLocks> = OnceLock::new();

pub fn table_locks() -> &'static TableLocks {
    TABLE_LOCKS.get_or_init(TableLocks::new)
}
