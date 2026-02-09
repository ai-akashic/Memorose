use anyhow::Result;
use super::kv::KvStore;

#[derive(Clone)]
pub struct SystemKvStore {
    inner: KvStore,
}

impl SystemKvStore {
    pub fn new(inner: KvStore) -> Self {
        Self { inner }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan(prefix)
    }

    pub fn multi_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        self.inner.multi_get(keys)
    }

    pub fn checkpoint(&self, path: &std::path::Path) -> Result<()> {
        self.inner.checkpoint(path)
    }
}
