use super::kv::KvStore;
use anyhow::Result;

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

    /// Scan keys with the given prefix, returning at most `limit` key-value pairs.
    /// More efficient than `scan()` when only a subset is needed, as it stops
    /// iterating once the limit is reached.
    pub fn scan_limited(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_limited(prefix, limit)
    }

    pub fn scan_range(
        &self,
        start_key: &[u8],
        end_key_exclusive: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_range(start_key, end_key_exclusive)
    }

    pub fn count_prefix(&self, prefix: &[u8]) -> Result<usize> {
        self.inner.count_prefix(prefix)
    }

    pub fn multi_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        self.inner.multi_get(keys)
    }

    pub fn checkpoint(&self, path: &std::path::Path) -> Result<()> {
        self.inner.checkpoint(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_system_kv_delegates_basic_operations() -> Result<()> {
        let temp_dir = tempdir()?;
        let checkpoint_root = tempdir()?;
        let checkpoint_dir = checkpoint_root.path().join("checkpoint");
        let inner = KvStore::open(temp_dir.path())?;
        let store = SystemKvStore::new(inner);

        store.put(b"sys:a", b"1")?;
        store.put(b"sys:b", b"2")?;
        store.put(b"other:c", b"3")?;

        assert_eq!(store.get(b"sys:a")?, Some(b"1".to_vec()));
        assert_eq!(store.count_prefix(b"sys:")?, 2);

        let multi = store.multi_get(&[b"sys:a", b"missing", b"sys:b"])?;
        assert_eq!(multi[0], Some(b"1".to_vec()));
        assert_eq!(multi[1], None);
        assert_eq!(multi[2], Some(b"2".to_vec()));

        let scan = store.scan(b"sys:")?;
        assert_eq!(scan.len(), 2);

        let range = store.scan_range(b"sys:a", b"sys:c")?;
        assert_eq!(range.len(), 2);

        store.delete(b"sys:b")?;
        assert_eq!(store.get(b"sys:b")?, None);

        store.checkpoint(&checkpoint_dir)?;
        let restored = KvStore::open(&checkpoint_dir)?;
        assert_eq!(restored.get(b"sys:a")?, Some(b"1".to_vec()));
        Ok(())
    }
}
