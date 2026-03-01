use rocksdb::{Options, DB};
use std::sync::Arc;
use std::path::Path;
use anyhow::Result;

#[derive(Clone)]
pub struct KvStore {
    db: Arc<DB>,
}

impl KvStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // Optimize for L0/WAL behavior if needed, but defaults are fine for now
        let db = DB::open(&opts, path)?;
        Ok(Self {
            db: Arc::new(db),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value)?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let val = self.db.get(key)?;
        Ok(val)
    }

    pub fn multi_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        let results = self.db.multi_get(keys);
        let mut final_res = Vec::new();
        for res in results {
            final_res.push(res?);
        }
        Ok(final_res)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key)?;
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }

    pub fn checkpoint(&self, path: &std::path::Path) -> Result<()> {
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.db)?;
        checkpoint.create_checkpoint(path)?;
        Ok(())
    }

    pub fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Use an explicit seek iterator instead of prefix_iterator: prefix_iterator
        // requires a configured SliceTransform prefix extractor; without one its
        // behaviour is undefined and bloom filters are bypassed.
        use rocksdb::{IteratorMode, Direction};
        let iter = self.db.iterator(IteratorMode::From(prefix, Direction::Forward));
        let mut results = Vec::new();
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    }

    /// Count the number of keys with the given prefix without loading values.
    /// Avoids the deserialization cost of `scan` when only the count is needed.
    pub fn count_prefix(&self, prefix: &[u8]) -> Result<usize> {
        use rocksdb::{IteratorMode, Direction};
        let iter = self.db.iterator(IteratorMode::From(prefix, Direction::Forward));
        let mut count = 0;
        for item in iter {
            let (k, _) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    /// Scan keys in the range [start_key, end_key_exclusive) using a RocksDB seek.
    /// This is O(result_size) instead of O(total_keys_with_prefix).
    pub fn scan_range(&self, start_key: &[u8], end_key_exclusive: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use rocksdb::{IteratorMode, Direction};
        let iter = self.db.iterator(IteratorMode::From(start_key, Direction::Forward));
        let mut results = Vec::new();
        for item in iter {
            let (k, v) = item?;
            if k.as_ref() >= end_key_exclusive {
                break;
            }
            results.push((k.to_vec(), v.to_vec()));
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_kv_store() -> Result<()> {
        let temp_dir = tempdir()?;
        let kv = KvStore::open(temp_dir.path())?;

        kv.put(b"key1", b"value1")?;
        let val = kv.get(b"key1")?;
        assert_eq!(val, Some(b"value1".to_vec()));

        let missing = kv.get(b"key2")?;
        assert_eq!(missing, None);

        Ok(())
    }

    #[test]
    fn test_kv_multi_get_and_scan() -> Result<()> {
        let temp_dir = tempdir()?;
        let kv = KvStore::open(temp_dir.path())?;

        kv.put(b"a:1", b"val1")?;
        kv.put(b"a:2", b"val2")?;
        kv.put(b"b:1", b"val3")?;

        // Multi-get
        let results = kv.multi_get(&[b"a:1", b"a:3", b"b:1"])?;
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(b"val1".to_vec()));
        assert_eq!(results[1], None);
        assert_eq!(results[2], Some(b"val3".to_vec()));

        // Scan prefix 'a:'
        let a_results = kv.scan(b"a:")?;
        assert_eq!(a_results.len(), 2);
        assert_eq!(a_results[0].0, b"a:1".to_vec());
        assert_eq!(a_results[1].0, b"a:2".to_vec());

        // Scan prefix 'b:'
        let b_results = kv.scan(b"b:")?;
        assert_eq!(b_results.len(), 1);
        
        Ok(())
    }
}
