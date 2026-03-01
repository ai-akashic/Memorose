use sha2::{Sha256, Digest};

/// Hash a user_id to a shard index in [0, shard_count).
pub fn user_id_to_shard(user_id: &str, shard_count: u32) -> u32 {
    if shard_count <= 1 {
        return 0;
    }
    let hash = Sha256::digest(user_id.as_bytes());
    // Use 4 bytes (32 bits) for ~4 billion pre-modulo values, giving much
    // better distribution than the single-byte approach (256 buckets).
    u32::from_le_bytes([hash[0], hash[1], hash[2], hash[3]]) % shard_count
}

/// Encode (shard_id, physical_node_id) into a single Raft node ID.
/// Layout: `shard_id * 1000 + physical_node_id`
/// Panics if `physical_node_id >= 1000` to prevent ID space collisions.
pub fn encode_raft_node_id(shard_id: u32, physical_node_id: u32) -> u64 {
    assert!(
        physical_node_id < 1000,
        "physical_node_id must be < 1000 (got {}); the encoding scheme reserves 3 digits for it",
        physical_node_id
    );
    shard_id as u64 * 1000 + physical_node_id as u64
}

/// Decode a Raft node ID back into (shard_id, physical_node_id).
pub fn decode_raft_node_id(raft_node_id: u64) -> (u32, u32) {
    let shard_id = (raft_node_id / 1000) as u32;
    let physical_node_id = (raft_node_id % 1000) as u32;
    (shard_id, physical_node_id)
}

/// Compute the Raft gRPC address for a given shard on a base port.
/// Uses saturating addition to prevent u16 overflow on large shard IDs.
pub fn raft_addr_for_shard(host: &str, base_port: u16, shard_id: u32) -> String {
    let port = (base_port as u32 + shard_id).min(u16::MAX as u32) as u16;
    format!("{}:{}", host, port)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_id_to_shard_deterministic() {
        let shard_count = 4;
        let s1 = user_id_to_shard("alice", shard_count);
        let s2 = user_id_to_shard("alice", shard_count);
        assert_eq!(s1, s2);
        assert!(s1 < shard_count);
    }

    #[test]
    fn test_user_id_to_shard_single() {
        assert_eq!(user_id_to_shard("anything", 1), 0);
        assert_eq!(user_id_to_shard("anything", 0), 0);
    }

    #[test]
    fn test_encode_decode_raft_node_id() {
        assert_eq!(encode_raft_node_id(0, 1), 1);
        assert_eq!(encode_raft_node_id(1, 1), 1001);
        assert_eq!(encode_raft_node_id(1, 2), 1002);
        assert_eq!(encode_raft_node_id(3, 5), 3005);

        assert_eq!(decode_raft_node_id(1), (0, 1));
        assert_eq!(decode_raft_node_id(1001), (1, 1));
        assert_eq!(decode_raft_node_id(1002), (1, 2));
        assert_eq!(decode_raft_node_id(3005), (3, 5));
    }

    #[test]
    fn test_raft_addr_for_shard() {
        assert_eq!(raft_addr_for_shard("127.0.0.1", 5000, 0), "127.0.0.1:5000");
        assert_eq!(raft_addr_for_shard("127.0.0.1", 5000, 1), "127.0.0.1:5001");
        assert_eq!(raft_addr_for_shard("0.0.0.0", 6000, 3), "0.0.0.0:6003");
    }
}
