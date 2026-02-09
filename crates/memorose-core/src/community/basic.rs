use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use memorose_common::GraphEdge;
use rand::seq::SliceRandom;
use rand::thread_rng;

pub struct CommunityDetector;

impl CommunityDetector {
    /// Detects communities using the Label Propagation Algorithm (LPA).
    /// Returns a map of NodeId -> CommunityId (where CommunityId is a Uuid representative).
    pub fn detect_communities(edges: &[GraphEdge]) -> HashMap<Uuid, Uuid> {
        let mut communities: HashMap<Uuid, Uuid> = HashMap::new();
        let mut adjacency: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
        let mut all_nodes: HashSet<Uuid> = HashSet::new();

        // Build adjacency list and initialize communities
        for edge in edges {
            adjacency.entry(edge.source_id).or_default().push(edge.target_id);
            adjacency.entry(edge.target_id).or_default().push(edge.source_id); // Undirected for community detection
            
            all_nodes.insert(edge.source_id);
            all_nodes.insert(edge.target_id);
        }

        // Init: each node is its own community
        for node in &all_nodes {
            communities.insert(*node, *node);
        }

        let mut nodes_vec: Vec<Uuid> = all_nodes.into_iter().collect();
        let mut rng = thread_rng();
        let max_iterations = 10;

        for _ in 0..max_iterations {
            nodes_vec.shuffle(&mut rng);
            let mut changed = false;

            for &node in &nodes_vec {
                if let Some(neighbors) = adjacency.get(&node) {
                    let mut label_counts: HashMap<Uuid, usize> = HashMap::new();
                    
                    for neighbor in neighbors {
                        if let Some(label) = communities.get(neighbor) {
                            *label_counts.entry(*label).or_default() += 1;
                        }
                    }

                    // Find max label
                    if let Some((best_label, _)) = label_counts.iter().max_by_key(|&(_, count)| count) {
                        if let Some(current_label) = communities.get(&node) {
                            if current_label != best_label {
                                communities.insert(node, *best_label);
                                changed = true;
                            }
                        }
                    }
                }
            }

            if !changed {
                break;
            }
        }

        communities
    }
}
