<div align="center">
  <img src=".github/assets/logo.png" alt="Memorose Logo" width="200"/>

  <h1>🧠 Memorose</h1>
  <p><b>The High-Performance Cognitive Memory Engine for AI Agents. Built in Rust.</b></p>
  
  <p>
    <em>"Stat rosa pristina nomine, nomina nuda tenemus."</em><br/>
    <em>"The rose of old remains only in its name; we possess naked names."</em>
  </p>

  <p>
    <a href="https://github.com/akashic/memorose/stargazers"><img src="https://img.shields.io/github/stars/akashic/memorose?style=for-the-badge&color=yellow" alt="Stars" /></a>
    <a href="https://github.com/akashic/memorose/network/members"><img src="https://img.shields.io/github/forks/akashic/memorose?style=for-the-badge" alt="Forks" /></a>
    <a href="https://github.com/akashic/memorose/blob/main/LICENSE"><img src="https://img.shields.io/github/license/akashic/memorose?style=for-the-badge" alt="License" /></a>
    <img src="https://img.shields.io/badge/Rust-1.70+-orange?style=for-the-badge&logo=rust" alt="Rust" />
  </p>
</div>

---

## 💡 Why Memorose? (Vector DBs are not enough)

Current AI agents suffer from "anterograde amnesia". Once the context window fills up, they forget everything. While Retrieval-Augmented Generation (RAG) and Vector Databases offer a band-aid, they merely store isolated chunks of text. **They don't consolidate, they don't abstract, and they don't truly *understand* relationships.**

**Memorose is built differently.** Inspired by human cognitive architecture (*Nested Learning* & *G-Memory*), it is a hybrid memory database that seamlessly blends Key-Value (RocksDB), Vector (LanceDB), and Full-Text (Tantivy) storage into a single, blazing-fast Rust engine.

Instead of just storing data, Memorose **perceives, consolidates, and actively forgets**—giving your LLM agents true human-like long-term memory.

---

## ✨ Core Architecture: The 3-Tier Cognitive Model

Memorose implements a hierarchical memory consolidation system:

1. ⚡ **L0 - Working Memory (Perception)**: Fast, append-only sensory log. Captures every interaction in real-time with zero friction (Powered by `SimpleMem` design).
2. 🔄 **L1 - Episodic Memory (Consolidation)**: Asynchronous background processes evaluate L0 logs. Important interactions are vectorized and contextually linked. Noise is filtered via **Active Entropy Forgetting**.
3. 🕸️ **L2 - Semantic Memory (Abstraction)**: The deepest layer. Memorose automatically abstracts L1 episodes into a dynamic **Knowledge Graph** (*G-Memory*), extracting rules, beliefs, and high-level concepts for the agent.

---

## 🛠️ Hardcore Systems Engineering

We didn't just build a smart concept; we built planet-scale infrastructure.

- **Blazing Fast (Rust Native)**: Sub-millisecond latency for memory retrieval. Built on top of `RocksDB`, `Lance`, and `Tantivy`.
- **Multi-Group Raft Consensus**: Truly distributed and highly available. Separate consensus for pure storage vs. heavy computation (LLM/Graph summarization) to prevent leader bottlenecking.
- **Strict Multi-Tenancy**: Tenant-key prefix isolation makes it production-ready for SaaS platforms hosting millions of distinct agents.
- **Hybrid Search**: Query by semantic similarity, temporal proximity, or graph relationship—all through a single unified API.

---

## ⏱️ 5-Minute Quickstart

Stop building fragile RAG pipelines. Give your agent a brain in 5 lines of code.

*(Note: Ensure you have Docker and Python installed)*

### 1. Start the Memorose Engine

You can spin up a local instance of the Memorose node instantly using Docker:

```bash
docker run -d -p 8080:8080 akashic/memorose:latest
```

### 2. Install the SDK

```bash
pip install memorose-sdk
```

### 3. Give your Agent a Memory

```python
from memorose import MemoroseClient

# Initialize the memory engine
brain = MemoroseClient(url="http://localhost:8080", tenant_id="agent_007")

# Write an observation (Goes to L0)
brain.perceive("The user, Dylan, prefers his code in Rust and hates unnecessary meetings.")

# ... Days later, in a new session ...

# Retrieve context for the LLM
context = brain.recall("What should I keep in mind when scheduling a project with Dylan?")

print(context)
# Output: [Semantic L2 Graph] Dylan -> prefers -> Rust
# Output: [Semantic L2 Graph] Dylan -> hates -> Meetings
```

---

## 📚 Machine-Readable Docs (AI-First)

Memorose is designed for AI. Not only does it power AI memory, but its entire documentation is available in `llms.txt` format. Point your agent to `https://memorose.io/llms.txt` and let it learn how to use Memorose autonomously!

## 🤝 Contributing & Community

We are building the future of AI infrastructure, and we need your help. 

- **Read the Docs**: Check out our [Architecture Guide](docs/architecture.md) and [Contribution Guidelines](CONTRIBUTING.md).
- **Join the Conversation**: Join our Discord / Slack (coming soon) or open a discussion on GitHub.
- **Report Bugs & Suggest Features**: Use the GitHub Issues tab.

*If you believe in giving AI a true memory, please give us a ⭐️!*
