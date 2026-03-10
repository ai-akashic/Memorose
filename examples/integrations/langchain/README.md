# LangChain + Memorose Integration

This example demonstrates how to integrate Memorose into your LangChain applications as a custom `BaseRetriever`. 

By plugging Memorose into LangChain, your agents instantly gain access to a long-term, cognitive memory engine capable of consolidating events and understanding semantic relationships.

## Quickstart

### 1. Requirements

Ensure you have the required packages installed:
```bash
pip install langchain-core requests
```

### 2. Usage

Simply import the custom `MemoroseRetriever` into your LangChain workflow. 

```python
from memorose_retriever import MemoroseRetriever
from langchain.chains import RetrievalQA
from langchain.llms import OpenAI

# 1. Initialize the Retriever connecting to your Memorose Cluster
retriever = MemoroseRetriever(
    api_url="http://localhost:3000",
    tenant_id="user_john_doe",
    stream_id="session_001",
    top_k=5
)

# 2. Automatically save events/context into the L0 buffer
retriever.save_memory("John loves drinking oat milk lattes.")
retriever.save_memory("John works as a Senior Rust Engineer.")

# 3. Use it in standard LangChain constructs
llm = OpenAI()
qa_chain = RetrievalQA.from_chain_type(llm=llm, retriever=retriever)

response = qa_chain.run("What kind of coffee should I buy for John?")
print(response) # "You should buy him an oat milk latte."
```

## Why use Memorose as a Retriever?

Traditional LangChain retrievers (like Pinecone, Chroma, etc.) treat all inserted text equally and do not "forget" or "consolidate". 

When using `MemoroseRetriever`, every time you call `save_memory()`, the event goes into the L0 working memory. In the background, the Memorose Rust engine automatically extracts concepts, builds knowledge graphs, and applies access decay to older, irrelevant memories, returning the most semantically compressed context to your LLM.
