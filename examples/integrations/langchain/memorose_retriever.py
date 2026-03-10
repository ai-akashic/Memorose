"""
LangChain Integration for Memorose

This module provides a custom LangChain BaseRetriever that connects to the
Memorose Cognitive Memory Engine. It allows AI Agents to automatically 
retrieve context from the user's long-term memory (L0/L1/L2) during conversation.

Setup:
    pip install langchain-core requests
"""

import requests
from typing import List, Optional, Any
from langchain_core.retrievers import BaseRetriever
from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.documents import Document

class MemoroseRetriever(BaseRetriever):
    """
    A custom retriever for LangChain that queries the Memorose Cognitive Engine.
    
    This retriever acts as the 'external hippocampus' for your LangChain agents,
    fetching semantically relevant memories from the user's past interactions.
    """
    
    api_url: str = "http://localhost:3000"
    tenant_id: str = "default_tenant"
    stream_id: str = "default_stream"
    top_k: int = 5
    
    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> List[Document]:
        """
        Query the Memorose engine and return the results as LangChain Documents.
        """
        endpoint = f"{self.api_url}/v1/streams/{self.stream_id}/retrieve"
        
        headers = {
            "Content-Type": "application/json",
            "x-tenant-id": self.tenant_id
        }
        
        payload = {
            "query": query,
            "top_k": self.top_k
        }
        
        try:
            response = requests.post(endpoint, json=payload, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            results = data.get("results", [])
            
            documents = []
            for item in results:
                # Memorose might return [unit, score] or just the dict
                content = ""
                metadata = {"source": "memorose", "tenant_id": self.tenant_id}
                
                if isinstance(item, list) and len(item) == 2:
                    unit, score = item
                    content = unit.get("content", "")
                    metadata["score"] = score
                    metadata["type"] = unit.get("type", "unknown") # L0, L1, or L2
                elif isinstance(item, dict):
                    content = item.get("content", "")
                    metadata["type"] = item.get("type", "unknown")
                else:
                    content = str(item)
                    
                if content:
                    doc = Document(page_content=content, metadata=metadata)
                    documents.append(doc)
                    
            return documents
            
        except requests.exceptions.RequestException as e:
            print(f"Error querying Memorose: {e}")
            return []

    def save_memory(self, content: str) -> bool:
        """
        A helper method to ingest new memories back into Memorose (L0 layer).
        Typically called after the LLM generates a response or user provides input.
        """
        endpoint = f"{self.api_url}/v1/streams/{self.stream_id}/events"
        
        headers = {
            "Content-Type": "application/json",
            "x-tenant-id": self.tenant_id
        }
        
        payload = {
            "tenant_id": self.tenant_id,
            "content": content
        }
        
        try:
            response = requests.post(endpoint, json=payload, headers=headers)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Error saving memory to Memorose: {e}")
            return False

# Example Usage
if __name__ == "__main__":
    # Initialize the retriever
    retriever = MemoroseRetriever(
        api_url="http://localhost:3000",
        tenant_id="user_123",
        stream_id="session_abc",
        top_k=3
    )
    
    print("Saving a memory...")
    retriever.save_memory("My favorite programming language is Rust because of its memory safety.")
    
    print("Retrieving memories...")
    docs = retriever.invoke("What language do I like?")
    
    for i, doc in enumerate(docs):
        print(f"\nResult {i+1}:")
        print(f"Content: {doc.page_content}")
        print(f"Metadata: {doc.metadata}")
