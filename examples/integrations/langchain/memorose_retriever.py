"""
LangChain Integration for Memorose

This module provides a custom LangChain BaseRetriever that connects to the
Memorose Cognitive Memory Engine. It allows AI Agents to automatically 
retrieve context from the user's long-term memory (L0/L1/L2) during conversation.

Setup:
    pip install langchain-core requests
"""

import requests
from typing import List, Optional, Any, Dict
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
    user_id: str = "default_user"
    stream_id: str = "default_stream"
    top_k: int = 5
    org_id: Optional[str] = None
    dashboard_token: Optional[str] = None
    
    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> List[Document]:
        """
        Query the Memorose engine and return the results as LangChain Documents.
        """
        endpoint = f"{self.api_url}/v1/users/{self.user_id}/streams/{self.stream_id}/retrieve"
        
        headers = {
            "Content-Type": "application/json",
        }
        
        payload = {
            "query": query,
            "limit": self.top_k
        }
        if self.org_id:
            payload["org_id"] = self.org_id
        
        try:
            response = requests.post(endpoint, json=payload, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            results = data.get("results", [])
            
            documents = []
            for item in results:
                # Memorose might return [unit, score] or just the dict
                content = ""
                metadata = {"source": "memorose", "user_id": self.user_id}
                
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
        endpoint = f"{self.api_url}/v1/users/{self.user_id}/streams/{self.stream_id}/events"
        
        headers = {
            "Content-Type": "application/json",
        }
        
        payload = {
            "content": content,
            "content_type": "text",
        }
        
        try:
            response = requests.post(endpoint, json=payload, headers=headers)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Error saving memory to Memorose: {e}")
            return False

    def semantic_preview(
        self,
        instruction: str,
        *,
        mode: str = "auto",
        forget_mode: str = "logical",
        limit: int = 10,
    ) -> Dict[str, Any]:
        endpoint = f"{self.api_url}/v1/users/{self.user_id}/memories/semantic/preview"
        headers = {"Content-Type": "application/json"}
        payload: Dict[str, Any] = {
            "instruction": instruction,
            "mode": mode,
            "forget_mode": forget_mode,
            "limit": limit,
        }
        if self.org_id:
            payload["org_id"] = self.org_id
        response = requests.post(endpoint, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()

    def semantic_execute(
        self,
        plan_id: str,
        *,
        reviewer: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        endpoint = f"{self.api_url}/v1/users/{self.user_id}/memories/semantic/execute"
        headers = {"Content-Type": "application/json"}
        payload: Dict[str, Any] = {
            "plan_id": plan_id,
            "confirm": True,
        }
        if self.org_id:
            payload["org_id"] = self.org_id
        if reviewer:
            payload["reviewer"] = reviewer
        if note:
            payload["note"] = note
        response = requests.post(endpoint, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()

    def semantic_update(self, instruction: str, **kwargs: Any) -> Dict[str, Any]:
        preview = self.semantic_preview(instruction, mode="update")
        execute = self.semantic_execute(preview["plan_id"], **kwargs)
        return {"preview": preview, "execute": execute}

    def semantic_forget(
        self,
        instruction: str,
        *,
        forget_mode: str = "logical",
        **kwargs: Any,
    ) -> Dict[str, Any]:
        preview = self.semantic_preview(
            instruction,
            mode="forget",
            forget_mode=forget_mode,
        )
        execute = self.semantic_execute(preview["plan_id"], **kwargs)
        return {"preview": preview, "execute": execute}

# Example Usage
if __name__ == "__main__":
    # Initialize the retriever
    retriever = MemoroseRetriever(
        api_url="http://localhost:3000",
        user_id="user_123",
        stream_id="session_abc",
        top_k=3,
        dashboard_token="optional-dashboard-jwt-for-review-observability",
    )
    
    print("Saving a memory...")
    retriever.save_memory("My favorite programming language is Rust because of its memory safety.")
    
    print("Retrieving memories...")
    docs = retriever.invoke("What language do I like?")

    print("\nRunning semantic update...")
    print(retriever.semantic_update("I now prefer Go over Rust.", reviewer="langchain-demo"))
    
    for i, doc in enumerate(docs):
        print(f"\nResult {i+1}:")
        print(f"Content: {doc.page_content}")
        print(f"Metadata: {doc.metadata}")
