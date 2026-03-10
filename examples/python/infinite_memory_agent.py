import requests
import json
import uuid
import time
import os

# ANSI Colors for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

MEMOROSE_URL = os.getenv("MEMOROSE_URL", "http://localhost:3000")
TENANT_ID = "infinite-agent-demo"
STREAM_ID = "session-001"

def print_agent(msg):
    print(f"\n{Colors.CYAN}🤖 Agent:{Colors.ENDC} {msg}")

def print_system(msg):
    print(f"{Colors.WARNING}⚙️  System:{Colors.ENDC} {msg}")

def save_memory(content):
    """Saves a user interaction into Memorose (L0 Layer)"""
    url = f"{MEMOROSE_URL}/v1/streams/{STREAM_ID}/events"
    headers = {"x-tenant-id": TENANT_ID, "Content-Type": "application/json"}
    payload = {"tenant_id": TENANT_ID, "content": content}
    
    try:
        resp = requests.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            print_system(f"Memory saved to Hippocampus (L0): {content[:30]}...")
        else:
            print_system(f"Failed to save memory: {resp.text}")
    except Exception as e:
        print_system(f"Connection error: {e}")

def recall_memory(query):
    """Retrieves relevant context from Memorose (L1/L2 Layer)"""
    url = f"{MEMOROSE_URL}/v1/streams/{STREAM_ID}/retrieve"
    headers = {"x-tenant-id": TENANT_ID, "Content-Type": "application/json"}
    payload = {"query": query}
    
    try:
        resp = requests.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                memories = []
                for item in results:
                    # Parse based on expected tuple format [unit, score] or raw dict
                    if isinstance(item, list) and len(item) == 2:
                        unit = item[0]
                        memories.append(unit.get('content', ''))
                    elif isinstance(item, dict):
                        memories.append(item.get('content', ''))
                    else:
                        memories.append(str(item))
                
                print_system(f"Recalled {len(memories)} relevant memories from past sessions.")
                return "\n".join(memories)
            return None
    except Exception as e:
        print_system(f"Recall error: {e}")
    return None

def simulate_chat():
    print(f"{Colors.HEADER}{Colors.BOLD}================================================={Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}   🧠 Akashic: Infinite Memory Agent Demo        {Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}================================================={Colors.ENDC}")
    print(f"{Colors.GREEN}Type 'quit' to exit. Type 'wait' to simulate time passing (consolidation).{Colors.ENDC}\n")
    
    # Optional: Mock LLM response generation (since we don't have a real LLM hooked up to this script)
    # In reality, you'd pass the recalled memory as system context to OpenAI/Gemini here.
    
    while True:
        try:
            user_input = input(f"\n{Colors.BLUE}🧑 You:{Colors.ENDC} ")
            if user_input.lower() in ['quit', 'exit']:
                break
            
            if user_input.lower() == 'wait':
                print_system("Time passes... Memorose background workers are consolidating L0 -> L1 and extracting L2 Graphs.")
                time.sleep(2)
                continue

            # 1. Before answering, the Agent searches its memory for context
            print_system("Searching memories for context...")
            past_context = recall_memory(user_input)
            
            # 2. Agent formulates response (Mocked logic for demo purposes)
            if past_context:
                print_agent(f"Based on what I remember:\n{Colors.WARNING}[Memory Context: {past_context}]{Colors.ENDC}\n\nI understand! I've incorporated your past preferences.")
            else:
                print_agent("I don't have any past memories about this. I'll remember this for next time!")
                
            # 3. Save the new interaction to memory
            save_memory(f"User stated: {user_input}")

        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    simulate_chat()
