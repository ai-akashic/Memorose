#!/bin/bash

# Memorose Management Script
# Supports Start, Stop, Restart in Standalone or Cluster modes

set -e

# Configuration
SERVER_BIN="./target/debug/memorose-server"
LOG_DIR="./logs"
DATA_DIR="./data"

show_help() {
    echo "Memorose Management Script"
    echo ""
    echo "Usage: ./scripts/start_cluster.sh [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  start       Start the server(s)"
    echo "  stop        Stop all running server(s)"
    echo "  restart     Stop and then start the server(s)"
    echo ""
    echo "Options:"
    echo "  --mode [standalone|cluster]    Set execution mode (default: cluster)"
    echo "  --clean                        Remove data directory before start"
    echo "  --help                         Show this help"
    echo ""
    echo "Examples:"
    echo "  ./scripts/start_cluster.sh start --mode standalone"
    echo "  ./scripts/start_cluster.sh start --mode cluster --clean"
    echo "  ./scripts/start_cluster.sh stop"
}

stop_servers() {
    echo "Stopping all Memorose processes..."
    pkill memorose-server || true
    pkill cortexdb-server || true
}

wait_for_ready() {
    local port=$1
    local max_attempts=30
    for i in $(seq 1 $max_attempts); do
        if curl -sf "http://localhost:$port/" > /dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "ERROR: Node on port $port did not become ready in ${max_attempts}s"
    return 1
}

start_servers() {
    # Load .env file
    if [ -f ".env" ]; then
        echo "Loading .env configuration..."
        export $(grep -v '^#' .env | xargs)
    fi

    # Ensure binary exists
    if [ ! -f "$SERVER_BIN" ]; then
        echo "Compiling Memorose..."
        cargo build -p memorose-server
    fi

    # Check if dashboard is built
    if [ ! -d "./crates/memorose-server/static/dashboard" ]; then
        echo "Dashboard not built yet. Building..."
        ./scripts/build_dashboard.sh
    fi

    # Clean data if requested
    if [ "$CLEAN" = "true" ]; then
        echo "Cleaning data directory..."
        rm -rf "$DATA_DIR"
    fi

    mkdir -p "$LOG_DIR"

    if [ "$MODE" == "standalone" ]; then
        echo "Starting Memorose in STANDALONE mode..."
        echo "  Starting Node 1 (Port 3000)..."
        NODE_ID=1 PORT=3000 RAFT_ADDR=127.0.0.1:5001 $SERVER_BIN > "$LOG_DIR/standalone.log" 2>&1 &

        echo "  Waiting for node to be ready..."
        wait_for_ready 3000

        echo "  Initializing cluster..."
        INIT_RESULT=$(curl -sf -X POST http://localhost:3000/v1/cluster/initialize 2>&1) || true
        echo "  Initialize: $INIT_RESULT"

        echo ""
        echo "Memorose Standalone is READY!"
        echo "Endpoint:  http://localhost:3000"
        echo "Dashboard: http://localhost:3000/dashboard  (admin/admin)"

    elif [ "$MODE" == "cluster" ]; then
        echo "Starting Memorose in CLUSTER mode..."

        # Detect warm restart (data directory exists with node data)
        WARM_RESTART="false"
        if [ -d "$DATA_DIR/node-1" ] || [ -d "$DATA_DIR/shard_0" ]; then
            WARM_RESTART="true"
            echo "  Detected existing data — warm restart (Raft will reconnect automatically)"
        fi

        echo "  Starting Node 1 (Port 3000)..."
        NODE_ID=1 PORT=3000 RAFT_ADDR=127.0.0.1:5001 $SERVER_BIN > "$LOG_DIR/node1.log" 2>&1 &

        echo "  Starting Node 2 (Port 3001)..."
        NODE_ID=2 PORT=3001 RAFT_ADDR=127.0.0.1:5002 $SERVER_BIN > "$LOG_DIR/node2.log" 2>&1 &

        echo "  Starting Node 3 (Port 3002)..."
        NODE_ID=3 PORT=3002 RAFT_ADDR=127.0.0.1:5003 $SERVER_BIN > "$LOG_DIR/node3.log" 2>&1 &

        echo "  Waiting for all nodes to be ready..."
        wait_for_ready 3000
        wait_for_ready 3001
        wait_for_ready 3002

        if [ "$WARM_RESTART" == "true" ]; then
            echo "  Warm restart: waiting for Raft leader election..."
            for i in $(seq 1 15); do
                LEADER=$(curl -sf http://localhost:3000/ 2>/dev/null)
                # Check cluster status for leader
                STATUS=$(curl -sf http://localhost:3000/v1/cluster/initialize -X POST 2>/dev/null || true)
                if echo "$STATUS" | grep -q '"already_initialized"'; then
                    break
                fi
                sleep 1
            done
            echo "  Cluster resumed from persisted state."
        else
            echo "  Initializing cluster on Node 1..."
            INIT_RESULT=$(curl -sf -X POST http://localhost:3000/v1/cluster/initialize 2>&1) || true
            echo "  Initialize: $INIT_RESULT"

            # Wait for leader election to settle
            sleep 3

            echo "  Joining Node 2..."
            JOIN2_RESULT=$(curl -sf --max-time 15 -X POST http://localhost:3000/v1/cluster/join \
                 -H "Content-Type: application/json" \
                 -d '{"node_id": 2, "address": "127.0.0.1:5002"}' 2>&1) || true
            echo "  Join Node 2: $JOIN2_RESULT"

            sleep 1

            echo "  Joining Node 3..."
            JOIN3_RESULT=$(curl -sf --max-time 15 -X POST http://localhost:3000/v1/cluster/join \
                 -H "Content-Type: application/json" \
                 -d '{"node_id": 3, "address": "127.0.0.1:5003"}' 2>&1) || true
            echo "  Join Node 3: $JOIN3_RESULT"
        fi

        echo ""
        echo "Memorose Cluster is READY!"
        echo "------------------------------------------------"
        echo "Node 1:    http://localhost:3000"
        echo "Node 2:    http://localhost:3001"
        echo "Node 3:    http://localhost:3002"
        echo "Dashboard: http://localhost:3000/dashboard  (admin/admin)"
        echo "------------------------------------------------"
    fi
    echo "Logs available in $LOG_DIR/"
}

# Default values
COMMAND=""
MODE="cluster"
CLEAN="false"

# Parse position 1 as command
if [[ "$1" =~ ^(start|stop|restart)$ ]]; then
    COMMAND=$1
    shift
fi

# Parse remaining options
while [[ $# -gt 0 ]]; do
  case $1 in
    --mode)
      MODE="$2"
      shift 2
      ;;
    --clean)
      CLEAN="true"
      shift
      ;;
    --help)
      show_help
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      show_help
      exit 1
      ;;
  esac
done

if [ -z "$COMMAND" ]; then
    show_help
    exit 1
fi

# Execute command
case $COMMAND in
    start)
        start_servers
        ;;
    stop)
        stop_servers
        ;;
    restart)
        stop_servers
        sleep 2
        start_servers
        ;;
esac
