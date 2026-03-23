#!/bin/bash

#
# reset_to_standalone.sh - 彻底重置为单节点模式
#
# 这个脚本会：
# 1. 停止所有 Memorose 进程
# 2. 清理所有持久化数据（包括 Raft 状态）
# 3. 用单节点配置重启

set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly NC='\033[0m'

log_info() { echo -e "${GREEN}==>${NC} $*"; }
log_warn() { echo -e "${YELLOW}⚠${NC} $*"; }
log_error() { echo -e "${RED}✗${NC} $*" >&2; }

echo ""
echo "========================================"
echo "🔧 重置 Memorose 为单节点模式"
echo "========================================"
echo ""

log_warn "这将删除所有数据和 Raft 状态！"
read -p "确定继续？(y/n): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "已取消"
    exit 0
fi

cd "${ROOT_DIR}"

# Step 1: 停止所有进程
log_info "停止所有 Memorose 进程..."
pkill -9 -f memorose-server 2>/dev/null || true
sleep 2
log_info "✓ 进程已停止"

# Step 2: 清理所有数据目录
log_info "清理数据目录..."
rm -rf data/ logs/ .pids/
log_info "✓ 数据已清理"

# Step 3: 确保 config.toml 存在且正确
if [[ ! -f config.toml ]]; then
    log_error "config.toml 不存在！"
    log_info "请运行以下命令创建优化配置："
    echo "  cp config.toml.example config.toml"
    exit 1
fi

log_info "验证 config.toml..."
if grep -q "^\[\[raft.peers\]\]" config.toml 2>/dev/null; then
    log_error "config.toml 中还有 [[raft.peers]] 配置！"
    log_info "请删除所有 [[raft.peers]] 部分"
    exit 1
fi
log_info "✓ config.toml 配置正确（无 peers）"

# Step 4: 检查环境变量
if [[ -z "${GOOGLE_API_KEY:-}" ]]; then
    if [[ -f .env ]]; then
        log_info "从 .env 加载 API key..."
        export $(grep GOOGLE_API_KEY .env | xargs)
    else
        log_error "GOOGLE_API_KEY 未设置！"
        log_info "请设置: export GOOGLE_API_KEY='your_key'"
        exit 1
    fi
fi

log_info "✓ API key 已设置"

# Step 5: 编译（如果需要）
if [[ ! -f target/debug/memorose-server ]]; then
    log_info "编译 Memorose..."
    cargo build -p memorose-server
fi

# Step 6: 启动单节点服务器
log_info "启动单节点服务器..."
mkdir -p logs

export RUST_LOG=info,memorose_core::worker=debug
export NODE_ID=1
export PORT=3000
export RAFT_ADDR=127.0.0.1:5001

./target/debug/memorose-server > logs/standalone.log 2>&1 &
SERVER_PID=$!

log_info "服务器 PID: ${SERVER_PID}"
log_info "等待服务器启动..."

# 等待服务器就绪
for i in {1..30}; do
    if curl -sf http://localhost:3000/ >/dev/null 2>&1; then
        log_info "✓ 服务器已就绪"
        break
    fi
    sleep 1
done

# Step 7: 初始化单节点集群
log_info "初始化单节点 Raft 集群..."
sleep 2
INIT_RESULT=$(curl -sf -X POST http://localhost:3000/v1/cluster/initialize 2>&1) || true
log_info "初始化结果: ${INIT_RESULT}"

# Step 8: 验证没有 Raft 错误
log_info "检查 Raft 状态..."
sleep 3

if grep -q "error replication to target" logs/standalone.log 2>/dev/null; then
    log_error "❌ 仍然有 Raft 复制错误！"
    log_error "请检查日志: tail -f logs/standalone.log"
    exit 1
fi

log_info "✓ 无 Raft 错误"

# Step 9: 测试 consolidation
log_info "测试 consolidation worker..."
TEST_STREAM="00000000-0000-0000-0000-000000000001"

curl -sf -X POST "http://localhost:3000/v1/users/test_user/streams/${TEST_STREAM}/events" \
    -H "Content-Type: application/json" \
    -d '{"content": "This is a test event for consolidation", "content_type": "text"}' \
    >/dev/null

sleep 6  # 等待 consolidation

PENDING=$(curl -sf http://localhost:3000/v1/status/pending | jq -r .pending 2>/dev/null || echo "unknown")

if [[ "${PENDING}" == "0" ]]; then
    log_info "✓ Consolidation 工作正常！"
else
    log_warn "Pending events: ${PENDING}"
    log_warn "Consolidation 可能还在处理，或者有配置问题"
    log_info "查看日志: tail -f logs/standalone.log | grep -i consolidat"
fi

echo ""
echo "========================================"
echo "✅ Memorose 单节点模式已启动"
echo "========================================"
echo ""
echo "  URL:       http://localhost:3000"
echo "  Dashboard: http://localhost:3000/dashboard"
echo "  日志:      tail -f logs/standalone.log"
echo ""
echo "现在可以运行 benchmark："
echo "  cd ../MemEval && sh quick_bench.sh"
echo ""
