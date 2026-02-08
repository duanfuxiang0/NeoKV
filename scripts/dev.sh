#!/bin/bash
#
# NeoKV Development Script
# Usage:
#   ./scripts/dev.sh build [target]     - Build (all, neoMeta, neoStore, neokv, test_xxx)
#   ./scripts/dev.sh start [service]    - Start service (all, meta, store/kv)
#   ./scripts/dev.sh stop [service]     - Stop service (all, meta, store/kv)
#   ./scripts/dev.sh restart [service]  - Restart service (all, meta, store/kv)
#   ./scripts/dev.sh test [name]        - Run tests (redis, slot, codec, or test binary name)
#   ./scripts/dev.sh status             - Show service status
#   ./scripts/dev.sh clean              - Clean data directories
#   ./scripts/dev.sh logs [service]     - Tail logs
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
META_PORT=8010
STORE_PORT=8110
REDIS_PORT=16379
META_SERVER_IP="127.0.1.1"  # From /etc/hosts hostname mapping

# Data directories
META_DATA_DIR="$PROJECT_ROOT/meta_data"
STORE_DATA_DIR="$PROJECT_ROOT/store_data"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# ============================================================================
# BUILD
# ============================================================================
do_build() {
    local target="${1:-all}"
    cd "$PROJECT_ROOT"
    
    log_step "Building target: $target"
    
    case "$target" in
        all)
            cmake . && make -j$(nproc)
            ;;
        meta)
            cmake . && make -j$(nproc) neoMeta
            ;;
        store)
            cmake . && make -j$(nproc) neoStore
            ;;
        neokv)
            cmake . && make -j$(nproc) neokv
            ;;
        test_*)
            cmake -DWITH_TESTS=ON . && make -j$(nproc) "$target"
            ;;
        tests)
            cmake -DWITH_TESTS=ON . && make -j$(nproc)
            ;;
        *)
            # Try to build as-is (for custom targets)
            cmake . && make -j$(nproc) "$target"
            ;;
    esac
    
    log_info "Build completed: $target"
}

# ============================================================================
# SERVICE MANAGEMENT
# ============================================================================
get_pid() {
    local service="$1"
    case "$service" in
        meta)
            pgrep -f "bin/neoMeta" 2>/dev/null | head -1 || true
            ;;
        store|kv)
            pgrep -f "bin/neoStore" 2>/dev/null | head -1 || true
            ;;
    esac
}

is_running() {
    local service="$1"
    local pid=$(get_pid "$service")
    [ -n "$pid" ]
}

wait_for_port() {
    local port="$1"
    local timeout="${2:-30}"
    local count=0
    
    while [ $count -lt $timeout ]; do
        if lsof -i ":$port" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        count=$((count + 1))
    done
    return 1
}

wait_for_raft_leader() {
    local timeout="${1:-30}"
    local count=0
    
    log_info "Waiting for Raft leader election..."
    while [ $count -lt $timeout ]; do
        local state=$(curl -s "http://${META_SERVER_IP}:${META_PORT}/raft_stat/meta_raft" 2>/dev/null | grep "^state:" | awk '{print $2}')
        if [ "$state" = "LEADER" ]; then
            log_info "Meta server is LEADER"
            return 0
        fi
        sleep 1
        count=$((count + 1))
    done
    log_warn "Timeout waiting for Raft leader (current state: $state)"
    return 1
}

init_data_dirs() {
    log_step "Initializing data directories..."
    
    mkdir -p "$META_DATA_DIR/rocks_db"
    mkdir -p "$META_DATA_DIR/raft_data"
    mkdir -p "$META_DATA_DIR/data/raft_data/snapshot"
    mkdir -p "$META_DATA_DIR/monitor"
    
    mkdir -p "$STORE_DATA_DIR/rocks_db"
    mkdir -p "$STORE_DATA_DIR/raft_data/raft_data/snapshot"
    mkdir -p "$STORE_DATA_DIR/monitor"
}

do_start() {
    local service="${1:-all}"
    cd "$PROJECT_ROOT"
    
    init_data_dirs
    
    case "$service" in
        all)
            do_start meta
            do_start store
            ;;
        meta)
            if is_running meta; then
                log_warn "neoMeta is already running (PID: $(get_pid meta))"
                return 0
            fi
            
            log_step "Starting neoMeta..."
            ./output/bin/neoMeta \
                --flagfile=conf/neoMeta/gflags.conf \
                --db_path="$META_DATA_DIR/rocks_db" \
                --snapshot_uri="local://$META_DATA_DIR/raft_data/snapshot" \
                >/dev/null 2>&1 &
            
            if wait_for_port $META_PORT 10; then
                log_info "neoMeta started (PID: $(get_pid meta))"
                wait_for_raft_leader 20 || true
            else
                log_error "Failed to start neoMeta"
                return 1
            fi
            ;;
        store|kv|redis)
            if is_running store; then
                log_warn "neoStore is already running (PID: $(get_pid store))"
                return 0
            fi
            
            # Ensure meta is running first
            if ! is_running meta; then
                log_warn "neoMeta is not running, starting it first..."
                do_start meta
            fi
            
            log_step "Starting neoStore (with Redis/KV on port $REDIS_PORT)..."
            ./output/bin/neoStore \
                --flagfile=conf/neoStore/gflags.conf \
                --db_path="$STORE_DATA_DIR/rocks_db" \
                --snapshot_uri="local://$STORE_DATA_DIR/raft_data/snapshot" \
                --redis_port=$REDIS_PORT \
                >/dev/null 2>&1 &
            
            if wait_for_port $REDIS_PORT 15; then
                log_info "neoStore started (PID: $(get_pid store)), Redis/KV on port $REDIS_PORT"
            else
                log_error "Failed to start neoStore"
                return 1
            fi
            ;;
        *)
            log_error "Unknown service: $service (use: all, meta, store/kv)"
            return 1
            ;;
    esac
}

do_stop() {
    local service="${1:-all}"
    
    case "$service" in
        all)
            do_stop store
            do_stop meta
            ;;
        meta)
            local pid=$(get_pid meta)
            if [ -n "$pid" ]; then
                log_step "Stopping neoMeta (PID: $pid)..."
                kill $pid 2>/dev/null || true
                sleep 2
                # Force kill if still running
                if is_running meta; then
                    kill -9 $(get_pid meta) 2>/dev/null || true
                fi
                log_info "neoMeta stopped"
            else
                log_info "neoMeta is not running"
            fi
            ;;
        store|kv|redis)
            local pid=$(get_pid store)
            if [ -n "$pid" ]; then
                log_step "Stopping neoStore (PID: $pid)..."
                kill $pid 2>/dev/null || true
                sleep 2
                # Force kill if still running
                if is_running store; then
                    kill -9 $(get_pid store) 2>/dev/null || true
                fi
                log_info "neoStore stopped"
            else
                log_info "neoStore is not running"
            fi
            ;;
        *)
            log_error "Unknown service: $service (use: all, meta, store/kv)"
            return 1
            ;;
    esac
}

do_restart() {
    local service="${1:-all}"
    log_step "Restarting $service..."
    do_stop "$service"
    sleep 2
    do_start "$service"
}

do_status() {
    echo ""
    echo "=== NeoKV Service Status ==="
    echo ""
    
    # Meta status
    local meta_pid=$(get_pid meta)
    if [ -n "$meta_pid" ]; then
        echo -e "neoMeta:  ${GREEN}RUNNING${NC} (PID: $meta_pid)"
        local state=$(curl -s "http://${META_SERVER_IP}:${META_PORT}/raft_stat/meta_raft" 2>/dev/null | grep "^state:" | awk '{print $2}')
        echo "             Raft state: $state"
        echo "             Port: $META_PORT"
    else
        echo -e "neoMeta:  ${RED}STOPPED${NC}"
    fi
    
    echo ""
    
    # Store status
    local store_pid=$(get_pid store)
    if [ -n "$store_pid" ]; then
        echo -e "neoStore: ${GREEN}RUNNING${NC} (PID: $store_pid)"
        echo "             Store port: $STORE_PORT"
        echo "             Redis port: $REDIS_PORT"
        
        # Test Redis
        if redis-cli -p $REDIS_PORT PING 2>/dev/null | grep -q PONG; then
            echo -e "             Redis:      ${GREEN}OK${NC}"
        else
            echo -e "             Redis:      ${RED}NOT RESPONDING${NC}"
        fi
    else
        echo -e "neoStore: ${RED}STOPPED${NC}"
    fi
    
    echo ""
}

# ============================================================================
# TESTING
# ============================================================================
do_test() {
    local test_name="${1:-redis}"
    cd "$PROJECT_ROOT"
    
    case "$test_name" in
        redis)
            # Run comprehensive Redis integration tests
            log_step "Running comprehensive Redis tests..."
            "$SCRIPT_DIR/redis_test.sh" all
            ;;
        redis_basic)
            "$SCRIPT_DIR/redis_test.sh" basic
            ;;
        redis_persistence|persistence)
            "$SCRIPT_DIR/redis_test.sh" persistence
            ;;
        redis_concurrent|concurrent)
            "$SCRIPT_DIR/redis_test.sh" concurrent
            ;;
        redis_stress|stress)
            "$SCRIPT_DIR/redis_test.sh" stress
            ;;
        redis_ttl|ttl)
            "$SCRIPT_DIR/redis_test.sh" ttl
            ;;
        redis_large|large)
            "$SCRIPT_DIR/redis_test.sh" large
            ;;
        redis_quick|quick)
            # Quick smoke test
            log_step "Quick Redis smoke test..."
            if ! redis-cli -p $REDIS_PORT PING 2>/dev/null | grep -q PONG; then
                log_error "Redis is not responding on port $REDIS_PORT"
                log_info "Try: $0 start store (or $0 start kv)"
                return 1
            fi
            echo "PING: $(redis-cli -p $REDIS_PORT PING)"
            redis-cli -p $REDIS_PORT SET quick_test "$(date)" >/dev/null
            echo "SET/GET: $(redis-cli -p $REDIS_PORT GET quick_test)"
            redis-cli -p $REDIS_PORT DEL quick_test >/dev/null
            log_info "Quick test passed!"
            ;;
        slot|redis_slot)
            log_step "Running test_redis_slot..."
            ./output/bin/test_redis_slot
            ;;
        codec|redis_codec)
            log_step "Running test_redis_codec..."
            ./output/bin/test_redis_codec
            ;;
        raft|redis_raft)
            log_step "Running test_redis_raft..."
            ./output/bin/test_redis_raft
            ;;
        unit)
            # Run all Redis unit tests
            log_step "Running all Redis unit tests..."
            ./output/bin/test_redis_slot && \
            ./output/bin/test_redis_codec && \
            ./output/bin/test_redis_raft
            ;;
        all_redis)
            # Run both unit and integration tests
            log_step "Running ALL Redis tests (unit + integration)..."
            ./output/bin/test_redis_slot && \
            ./output/bin/test_redis_codec && \
            ./output/bin/test_redis_raft && \
            "$SCRIPT_DIR/redis_test.sh" all
            ;;
        test_*)
            log_step "Running $test_name..."
            ./output/bin/$test_name
            ;;
        *)
            # Try to run as test binary name
            if [ -x "./output/bin/test_$test_name" ]; then
                log_step "Running test_$test_name..."
                ./output/bin/test_$test_name
            elif [ -x "./output/bin/$test_name" ]; then
                log_step "Running $test_name..."
                ./output/bin/$test_name
            else
                log_error "Unknown test: $test_name"
                echo ""
                echo "Available Redis tests:"
                echo "  redis           - Full Redis integration test suite"
                echo "  redis_basic     - Basic commands only"
                echo "  redis_quick     - Quick smoke test"
                echo "  redis_ttl       - TTL/expiration tests"
                echo "  redis_large     - Large value tests"
                echo "  redis_concurrent- Concurrent write tests"
                echo "  redis_stress    - Stress/performance tests"
                echo "  redis_persistence- Persistence across restarts"
                echo ""
                echo "Redis unit tests:"
                echo "  slot            - Slot calculation tests"
                echo "  codec           - Codec encode/decode tests"
                echo "  raft            - Raft integration unit tests"
                echo "  unit            - All Redis unit tests"
                echo "  all_redis       - All Redis tests (unit + integration)"
                echo ""
                echo "Other test binaries:"
                ls -1 ./output/bin/test_* 2>/dev/null | xargs -n1 basename || echo "  (none built)"
                return 1
            fi
            ;;
    esac
}

# ============================================================================
# CLEAN
# ============================================================================
do_clean() {
    local target="${1:-data}"
    
    case "$target" in
        data)
            log_step "Cleaning data directories..."
            do_stop all
            rm -rf "$META_DATA_DIR"/*
            rm -rf "$STORE_DATA_DIR"/*
            init_data_dirs
            log_info "Data directories cleaned"
            ;;
        build)
            log_step "Cleaning build artifacts..."
            cd "$PROJECT_ROOT"
            make clean 2>/dev/null || true
            rm -rf CMakeCache.txt CMakeFiles
            log_info "Build cleaned"
            ;;
        all)
            do_clean data
            do_clean build
            ;;
        *)
            log_error "Unknown clean target: $target (use: data, build, all)"
            return 1
            ;;
    esac
}

# ============================================================================
# LOGS
# ============================================================================
do_logs() {
    local service="${1:-store}"
    cd "$PROJECT_ROOT"
    
    case "$service" in
        meta)
            tail -f "$META_DATA_DIR"/*.log 2>/dev/null || log_error "No meta logs found"
            ;;
        store|kv)
            tail -f "$STORE_DATA_DIR"/*.log 2>/dev/null || log_error "No store logs found"
            ;;
        *)
            log_error "Unknown service: $service (use: meta, store/kv)"
            ;;
    esac
}

# ============================================================================
# HELP
# ============================================================================
show_help() {
    echo "NeoKV Development Script"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  build [target]     Build project"
    echo "                     Targets: all, meta, store, db, tests, test_xxx"
    echo ""
    echo "  start [service]    Start services"
    echo "                     Services: all, meta, store/kv (store and kv are the same)"
    echo ""
    echo "  stop [service]     Stop services"
    echo "                     Services: all, meta, store/kv"
    echo ""
    echo "  restart [service]  Restart services (stop + start)"
    echo "                     Services: all, meta, store/kv"
    echo ""
    echo "  status             Show service status"
    echo ""
    echo "  test [name]        Run tests"
    echo "                     Redis integration: redis, redis_basic, redis_quick,"
    echo "                       redis_ttl, redis_large, redis_concurrent, redis_stress"
    echo "                     Redis unit: slot, codec, raft, unit"
    echo "                     All Redis: all_redis"
    echo ""
    echo "  clean [target]     Clean up"
    echo "                     Targets: data, build, all"
    echo ""
    echo "  logs [service]     Tail service logs"
    echo "                     Services: meta, store/kv"
    echo ""
    echo "Examples:"
    echo "  $0 build store          # Build only neoStore"
    echo "  $0 build test_redis_slot # Build specific test"
    echo "  $0 start meta           # Start meta service"
    echo "  $0 start store          # Start store/kv service"
    echo "  $0 start kv             # Start store/kv service (alias)"
    echo "  $0 restart store        # Restart neoStore"
    echo "  $0 test redis           # Test Redis commands"
    echo "  $0 status               # Show all service status"
    echo ""
}

# ============================================================================
# MAIN
# ============================================================================
main() {
    local command="${1:-help}"
    shift || true
    
    case "$command" in
        build)
            do_build "$@"
            ;;
        start)
            do_start "$@"
            ;;
        stop)
            do_stop "$@"
            ;;
        restart)
            do_restart "$@"
            ;;
        status)
            do_status
            ;;
        test)
            do_test "$@"
            ;;
        clean)
            do_clean "$@"
            ;;
        logs)
            do_logs "$@"
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "Unknown command: $command"
            show_help
            return 1
            ;;
    esac
}

main "$@"

