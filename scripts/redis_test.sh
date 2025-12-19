#!/bin/bash
#
# Comprehensive Redis Raft Integration Tests
# Tests: Basic commands, persistence, restart recovery, concurrent writes, stress tests
#
# Usage:
#   ./scripts/redis_test.sh [test_name]
#   
# Tests available:
#   basic        - Basic Redis commands (SET/GET/DEL/MSET/MGET/EXPIRE/TTL)
#   persistence  - Data persistence across restarts
#   concurrent   - Concurrent write tests
#   stress       - High throughput stress test
#   ttl          - TTL expiration tests
#   large        - Large value tests
#   all          - Run all tests
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
REDIS_PORT=16379
REDIS_CLI="redis-cli -p $REDIS_PORT"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0
TOTAL_TESTS=0

# ============================================================================
# Helper Functions
# ============================================================================

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_test() {
    echo -e "${CYAN}[TEST]${NC} $1"
}

log_section() {
    echo ""
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""
}

assert_eq() {
    local expected="$1"
    local actual="$2"
    local msg="$3"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if [ "$expected" = "$actual" ]; then
        echo -e "${GREEN}  ✓ PASS:${NC} $msg"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        return 0
    else
        echo -e "${RED}  ✗ FAIL:${NC} $msg"
        echo -e "       Expected: '$expected'"
        echo -e "       Actual:   '$actual'"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

assert_ne() {
    local not_expected="$1"
    local actual="$2"
    local msg="$3"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if [ "$not_expected" != "$actual" ]; then
        echo -e "${GREEN}  ✓ PASS:${NC} $msg"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        return 0
    else
        echo -e "${RED}  ✗ FAIL:${NC} $msg (got unexpected value: '$actual')"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

assert_gt() {
    local threshold="$1"
    local actual="$2"
    local msg="$3"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if [ "$actual" -gt "$threshold" ]; then
        echo -e "${GREEN}  ✓ PASS:${NC} $msg (value: $actual > $threshold)"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        return 0
    else
        echo -e "${RED}  ✗ FAIL:${NC} $msg (expected > $threshold, got $actual)"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

assert_contains() {
    local expected="$1"
    local actual="$2"
    local msg="$3"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if echo "$actual" | grep -q "$expected"; then
        echo -e "${GREEN}  ✓ PASS:${NC} $msg"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        return 0
    else
        echo -e "${RED}  ✗ FAIL:${NC} $msg (expected to contain '$expected')"
        echo -e "       Actual: '$actual'"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

check_redis_running() {
    if ! $REDIS_CLI PING 2>/dev/null | grep -q PONG; then
        log_error "Redis is not responding on port $REDIS_PORT"
        log_info "Start it with: ./scripts/dev.sh start store"
        exit 1
    fi
}

cleanup_test_keys() {
    log_info "Cleaning up test keys..."
    $REDIS_CLI DEL test_key >/dev/null 2>&1 || true
    $REDIS_CLI DEL '{test}key1' '{test}key2' '{test}key3' >/dev/null 2>&1 || true
    $REDIS_CLI DEL ttl_test >/dev/null 2>&1 || true
    $REDIS_CLI DEL persist_key >/dev/null 2>&1 || true
    $REDIS_CLI DEL large_key >/dev/null 2>&1 || true
    # Clean concurrent test keys
    for i in $(seq 1 100); do
        $REDIS_CLI DEL "concurrent_$i" >/dev/null 2>&1 || true
    done
    # Clean stress test keys
    for i in $(seq 1 1000); do
        $REDIS_CLI DEL "stress_$i" >/dev/null 2>&1 || true
    done
}

# ============================================================================
# Basic Command Tests
# ============================================================================

test_basic() {
    log_section "Basic Command Tests"
    check_redis_running
    cleanup_test_keys
    
    log_test "PING"
    local result=$($REDIS_CLI PING)
    assert_eq "PONG" "$result" "PING returns PONG"
    
    log_test "SET/GET"
    $REDIS_CLI SET test_key "hello world" >/dev/null
    result=$($REDIS_CLI GET test_key)
    assert_eq "hello world" "$result" "SET and GET basic string"
    
    log_test "SET with EX option"
    $REDIS_CLI SET test_key "expiring" EX 3600 >/dev/null
    result=$($REDIS_CLI GET test_key)
    assert_eq "expiring" "$result" "SET with EX preserves value"
    local ttl=$($REDIS_CLI TTL test_key)
    assert_gt 3590 "$ttl" "TTL is approximately 3600 seconds"
    
    log_test "MSET/MGET (with hash tag)"
    $REDIS_CLI MSET '{test}key1' val1 '{test}key2' val2 '{test}key3' val3 >/dev/null
    result=$($REDIS_CLI MGET '{test}key1' '{test}key2' '{test}key3')
    assert_contains "val1" "$result" "MGET returns val1"
    assert_contains "val2" "$result" "MGET returns val2"
    assert_contains "val3" "$result" "MGET returns val3"
    
    log_test "DEL single key"
    $REDIS_CLI SET test_key "to_delete" >/dev/null
    result=$($REDIS_CLI DEL test_key)
    assert_eq "1" "$result" "DEL returns 1 for existing key"
    result=$($REDIS_CLI GET test_key)
    assert_eq "" "$result" "GET returns nil after DEL"
    
    log_test "DEL multiple keys (with hash tag)"
    result=$($REDIS_CLI DEL '{test}key1' '{test}key2' '{test}key3')
    assert_eq "3" "$result" "DEL returns 3 for three keys"
    
    log_test "DEL non-existent key"
    result=$($REDIS_CLI DEL non_existent_key_12345)
    assert_eq "0" "$result" "DEL returns 0 for non-existent key"
    
    log_test "EXPIRE/TTL"
    $REDIS_CLI SET ttl_test "ttl_value" >/dev/null
    ttl=$($REDIS_CLI TTL ttl_test)
    assert_eq "-1" "$ttl" "TTL returns -1 for key without expire"
    $REDIS_CLI EXPIRE ttl_test 300 >/dev/null
    ttl=$($REDIS_CLI TTL ttl_test)
    assert_gt 290 "$ttl" "TTL after EXPIRE is approximately 300"
    
    log_test "TTL on non-existent key"
    ttl=$($REDIS_CLI TTL non_existent_key_67890)
    assert_eq "-2" "$ttl" "TTL returns -2 for non-existent key"
    
    log_test "ECHO"
    result=$($REDIS_CLI ECHO "test message")
    assert_eq "test message" "$result" "ECHO returns input"
    
    log_test "CLUSTER INFO"
    result=$($REDIS_CLI CLUSTER INFO)
    assert_contains "cluster_state" "$result" "CLUSTER INFO returns cluster info"
    
    log_test "CLUSTER KEYSLOT"
    result=$($REDIS_CLI CLUSTER KEYSLOT "test_key")
    # Slot should be a number between 0 and 16383
    assert_gt -1 "$result" "CLUSTER KEYSLOT returns valid slot number"
    
    cleanup_test_keys
}

# ============================================================================
# Persistence/Restart Tests
# ============================================================================

test_persistence() {
    log_section "Persistence Tests"
    check_redis_running
    cleanup_test_keys
    
    log_test "Write data before restart"
    local timestamp=$(date +%s%N)
    $REDIS_CLI SET persist_key "value_$timestamp" >/dev/null
    local result=$($REDIS_CLI GET persist_key)
    assert_eq "value_$timestamp" "$result" "Data written successfully"
    
    # Write multiple keys
    for i in $(seq 1 10); do
        $REDIS_CLI SET "persist_test_$i" "data_$i" >/dev/null
    done
    
    log_info "Restarting baikalStore to test persistence..."
    
    # Restart store
    "$SCRIPT_DIR/dev.sh" restart store >/dev/null 2>&1
    sleep 3
    
    log_test "Read data after restart"
    check_redis_running
    
    result=$($REDIS_CLI GET persist_key)
    assert_eq "value_$timestamp" "$result" "persist_key survived restart"
    
    local all_survived=true
    for i in $(seq 1 10); do
        result=$($REDIS_CLI GET "persist_test_$i")
        if [ "$result" != "data_$i" ]; then
            all_survived=false
            log_error "persist_test_$i did not survive restart"
        fi
    done
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if $all_survived; then
        echo -e "${GREEN}  ✓ PASS:${NC} All 10 persist_test keys survived restart"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}  ✗ FAIL:${NC} Some persist_test keys lost after restart"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
    
    # Cleanup
    for i in $(seq 1 10); do
        $REDIS_CLI DEL "persist_test_$i" >/dev/null 2>&1 || true
    done
    $REDIS_CLI DEL persist_key >/dev/null 2>&1 || true
}

# ============================================================================
# Concurrent Write Tests
# ============================================================================

test_concurrent() {
    log_section "Concurrent Write Tests"
    check_redis_running
    cleanup_test_keys
    
    local num_clients=10
    local ops_per_client=50
    
    log_test "Launching $num_clients concurrent clients with $ops_per_client operations each"
    
    # Function to run in background
    run_client() {
        local client_id=$1
        local ops=$2
        for i in $(seq 1 $ops); do
            $REDIS_CLI SET "concurrent_${client_id}_$i" "value_${client_id}_$i" >/dev/null 2>&1
        done
    }
    
    # Start concurrent clients
    local start_time=$(date +%s%N)
    for c in $(seq 1 $num_clients); do
        run_client $c $ops_per_client &
    done
    
    # Wait for all clients to complete
    wait
    local end_time=$(date +%s%N)
    
    local duration_ms=$(( (end_time - start_time) / 1000000 ))
    local total_ops=$((num_clients * ops_per_client))
    local ops_per_sec=$((total_ops * 1000 / duration_ms))
    
    log_info "Completed $total_ops operations in ${duration_ms}ms (~$ops_per_sec ops/sec)"
    
    # Verify all data
    log_test "Verifying all concurrent writes"
    local success_count=0
    for c in $(seq 1 $num_clients); do
        for i in $(seq 1 $ops_per_client); do
            local result=$($REDIS_CLI GET "concurrent_${c}_$i")
            if [ "$result" = "value_${c}_$i" ]; then
                success_count=$((success_count + 1))
            fi
        done
    done
    
    assert_eq "$total_ops" "$success_count" "All $total_ops concurrent writes succeeded"
    
    # Cleanup
    log_info "Cleaning up concurrent test keys..."
    for c in $(seq 1 $num_clients); do
        for i in $(seq 1 $ops_per_client); do
            $REDIS_CLI DEL "concurrent_${c}_$i" >/dev/null 2>&1 || true
        done
    done
}

# ============================================================================
# Stress Tests
# ============================================================================

test_stress() {
    log_section "Stress Tests"
    check_redis_running
    cleanup_test_keys
    
    local num_operations=1000
    
    log_test "Sequential write stress test ($num_operations operations)"
    
    local start_time=$(date +%s%N)
    for i in $(seq 1 $num_operations); do
        $REDIS_CLI SET "stress_$i" "value_$i" >/dev/null 2>&1
    done
    local end_time=$(date +%s%N)
    
    local duration_ms=$(( (end_time - start_time) / 1000000 ))
    local ops_per_sec=$((num_operations * 1000 / duration_ms))
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if [ "$duration_ms" -lt 60000 ]; then
        echo -e "${GREEN}  ✓ PASS:${NC} $num_operations writes completed in ${duration_ms}ms (~$ops_per_sec ops/sec)"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}  ✗ FAIL:${NC} Stress test took too long (${duration_ms}ms)"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
    
    log_test "Sequential read stress test ($num_operations operations)"
    
    start_time=$(date +%s%N)
    local read_success=0
    for i in $(seq 1 $num_operations); do
        local result=$($REDIS_CLI GET "stress_$i")
        if [ "$result" = "value_$i" ]; then
            read_success=$((read_success + 1))
        fi
    done
    end_time=$(date +%s%N)
    
    duration_ms=$(( (end_time - start_time) / 1000000 ))
    ops_per_sec=$((num_operations * 1000 / duration_ms))
    
    assert_eq "$num_operations" "$read_success" "All $num_operations reads returned correct values"
    log_info "Read performance: ${duration_ms}ms (~$ops_per_sec ops/sec)"
    
    log_test "Mixed read/write stress test"
    
    start_time=$(date +%s%N)
    for i in $(seq 1 500); do
        $REDIS_CLI SET "stress_mixed_$i" "mixed_$i" >/dev/null 2>&1
        $REDIS_CLI GET "stress_mixed_$i" >/dev/null 2>&1
    done
    end_time=$(date +%s%N)
    
    duration_ms=$(( (end_time - start_time) / 1000000 ))
    ops_per_sec=$((1000 * 1000 / duration_ms))
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -e "${GREEN}  ✓ PASS:${NC} Mixed operations completed in ${duration_ms}ms (~$ops_per_sec ops/sec)"
    TESTS_PASSED=$((TESTS_PASSED + 1))
    
    # Cleanup
    log_info "Cleaning up stress test keys..."
    for i in $(seq 1 $num_operations); do
        $REDIS_CLI DEL "stress_$i" >/dev/null 2>&1 || true
    done
    for i in $(seq 1 500); do
        $REDIS_CLI DEL "stress_mixed_$i" >/dev/null 2>&1 || true
    done
}

# ============================================================================
# TTL Expiration Tests
# ============================================================================

test_ttl() {
    log_section "TTL Expiration Tests"
    check_redis_running
    cleanup_test_keys
    
    log_test "Short TTL expiration (2 seconds)"
    $REDIS_CLI SET ttl_test "will_expire" EX 2 >/dev/null
    
    local result=$($REDIS_CLI GET ttl_test)
    assert_eq "will_expire" "$result" "Key exists immediately after SET with TTL"
    
    local ttl=$($REDIS_CLI TTL ttl_test)
    assert_gt 0 "$ttl" "TTL is positive before expiration"
    
    log_info "Waiting 3 seconds for TTL expiration..."
    sleep 3
    
    result=$($REDIS_CLI GET ttl_test)
    assert_eq "" "$result" "Key is nil after TTL expiration"
    
    ttl=$($REDIS_CLI TTL ttl_test)
    assert_eq "-2" "$ttl" "TTL returns -2 for expired/non-existent key"
    
    log_test "EXPIRE on existing key"
    $REDIS_CLI SET ttl_test "new_value" >/dev/null
    ttl=$($REDIS_CLI TTL ttl_test)
    assert_eq "-1" "$ttl" "TTL is -1 for key without expiration"
    
    $REDIS_CLI EXPIRE ttl_test 10 >/dev/null
    ttl=$($REDIS_CLI TTL ttl_test)
    assert_gt 5 "$ttl" "TTL is set after EXPIRE command"
    
    log_test "Overwrite key removes TTL"
    $REDIS_CLI SET ttl_test "overwritten" >/dev/null
    ttl=$($REDIS_CLI TTL ttl_test)
    assert_eq "-1" "$ttl" "TTL is -1 after overwriting key (TTL removed)"
    
    cleanup_test_keys
}

# ============================================================================
# Large Value Tests
# ============================================================================

test_large() {
    log_section "Large Value Tests"
    check_redis_running
    cleanup_test_keys
    
    log_test "1KB value"
    local value_1kb=$(python3 -c "print('X' * 1024)")
    $REDIS_CLI SET large_key "$value_1kb" >/dev/null
    local result=$($REDIS_CLI GET large_key)
    local len=${#result}
    assert_eq "1024" "$len" "1KB value stored and retrieved correctly"
    
    log_test "10KB value"
    local value_10kb=$(python3 -c "print('Y' * 10240)")
    $REDIS_CLI SET large_key "$value_10kb" >/dev/null
    result=$($REDIS_CLI GET large_key)
    len=${#result}
    assert_eq "10240" "$len" "10KB value stored and retrieved correctly"
    
    log_test "100KB value"
    local value_100kb=$(python3 -c "print('Z' * 102400)")
    $REDIS_CLI SET large_key "$value_100kb" >/dev/null
    result=$($REDIS_CLI GET large_key)
    len=${#result}
    assert_eq "102400" "$len" "100KB value stored and retrieved correctly"
    
    log_test "Binary data (with special chars)"
    # Test value with various special characters
    $REDIS_CLI SET large_key "binary\x00data\twith\nnewlines\rand\x1fcontrol" >/dev/null
    result=$($REDIS_CLI GET large_key)
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if [ -n "$result" ]; then
        echo -e "${GREEN}  ✓ PASS:${NC} Binary data stored and retrieved"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}  ✗ FAIL:${NC} Binary data test failed"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
    
    cleanup_test_keys
}

# ============================================================================
# Error Handling Tests
# ============================================================================

test_errors() {
    log_section "Error Handling Tests"
    check_redis_running
    
    log_test "GET non-existent key returns nil"
    local result=$($REDIS_CLI GET "absolutely_non_existent_key_$(date +%s)")
    assert_eq "" "$result" "GET non-existent key returns empty"
    
    log_test "CROSSSLOT error for keys in different slots"
    # These keys have different slots (no hash tag)
    result=$($REDIS_CLI MSET key_abc value1 key_xyz value2 2>&1)
    assert_contains "CROSSSLOT" "$result" "MSET with different slots returns CROSSSLOT error"
    
    log_test "Unknown command returns error"
    result=$($REDIS_CLI UNKNOWN_COMMAND arg1 arg2 2>&1)
    assert_contains "unknown command" "$result" "Unknown command returns error"
}

# ============================================================================
# Slot Consistency Tests
# ============================================================================

test_slots() {
    log_section "Slot Consistency Tests"
    check_redis_running
    cleanup_test_keys
    
    log_test "Hash tag same slot verification"
    local slot1=$($REDIS_CLI CLUSTER KEYSLOT "{user1000}.profile")
    local slot2=$($REDIS_CLI CLUSTER KEYSLOT "{user1000}.settings")
    local slot3=$($REDIS_CLI CLUSTER KEYSLOT "{user1000}:data")
    local slot_base=$($REDIS_CLI CLUSTER KEYSLOT "user1000")
    
    assert_eq "$slot_base" "$slot1" "{user1000}.profile has same slot as user1000"
    assert_eq "$slot_base" "$slot2" "{user1000}.settings has same slot as user1000"
    assert_eq "$slot_base" "$slot3" "{user1000}:data has same slot as user1000"
    
    log_test "MSET/MGET with hash tags works"
    $REDIS_CLI MSET '{slot_test}a' '1' '{slot_test}b' '2' '{slot_test}c' '3' >/dev/null
    local result=$($REDIS_CLI MGET '{slot_test}a' '{slot_test}b' '{slot_test}c')
    assert_contains "1" "$result" "MGET with hash tags returns value 1"
    assert_contains "2" "$result" "MGET with hash tags returns value 2"
    assert_contains "3" "$result" "MGET with hash tags returns value 3"
    
    # Cleanup
    $REDIS_CLI DEL '{slot_test}a' '{slot_test}b' '{slot_test}c' >/dev/null 2>&1 || true
}

# ============================================================================
# Summary
# ============================================================================

print_summary() {
    echo ""
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}  Test Summary${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""
    echo -e "  Total Tests:  $TOTAL_TESTS"
    echo -e "  ${GREEN}Passed:${NC}       $TESTS_PASSED"
    echo -e "  ${RED}Failed:${NC}       $TESTS_FAILED"
    echo ""
    
    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}  ✓ All tests passed!${NC}"
        return 0
    else
        echo -e "${RED}  ✗ Some tests failed!${NC}"
        return 1
    fi
}

# ============================================================================
# Main
# ============================================================================

main() {
    local test_name="${1:-all}"
    
    echo -e "${CYAN}"
    echo "  ____          _ _      ____        __ _     _____         _   "
    echo " |  _ \ ___  __| (_)___ |  _ \ __ _ / _| |_  |_   _|__  ___| |_ "
    echo " | |_) / _ \/ _\` | / __|| |_) / _\` | |_| __|   | |/ _ \/ __| __|"
    echo " |  _ <  __/ (_| | \__ \|  _ < (_| |  _| |_    | |  __/\__ \ |_ "
    echo " |_| \_\___|\__,_|_|___/|_| \_\__,_|_|  \__|   |_|\___||___/\__|"
    echo ""
    echo -e "${NC}"
    
    case "$test_name" in
        basic)
            test_basic
            ;;
        persistence)
            test_persistence
            ;;
        concurrent)
            test_concurrent
            ;;
        stress)
            test_stress
            ;;
        ttl)
            test_ttl
            ;;
        large)
            test_large
            ;;
        errors)
            test_errors
            ;;
        slots)
            test_slots
            ;;
        all)
            test_basic
            test_slots
            test_ttl
            test_large
            test_errors
            test_concurrent
            test_stress
            test_persistence
            ;;
        *)
            echo "Unknown test: $test_name"
            echo ""
            echo "Available tests:"
            echo "  basic        - Basic Redis commands"
            echo "  persistence  - Data persistence across restarts"
            echo "  concurrent   - Concurrent write tests"
            echo "  stress       - High throughput stress test"
            echo "  ttl          - TTL expiration tests"
            echo "  large        - Large value tests"
            echo "  errors       - Error handling tests"
            echo "  slots        - Slot consistency tests"
            echo "  all          - Run all tests"
            exit 1
            ;;
    esac
    
    print_summary
}

main "$@"

