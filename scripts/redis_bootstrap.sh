#!/bin/bash
# Bootstrap script for BaikalDB Redis module
# Creates the necessary system tables and initial configuration

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default configuration
BAIKALDB_HOST="${BAIKALDB_HOST:-127.0.0.1}"
BAIKALDB_PORT="${BAIKALDB_PORT:-28282}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Bootstrap BaikalDB Redis module by creating system tables."
    echo ""
    echo "Options:"
    echo "  -h, --host HOST      BaikalDB host (default: 127.0.0.1)"
    echo "  -P, --port PORT      BaikalDB port (default: 28282)"
    echo "  -u, --user USER      MySQL user (default: root)"
    echo "  -p, --password PASS  MySQL password (default: empty)"
    echo "  --help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                           # Use defaults"
    echo "  $0 -h 10.0.0.1 -P 28282     # Custom host and port"
    echo "  $0 -u admin -p secret       # With authentication"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--host)
            BAIKALDB_HOST="$2"
            shift 2
            ;;
        -P|--port)
            BAIKALDB_PORT="$2"
            shift 2
            ;;
        -u|--user)
            MYSQL_USER="$2"
            shift 2
            ;;
        -p|--password)
            MYSQL_PASSWORD="$2"
            shift 2
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Check if mysql client is available
if ! command -v mysql &> /dev/null; then
    log_error "mysql client not found. Please install mysql-client."
    exit 1
fi

# Build mysql command
MYSQL_CMD="mysql -h $BAIKALDB_HOST -P $BAIKALDB_PORT -u $MYSQL_USER"
if [ -n "$MYSQL_PASSWORD" ]; then
    MYSQL_CMD="$MYSQL_CMD -p$MYSQL_PASSWORD"
fi

log_info "Connecting to BaikalDB at $BAIKALDB_HOST:$BAIKALDB_PORT..."

# Test connection
if ! $MYSQL_CMD -e "SELECT 1" &> /dev/null; then
    log_error "Failed to connect to BaikalDB. Please check host, port, and credentials."
    exit 1
fi

log_info "Connection successful."

# Check if __redis__ database already exists
DB_EXISTS=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '__redis__'")
if [ "$DB_EXISTS" -gt 0 ]; then
    log_warn "__redis__ database already exists."
    
    # Check if kv table exists
    TABLE_EXISTS=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '__redis__' AND TABLE_NAME = 'kv'")
    if [ "$TABLE_EXISTS" -gt 0 ]; then
        log_info "__redis__.kv table already exists. Skipping initialization."
        exit 0
    fi
fi

log_info "Creating Redis system tables..."

# Execute the SQL script
if $MYSQL_CMD < "$SCRIPT_DIR/redis_init.sql"; then
    log_info "Redis system tables created successfully!"
else
    log_error "Failed to create Redis system tables."
    exit 1
fi

# Verify the table was created
TABLE_EXISTS=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '__redis__' AND TABLE_NAME = 'kv'")
if [ "$TABLE_EXISTS" -gt 0 ]; then
    log_info "Verification passed: __redis__.kv table exists."
    
    # Show table info
    log_info "Table structure:"
    $MYSQL_CMD -e "DESCRIBE \`__redis__\`.\`kv\`"
else
    log_error "Verification failed: __redis__.kv table not found."
    exit 1
fi

log_info "Redis bootstrap completed successfully!"
log_info ""
log_info "You can now use Redis commands on your BaikalDB Store nodes."
log_info "Example: redis-cli -p 16379 SET mykey myvalue"
