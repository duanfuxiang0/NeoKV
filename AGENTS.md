# NEOKV Development Guidelines

## Build Commands
- Build: `mkdir build && cd build && cmake .. && make -j$(nproc)`
- Build with tests: `cmake -DWITH_TESTS=ON . && make -j$(nproc)`
- Build with debug: `cmake -DDEBUG=ON -DWITH_DEBUG_SYMBOLS=ON . && make -j$(nproc)`

## Test Commands
- Run all tests: `make test` (after building with WITH_TESTS=ON)
- Run single test: `./output/bin/test_<test_name>` (e.g., `./output/bin/test_schema_factory`)
- Test files are located in `test/` directory with naming pattern `test_*.cpp`

## Code Style Guidelines

### Imports/Includes
- System headers: `#include <system_header.h>`
- Project headers: `#include "project_header.h"`
- Order: system headers first, then third-party, then project headers
- Use relative paths for project includes from include/ directory

### Naming Conventions
- Classes: PascalCase (e.g., `SchemaFactory`)
- Functions/variables: snake_case (e.g., `get_table_info()`)
- Constants: UPPER_SNAKE_CASE (e.g., `TABLE_SWITCH_MERGE`)
- Private members: trailing underscore (e.g., `table_info_`)

### Formatting
- C++17 standard
- 4-space indentation
- Line length: ~100 characters
- Braces on same line for functions/classes
- Use `pragma once` for header guards

### Error Handling
- Return int/Status codes for errors
- Use RAII for resource management
- Log errors with glog: `LOG(ERROR) << "message"`
- Check return values and handle appropriately

### Types
- Use standard library containers (std::vector, std::string, etc.)
- Prefer smart pointers over raw pointers
- Use `int64_t` instead of `long` for 64-bit integers
- Use size_t for sizes/counts

### Project Structure
- Headers in `include/` mirroring `src/` structure
- Implementation in `src/`
- Protobuf files in `proto/`
- Tests in `test/` with `test_` prefix