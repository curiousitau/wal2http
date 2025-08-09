# PostgreSQL Replication Checker - Rust Edition

A Rust implementation of a PostgreSQL logical replication client that connects to a database, creates replication slots, and displays changes in real-time. This is a Rust port of the original C++ implementation from [https://github.com/fkfk000/replication_checker](https://github.com/fkfk000/replication_checker).

>　https://www.postgresql.org/docs/current/protocol-replication.html

## Features

- **Logical Replication**: Connects to PostgreSQL as a replication client using the logical replication protocol
- **Real-time Change Display**: Shows INSERT, UPDATE, DELETE, TRUNCATE operations as they happen
- **Streaming Transaction Support**: Handles both regular and streaming (large) transactions
- **Built with libpq-sys**: Uses low-level PostgreSQL libpq bindings for maximum performance and control
- **Comprehensive Logging**: Uses tracing for structured logging and debugging

## Prerequisites

- PostgreSQL server with logical replication enabled (`wal_level = logical`)
- A publication created on the source database
- libpq development libraries installed on your system
- Rust 1.70+ with Cargo

### PostgreSQL Setup

1. Enable logical replication in your PostgreSQL configuration:
   ```sql
   ALTER SYSTEM SET wal_level = logical;
   -- Restart PostgreSQL server after this change
   ```

2. Create a publication for the tables you want to replicate:
   ```sql
   CREATE PUBLICATION my_publication FOR TABLE table1, table2;
   -- Or for all tables:
   CREATE PUBLICATION my_publication FOR ALL TABLES;
   ```

3. Create a user with replication privileges:
   ```sql
   CREATE USER replicator WITH REPLICATION LOGIN PASSWORD 'password';
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
   ```

## Installation

> [!WARNING]
> Please note : PostgreSQL DB version must equal or higher to version 14, more information refer to below link. 
> * https://www.postgresql.org/docs/14/protocol-replication.html
> * https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS


### From Source

```bash
git clone <this-repository>
cd pg_replica_rs
cargo build --release
```

### System Dependencies

Make sure you have libpq development libraries installed:

**Ubuntu/Debian:**
```bash
sudo apt-get install libpq-dev
```

**CentOS/RHEL/Fedora:**
```bash
sudo yum install postgresql-devel
# or
sudo dnf install postgresql-devel
```

**macOS:**
```bash
brew install postgresql
```

## Usage

### Basic Usage

```bash
# Set environment variables
export SlotName="my_slot"
export PubName="my_publication"

# Run the replication checker
./target/release/pg_replica_rs user azureuser replication database host 127.0.0.1 dbname postgres port 5432
```

### Command Line Arguments

The program accepts PostgreSQL connection parameters as space-separated key-value pairs:

```bash
pg_replica_rs user <username> password <password> host <hostname> port <port> dbname <database> [additional_params...]
```

### Environment Variables

- `SlotName`: Name of the replication slot to create/use (default: "sub")
- `PubName`: Name of the publication to subscribe to (default: "pub")

### Logging

Set the `RUST_LOG` environment variable to control logging levels:

```bash
# Info level (default)
RUST_LOG=info ./target/release/pg_replica_rs ...

# Debug level for detailed output
RUST_LOG=debug ./target/release/pg_replica_rs ...

# Only errors
RUST_LOG=error ./target/release/pg_replica_rs ...
```

## Example Output

When running, you'll see output like:

```
2024-01-15T10:30:00.123Z INFO pg_replica_rs: Connection string: user=postgres password=*** host=localhost port=5432 dbname=testdb
2024-01-15T10:30:00.124Z INFO pg_replica_rs: Slot name: my_slot  
2024-01-15T10:30:00.124Z INFO pg_replica_rs: Publication name: my_publication
2024-01-15T10:30:00.125Z INFO pg_replica_rs: Successfully connected to database server
2024-01-15T10:30:00.126Z INFO pg_replica_rs: System identification successful
2024-01-15T10:30:00.127Z INFO pg_replica_rs: Creating replication slot: my_slot
2024-01-15T10:30:00.128Z INFO pg_replica_rs: Started receiving data from database server

BEGIN: Xid 12345
table public.users: INSERT: id: 100 name: John Doe email: john@example.com 
table public.orders: INSERT: id: 50 user_id: 100 amount: 99.99 
COMMIT

BEGIN: Xid 12346
table public.users UPDATE Old REPLICA IDENTITY: id: 100 New Row: id: 100 name: John Smith email: john.smith@example.com 
COMMIT

BEGIN: Xid 12347  
table public.orders: DELETE: (INDEX): id: 50 user_id: 100 
COMMIT
```

## Architecture

The implementation consists of several key modules:

- **`main.rs`**: Application entry point, argument parsing, and async runtime setup
- **`server.rs`**: Main replication server that manages the PostgreSQL connection and message processing
- **`parser.rs`**: Protocol message parser for PostgreSQL logical replication messages
- **`types.rs`**: Data structures for relations, tuples, and replication messages
- **`utils.rs`**: Utility functions for connection management, byte manipulation, and PostgreSQL integration

### Key Components

1. **Connection Management**: Safe wrapper around libpq connections with proper resource cleanup
2. **Protocol Parsing**: Complete implementation of PostgreSQL logical replication protocol
3. **Message Processing**: Handlers for all logical replication message types (BEGIN, COMMIT, INSERT, UPDATE, DELETE, TRUNCATE, etc.)
4. **Feedback System**: Implements the feedback protocol to acknowledge processed WAL positions

## Supported Operations

- ✅ **BEGIN** - Transaction start
- ✅ **COMMIT** - Transaction commit  
- ✅ **INSERT** - Row insertions
- ✅ **UPDATE** - Row updates (with old/new value support)
- ✅ **DELETE** - Row deletions
- ✅ **TRUNCATE** - Table truncation
- ✅ **RELATION** - Table schema information
- ✅ **Streaming Transactions** - Large transaction support
- ✅ **Keep-alive** - Connection health monitoring

## Limitations

- Currently displays changes in a human-readable format rather than processing them
- Only supports text data types (binary types show as raw data)
- Replication slot management is basic (creates slot, doesn't handle cleanup on exit)
- Error recovery is minimal (will exit on most errors)

## Troubleshooting

### Connection Issues

1. **"Connection failed"**: Check your PostgreSQL server is running and accessible
2. **"Permission denied"**: Ensure your user has REPLICATION privilege
3. **"Replication slot creation failed"**: The slot might already exist, or you lack privileges

### Compilation Issues

1. **"libpq not found"**: Install PostgreSQL development libraries
2. **"linking failed"**: Ensure libpq is in your library path

### Runtime Issues

1. **No data received**: Check that your publication includes the tables being modified
2. **"Unknown relation"**: The replication stream may be out of sync; restart the application

### docker Build and run:

```bash
docker build -t pg_replica_rs .
docker run -e SlotName=my_slot -e PubName=my_pub pg_replica_rs user postgres password secret host host.docker.internal port 5432 dbname mydb
```

## License

This project is licensed under the same terms as the original C++ implementation.

## Acknowledgments

- Based on the original C++ implementation by [fkfk000](https://github.com/fkfk000/replication_checker)
- Uses the excellent [libpq-sys](https://crates.io/crates/libpq-sys) crate for PostgreSQL integration
- Built with the [Tokio](https://tokio.rs/) async runtime
