# README: Goldb Key-Value Database Engine

## Project Overview

Goldb is a simple key-value database engine built in Go, inspired by **Log-Structured Merge-Trees (LSM-Trees)**. It ensures durability through a Write-Ahead Log (WAL), uses an in-memory AVL tree (memtable) for temporary storage, and employs SSTables for persistent storage. The project includes a REST API for interaction and can be embedded in other Go applications.

Goldb is designed for learning and lightweight use cases, implementing core LSM-tree principles such as memtables, SSTables, WAL, and compaction, while keeping the architecture simple and easy to understand.

## Project Structure

- **`cmd/`**: Contains the main package to run the server.
- **`internal/`**: Core components:
  - `index_manager/`: Manages indexes, including SSTables and levels.
  - `memtable/`: In-memory AVL tree for temporary key-value storage.
  - `storage_manager/`: Handles data writing and reading to persistent storage.
  - `wal/`: Write-Ahead Log for durability.
- **`api/`**: Provides RESTful API endpoints.

## LSM-Tree Inspiration

Goldb is inspired by **Log-Structured Merge-Trees (LSM-Trees)**, a popular design for key-value storage systems optimized for write-heavy workloads. Here's how Goldb implements LSM-tree-like behavior:

1. **Memtable (In-Memory AVL Tree)**:

   - Stores key-value pairs temporarily in memory.
   - Provides fast writes and reads for recently inserted data.

2. **Write-Ahead Log (WAL)**:

   - Ensures durability by logging all writes before they are applied to the memtable.
   - Allows recovery of data in case of a crash.

3. **SSTables (Sorted String Tables)**:

   - Immutable, sorted files on disk that store key-value pairs.
   - When the memtable is full, it is flushed to disk as an SSTable.

4. **Compaction**:

   - Merges multiple SSTables into a single larger SSTable (a "level") when the number of SSTables exceeds a threshold.
   - Improves read performance and reduces disk usage by removing redundant or deleted keys.

5. **Index Manager**:
   - Manages the organization of SSTables and levels.
   - Handles compaction and ensures efficient key lookups across the memtable, SSTables, and levels.

## Getting Started

1. **Prerequisites**:

   - Go installed on your system.

2. **Setup**:
   - Clone the repository:
     ```bash
     git clone https://github.com/hasssanezzz/goldb.git
     ```
   - Build the project:
     ```bash
     go build -o goldb-engine
     ```
   - Run the server:
     ```bash
     ./goldb-engine
     ```
   - The server runs on `http://localhost:3011`.

## Using the REST API

### Endpoints

- **POST /**: Set a key-value pair.

  - Headers: `key`
  - Data: Value as a byte array.
  - Example:
    ```bash
    curl -X POST -H "key: testKey" -d "testValue" http://localhost:3011
    ```

- **GET /**: Get the value by key.

  - Headers: `key`
  - Example:
    ```bash
    curl -X GET -H "key: testKey" http://localhost:3011
    ```

- **DELETE /**: Delete a key.

  - Headers: `key`
  - Example:
    ```bash
    curl -X DELETE -H "key: testKey" http://localhost:3011
    ```

- **GET / with prefix header**: Scan keys with a prefix.
  - Headers: `prefix`
  - Example:
    ```bash
    curl -X GET -H "prefix: test" http://localhost:3011
    ```

## Using the Go Package

1. **Import the Package**:

   ```go
   import "github.com/hasssanezzz/goldb"
   ```

2. **Create and Use the Engine**:

   ```go
   package main

   import (
       "log"
       "github.com/hasssanezzz/goldb"
   )

   func main() {
       db, err := goldb.New("path/to/home")
       if err != nil {
           log.Fatal(err)
       }
       defer db.Close()

       // Set a key-value pair
       err = db.Set("testKey", []byte("testValue"))
       if err != nil {
           log.Fatal(err)
       }

       // Get a value by key
       value, err := db.Get("testKey")
       if err != nil {
           log.Fatal(err)
       }
       log.Println(string(value))


       keys, err := db.Scan("user") // returns all keys starting with "user"
       keys, err := db.Scan("")     // returns all keys

       // Delete a key
       err = db.Delete("testKey")
       if err != nil {
           log.Fatal(err)
       }
   }
   ```

## Todos

Project is not finished yet.

- [x] Layer the DB engine with an HTTP server, like CouchDB.
- [x] Manage periodic flushes.
- [x] Implement a WAL (Write-Ahead Log).
- [x] Make the WAL smarter by ignoring deleted set operations (Compaction).
- [x] Use a compaction algorithm and perform compaction periodically.
- [ ] Utilze go routines.
- [ ] Add the use of bloom filters.
- [ ] Write better documentation.
- [ ] Make logging conditional.
