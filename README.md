# Goldb Database Engine

Goldb is a lightweight, efficient key-value database engine that leverages the power of a **Log-Structured Merge (LSM) tree** for high performance and reliability. The `engine` package serves as the primary interface, abstracting complex internal mechanisms to provide an intuitive API for users.

## Table of Contents

- [TODOs](#todos)
- [How It Works](#how-it-works)
  - [Setup Instructions](#setup-instructions)
  - [Running the Server](#running-the-server)
  - [RESTful API](#restful-api)
  - [How to Use The Engine](#how-to-use-the-engine)
- [Features](#features)
- [Project Design](#project-design)
- [Highlights](#highlights)
- [Contributing](#contributing)

---

## TODOS

### Project is not finished yet

- [x] Layer the DB engine with an HTTP server, like [CouchDB](https://couchdb.apache.org/).
- [x] Manage periodic flushes.
- [x] Implement a WAL (Write-Ahead Log).
- [ ] Write better documentation.
- [ ] Make the WAL smarter by ignoring deleted set operations.
- [ ] Make logging conditional.
- [ ] Utilze go routines.
- [ ] Add locks to avoid concurrency issues.
- [ ] Use a compaction algorithm and perform compaction periodically.
- [ ] Add the use of bloom filters.

---

## **How It Works**

Goldb is built around the **LSM tree**, a modern data structure optimized for write-heavy workloads. Here’s how it operates:

1. **In-Memory Data Handling (Memtable):**

   - Data is first written to an in-memory AVL tree, acting as the **active layer** of the LSM tree.
   - The memtable ensures fast writes and lookups, maintaining sorted data for efficient flushing.

2. **Persistent Storage (SSTables):**

   - When the memtable reaches a size limit, its contents are flushed to disk as **Sorted String Tables (SSTables)**.
   - SSTables are immutable and sorted, enabling efficient binary search operations during reads.

3. **Storage Management:**

   - The engine manages the lifecycle of SSTables, including merging and compaction, ensuring that older tables are consolidated into fewer, larger ones to optimize performance.

4. **High-Level API:**
   - The `engine` package provides an easy-to-use interface for storing, retrieving, and deleting key-value pairs, while abstracting the complexities of the LSM tree.

## **Setup Instructions**

### **Requirements**

- **Go** (Golang) version 1.20 or higher.
- A Unix-based or Windows operating system.

### **Installation**

1. Clone the repository:

   ```bash
   git clone https://github.com/hasssanezzz/goldb-engine.git
   cd goldb-engine
   ```

2. Install dependencies:

   ```bash
   go mod tidy
   ```

3. Build the project:
   ```bash
   go build -o goldb
   ```

## **Running the Server**

### **Default Execution**

By default, the server will:

- Run on `localhost` at port `3011`.
- Store data in the `~/.goldb` directory.

Run the server:

```bash
./goldb
```

### **Custom Options**

You can customize the server's host, port, and data directory using flags:

```bash
./goldb -h <host> -p <port> -s <source_directory>
```

#### Example:

```bash
./goldb -h 0.0.0.0 -p 8080 -s /path/to/data
```

### **Help Command**

To view the available options:

```bash
./goldb --help
```

## **RESTful API**

The database engine is accessible via HTTP. Below are the available endpoints:

### **1. GET**

Retrieve the value of a key.

#### Request:

```http
GET /
Key: <key>
```

#### Response:

- **200 OK**: Returns the value.
- **404 Not Found**: Key does not exist.
- **500 Internal Server Error**: Unexpected server error.

---

### **2. POST/PUT**

Insert or update a key-value pair.

#### Request:

```http
POST /
Key: <key>

<body>
<value>
</body>
```

#### Response:

- **200 OK**: Returns the value.
- **500 Internal Server Error**: If the operation fails.

---

### **3. DELETE**

Delete a key-value pair.

#### Request:

```http
DELETE /
Key: <key>
```

#### Response:

- **200 OK**: Key deleted successfully.

## **How to Use The Engine**

### **1. Initialize the Engine**

Initialize the database engine with a directory for storing SSTables:

```go
package main

import (
    "log"
    "github.com/hasssanezzz/goldb-engine/engine"
)

func main() {
    db, err := engine.New("./.db")
    if err != nil {
        log.Fatalf("Failed to initialize engine: %v", err)
    }

    defer db.Close() // Ensure proper cleanup
}
```

### **2. Insert Data**

Store key-value pairs using the `Set` method:

```go
db.Set("key-1", "value-1")
db.Set("key-2", "value-2")
```

### **3. Retrieve Data**

Retrieve stored values using the `Get` method:

```go
value, err := db.Get("key-1")
if err != nil {
    log.Printf("Failed to retrieve value: %v", err)
} else {
    log.Printf("Retrieved value: %s", value)
}
```

### **4. Flush Data**

Flush the in-memory data to SSTables manually if needed:

```go
err = db.Flush()
if err != nil {
    log.Fatalf("Failed to flush data: %v", err)
}
```

### **5. Delete Data**

Remove a key-value pair using the `Delete` method:

```go
if err := db.Delete("key-1"); err != nil {
   log.Fatalf("Failed to delete data: %v", err)
}
```

---

## **Features**

- **Powered by LSM-Tree:**

  - Combines in-memory writes and immutable SSTables for fast and efficient storage.
  - Supports write-heavy workloads while maintaining read efficiency.

- **Write Optimization:**

  - In-memory AVL tree ensures quick inserts and updates.
  - Periodic flushing to SSTables ensures data persistence.

- **Read Optimization:**

  - Binary search through sorted SSTables for efficient lookups.
  - Recently written data is quickly accessible in memory.

- **Persistence and Recovery:**
  - **Write-Ahead Log (WAL)**: Ensures durability and enables crash recovery by logging all writes before they are applied to the in-memory structure.
  - Durable storage with immutable SSTables.
  - [Will do] Optimized disk usage through compaction of older SSTables.

## **Project Design**

1. **API Layer**:

   - Implements HTTP endpoints using the `api` package.
   - Maps HTTP requests to database operations (e.g., GET, POST, DELETE).

2. **Engine Layer**:

   - Manages in-memory storage, SSTables, and compaction.
   - Provides a clean interface for database operations.

3. **Write-Ahead Log (WAL)**:

   - Logs all write operations before applying them to in-memory structures.
   - Ensures data durability and enables recovery from crashes.

4. **Periodic Flushing**:

   - Ensures that in-memory data is frequently persisted to disk.

5. **Server Initialization**:

   - Automatically creates a `.goldb` directory in the user’s home if no custom directory is provided.

## **Contributing**

I welcome contributions! Here’s how you can help:

1. Fork the repository.
2. Create a feature branch.
3. Make your changes and submit a pull request.
