**(ignore the main.go file for now, project is not done yet)**
# Goldb Database Engine

Goldb is a lightweight, efficient key-value database engine that leverages the power of a **Log-Structured Merge (LSM) tree** for high performance and reliability. The `engine` package serves as the primary interface, abstracting complex internal mechanisms to provide an intuitive API for users.

---

## TODOS

### Project is not finished

1. Implement a WAL (write ahead log).
1. layer the db engine with an HTTP server, like [couchdb](https://couchdb.apache.org/).
1. Use a compaction alogrithm and perform compaction periodically.
2. Add the use of bloom filters.
3. Manage periodic flushes.

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

---

## **Setup Instructions**

### **Requirements**
- **Go** (Golang) version 1.20 or higher.
- A Unix-based or Windows operating system.

### **Installation**
1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/goldb.git
   cd goldb
   ```

2. Install dependencies:
   ```bash
   go mod tidy
   ```

3. Build the project:
   ```bash
   go build -o goldb
   ```

---

## **How to Use**

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
db.Delete("key-1")
```

---

## **Features**

- **Powered by LSM-Tree:**
  - Combines in-memory writes and immutable SSTables for fast and efficient storage.
  - Supports write-heavy workloads while maintaining read efficiency.

- **Write Optimization:**
  - In-memory AVL tree ensures quick inserts and updates.
  - [Will do] Periodic flushing to SSTables ensures data persistence.

- **Read Optimization:**
  - Binary search through sorted SSTables for efficient lookups.
  - Recently written data is quickly accessible in memory.

- **Persistence and Compaction:**
  - Durable storage with immutable SSTables.
  - [Will do] Optimized disk usage through compaction of older SSTables.

---

## **Design Highlights**

1. **LSM Tree Architecture:**
   - Seamless integration of memtables and SSTables for efficient data management.

2. **Layered Abstraction:**
   - Separates concerns across components like memtables, SSTables, and the storage manager.

3. **Scalability:**
   - Handles large datasets with minimal performance degradation.

4. **User-Friendly API:**
   - The `engine` package abstracts complexities for an intuitive experience.

---

## **Contributing**

I welcome contributions! Here’s how you can help:

1. Fork the repository.
2. Create a feature branch.
3. Make your changes and submit a pull request.
