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
- [Contributing](#contributing)

---

## TODOS

### Project is not finished yet

- [x] Layer the DB engine with an HTTP server, like [CouchDB](https://couchdb.apache.org/).
- [x] Manage periodic flushes.
- [x] Implement a WAL (Write-Ahead Log).
- [x] Make the WAL smarter by ignoring deleted set operations (Compaction).
- [x] Use a compaction algorithm and perform compaction periodically.
- [ ] Write better documentation.
- [ ] Make logging conditional.
- [ ] Utilze go routines.
- [ ] Add the use of bloom filters.

---

## **Setup Instructions**

### **Requirements**

- **Go** (Golang) version 1.20 or higher.
- A Unix-based or Windows operating system.

### **Installation**

1. Clone the repository:

   ```bash
   git clone https://github.com/hasssanezzz/goldb.git
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

## API Endpoints

All operations use custom headers for key specification.

##### Set a Key-Value Pair

```bash
# PUT or POST to set/update a key
curl -X POST http://localhost:3011 \
     -H "Key: mykey" \
     -d "myvalue"
```

##### Get a Value

```bash
# GET a value by key
curl -X GET http://localhost:3011 \
     -H "Key: mykey"
```

##### Delete a Key

```bash
# DELETE a key
curl -X DELETE http://localhost:3011 \
     -H "Key: mykey"
```

##### Scan Keys

```bash
# Get all keys
curl -X GET http://localhost:3011 -H "prefix:"

# Get keys with a prefix
curl -X GET http://localhost:3011 -H "prefix:user"
```

## **How to Use The Engine**

### **1. Initialize the Engine**

Initialize the database engine with a directory for storing SSTables:

```go
package main

import (
    "log"
    "github.com/hasssanezzz/goldb/engine"
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

keys, err := db.Scan("user") // returns all keys starting with "user"
keys, err := db.Scan("")     // returns all keys
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
v

```go
if err := db.Delete("key-1"); err != nil {
   log.Fatalf("Failed to delete data: %v", err)
}
```

---

## **Contributing**

I welcome contributions! Here’s how you can help:

1. Fork the repository.
2. Create a feature branch.
3. Make your changes and submit a pull request.
