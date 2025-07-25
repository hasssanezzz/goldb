# Goldb Key-Value Database Engine (WIP)

## Project Overview

Goldb is a simple key-value database engine built in Go, inspired by **Log-Structured Merge-Trees (LSM-Trees)**. It ensures durability through a Write-Ahead Log (WAL), uses an in-memory AVL tree (memtable) for temporary storage, and employs SSTables for persistent storage. The project includes a REST API for interaction and can be embedded in other Go applications.

Goldb is designed for learning and lightweight use cases, implementing core LSM-tree principles such as memtables, SSTables, WAL, and compaction, while keeping the architecture simple and easy to understand.
