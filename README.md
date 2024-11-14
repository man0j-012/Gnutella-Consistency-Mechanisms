# CS485 Programming Assignment 3 (PA3)

## Maintaining File Consistency in Hierarchical Gnutella-Style P2P System

### Table of Contents
1. [Project Overview](#project-overview)
2. [Features](#features)
3. [Directory Structure](#directory-structure)
4. [Prerequisites](#prerequisites)
5. [Installation](#installation)
6. [Configuration](#configuration)
7. [Running the Application](#running-the-application)
    - [Starting SuperPeers](#starting-superpeers)
    - [Starting LeafNodes](#starting-leafnodes)
8. [Testing and Experiments](#testing-and-experiments)
    - [Push-Based Consistency Testing](#push-based-consistency-testing)
    - [Pull-Based Consistency Testing](#pull-based-consistency-testing)
    - [Statistics Collection](#statistics-collection)
9. [Sample Outputs](#sample-outputs)
10. [Design Document](#design-document)
11. [Verification](#verification)
12. [Manual](#manual)
13. [Performance Results](#performance-results)
14. [Contributing](#contributing)
15. [License](#license)
16. [Acknowledgements](#acknowledgements)

---

## Project Overview

This project implements **push** and **pull-based consistency mechanisms** in a hierarchical Gnutella-style P2P file-sharing system. The system ensures that all copies of a file across the network remain consistent with the master copy located at the origin server (the leaf node where the file was initially created).

### Key Objectives:
- **Push-Based Consistency:** Origin servers broadcast invalidation messages upon file modifications to ensure that all cached copies are invalidated immediately.
- **Pull-Based Consistency:** Leaf nodes periodically poll origin servers based on a Time-To-Refresh (TTR) value to verify the consistency of their cached files.
- **Statistics Collection:** Collect and analyze statistics to evaluate the effectiveness of both consistency mechanisms.

---

## Features

1. **Push-Based Consistency:**
   - **Invalidation Messages:** Origin servers broadcast invalidation messages to notify all peers of file modifications.
   - **Immediate Invalidations:** Cached copies are invalidated immediately upon receiving invalidation messages, ensuring strong consistency.

2. **Pull-Based Consistency:**
   - **Periodic Polling:** Leaf nodes poll origin servers at intervals defined by TTR to check the validity of cached files.
   - **Lazy and Eager Polling:** Configurable polling strategies (lazy or eager) to optimize performance and consistency.

3. **Statistics Collection:**
   - **Query Results Tracking:** Collect statistics on the percentage of invalid query results received by leaf nodes.
   - **CSV Output:** Export collected statistics to CSV files for analysis.

4. **User Interaction:**
   - **Manual Refresh:** Users can manually refresh outdated files via console commands.
   - **Statistics Display:** Users can view statistics directly from the application.

---


---

## Prerequisites

- **Go Programming Language:** Ensure that Go is installed on your system. You can download it from [https://golang.org/dl/](https://golang.org/dl/).
- **Git:** For version control and cloning the repository.
- **CSV Viewer:** To view and analyze the statistics collected in CSV format (e.g., Microsoft Excel, Google Sheets).

---

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/yourusername/p2p-file-consistency.git
   cd p2p-file-consistency

Navigate to SuperPeers Directory and Build Executable:

cd superpeers
go build -o superpeer.exe main_superpeer.go superpeer.go messages.go

Navigate to LeafNodes Directory and Build Executable:

cd ../leafnodes
go build -o leafnode.exe main_leafnode.go leafnode.go messages.go

superpeer_config1.json
{
  "id": "SP1",
  "address": "127.0.0.1",
  "port": 8001,
  "neighbors": ["SP2", "SP10"],
  "leaf_nodes": ["LN1"],
  "enable_push": true,
  "enable_pull": false
}
leafnode_config1.json

{
  "id": "LN1",
  "address": "127.0.0.1",
  "port": 9001,
  "super_peer": "SP1",
  "enable_push": true,
  "enable_pull": true,
  "TTR": 60,
  "base_directory": "./leafnode_shared",
  "simulate_modifications": false
}


Configuration Parameters:

id: Unique identifier for the SuperPeer or LeafNode.
address: IP address to bind the SuperPeer or LeafNode.
port: Port number for the SuperPeer or LeafNode.
neighbors: (SuperPeer only) List of neighboring SuperPeer IDs to connect with.
leaf_nodes: (SuperPeer only) List of LeafNode IDs connected to this SuperPeer.
super_peer: (LeafNode only) ID of the SuperPeer to connect to.
enable_push: Enable or disable push-based consistency.
enable_pull: Enable or disable pull-based consistency.
TTR: (LeafNode only) Time-To-Refresh in seconds for pull-based polling.
base_directory: Base directory path for storing owned and cached files.
simulate_modifications: Enable or disable automatic file modifications for testing.



