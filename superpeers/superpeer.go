// superpeer.go
package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "net"
    "os"
    "sync"
    "time"

    "github.com/google/uuid" // For generating unique message IDs
)

// SuperPeerConfig represents the configuration for a SuperPeer
type SuperPeerConfig struct {
    ID          string   `json:"id"`
    Address     string   `json:"address"`
    Port        int      `json:"port"`
    Neighbors   []string `json:"neighbors"`  // IDs of neighboring SuperPeers
    LeafNodes   []string `json:"leaf_nodes"` // IDs of connected LeafNodes
    EnablePush  bool     `json:"enable_push"`
    EnablePull  bool     `json:"enable_pull"`
}

// FileMetadataExtended extends FileMetadata with version and consistency state
type FileMetadataExtended struct {
    FileName         string `json:"file_name"`
    VersionNumber    int    `json:"version_number"`
    OriginServerID   string `json:"origin_server_id"`
    ConsistencyState string `json:"consistency_state"` // "valid" or "invalid"
    LastModified     string `json:"last_modified_time"`
}

// CacheEntry represents an entry in the message cache to prevent duplicate processing
type CacheEntry struct {
    OriginID     string
    UpstreamConn net.Conn
    Timestamp    time.Time
}

// SuperPeer represents a SuperPeer in the network
type SuperPeer struct {
    Config        SuperPeerConfig
    NeighborConns map[string]net.Conn              // Map of Neighbor SuperPeer ID to connection
    LeafNodeConns map[string]net.Conn              // Map of LeafNode ID to connection
    FileIndex     map[string]map[string]struct{}   // FileName -> Set of LeafNode IDs
    MessageCache  map[string]CacheEntry            // MessageID -> CacheEntry
    FileMetadata  map[string]FileMetadataExtended  // FileName -> Metadata
    mu            sync.RWMutex                     // Mutex to protect shared resources
}

// NewSuperPeer initializes a new SuperPeer instance
func NewSuperPeer(config SuperPeerConfig) *SuperPeer {
    return &SuperPeer{
        Config:        config,
        NeighborConns: make(map[string]net.Conn),
        LeafNodeConns: make(map[string]net.Conn),
        FileIndex:     make(map[string]map[string]struct{}),
        MessageCache:  make(map[string]CacheEntry),
        FileMetadata:  make(map[string]FileMetadataExtended),
    }
}

// Start launches the SuperPeer's server to listen for incoming connections
func (sp *SuperPeer) Start(wg *sync.WaitGroup) {
    defer wg.Done()

    address := fmt.Sprintf("%s:%d", sp.Config.Address, sp.Config.Port)
    listener, err := net.Listen("tcp", address)
    if err != nil {
        log.Fatalf("Super-Peer %s: Failed to listen on %s: %v", sp.Config.ID, address, err)
    }
    defer listener.Close()
    log.Printf("Super-Peer %s: Listening on %s", sp.Config.ID, address)

    // Optionally, connect to neighboring SuperPeers
    sp.connectToNeighbors()

    // Continuously accept incoming connections
    for {
        conn, err := listener.Accept()
        if err != nil {
            log.Printf("Super-Peer %s: Error accepting connection: %v", sp.Config.ID, err)
            continue
        }
        go sp.handleConnection(conn)
    }
}

// connectToNeighbors establishes outgoing connections to neighboring SuperPeers
func (sp *SuperPeer) connectToNeighbors() {
    for _, neighborID := range sp.Config.Neighbors {
        // Assume neighbor addresses are known or can be resolved via a registry
        // For simplicity, we'll construct the address based on neighbor ID
        neighborPort := getPortOffset(neighborID) // Implement getPortOffset accordingly
        neighborAddress := fmt.Sprintf("127.0.0.1:%d", neighborPort)

        conn, err := net.Dial("tcp", neighborAddress)
        if err != nil {
            log.Printf("Super-Peer %s: Error connecting to neighbor %s at %s: %v", sp.Config.ID, neighborID, neighborAddress, err)
            continue
        }

        sp.mu.Lock()
        sp.NeighborConns[neighborID] = conn
        sp.mu.Unlock()

        // Send PeerIDMessage upon connection
        peerIDMsg := PeerIDMessage{
            MessageType: MsgTypePeerID,
            PeerType:    SuperPeerType,
            PeerID:      sp.Config.ID,
        }
        err = sendJSONMessage(conn, peerIDMsg)
        if err != nil {
            log.Printf("Super-Peer %s: Error sending PeerIDMessage to neighbor %s: %v", sp.Config.ID, neighborID, err)
            conn.Close()
            sp.mu.Lock()
            delete(sp.NeighborConns, neighborID)
            sp.mu.Unlock()
            continue
        }

        log.Printf("Super-Peer %s: Connected to neighbor SuperPeer %s at %s", sp.Config.ID, neighborID, neighborAddress)

        // Start listening for messages from this neighbor
        go sp.listenToSuperPeer(conn, neighborID)
    }
}

// handleConnection determines the type of peer (SuperPeer or LeafNode) and handles accordingly
func (sp *SuperPeer) handleConnection(conn net.Conn) {
    decoder := json.NewDecoder(conn)
    var peerIDMsg PeerIDMessage
    err := decoder.Decode(&peerIDMsg)
    if err != nil {
        log.Printf("Super-Peer %s: Error decoding PeerIDMessage: %v", sp.Config.ID, err)
        conn.Close()
        return
    }

    if peerIDMsg.MessageType != MsgTypePeerID {
        log.Printf("Super-Peer %s: Expected PeerIDMessage, received %s", sp.Config.ID, peerIDMsg.MessageType)
        conn.Close()
        return
    }

    switch peerIDMsg.PeerType {
    case SuperPeerType:
        sp.handleSuperPeer(conn, peerIDMsg.PeerID)
    case LeafNodeType:
        sp.handleLeafNode(conn, peerIDMsg.PeerID)
    default:
        log.Printf("Super-Peer %s: Unknown PeerType '%s' from PeerID '%s'", sp.Config.ID, peerIDMsg.PeerType, peerIDMsg.PeerID)
        conn.Close()
    }
}

// handleSuperPeer manages connections and message handling for neighboring SuperPeers
func (sp *SuperPeer) handleSuperPeer(conn net.Conn, peerID string) {
    sp.mu.Lock()
    if _, exists := sp.NeighborConns[peerID]; exists {
        sp.mu.Unlock()
        log.Printf("Super-Peer %s: SuperPeer %s already connected. Closing duplicate connection.", sp.Config.ID, peerID)
        conn.Close()
        return
    }
    sp.NeighborConns[peerID] = conn
    sp.mu.Unlock()
    log.Printf("Super-Peer %s: Connected to SuperPeer %s", sp.Config.ID, peerID)

    // Start listening for messages from this SuperPeer
    go sp.listenToSuperPeer(conn, peerID)
}

// handleLeafNode manages connections and message handling for LeafNodes
func (sp *SuperPeer) handleLeafNode(conn net.Conn, leafNodeID string) {
    sp.mu.Lock()
    if _, exists := sp.LeafNodeConns[leafNodeID]; exists {
        sp.mu.Unlock()
        log.Printf("Super-Peer %s: LeafNode %s already connected. Closing duplicate connection.", sp.Config.ID, leafNodeID)
        conn.Close()
        return
    }
    sp.LeafNodeConns[leafNodeID] = conn
    sp.mu.Unlock()
    log.Printf("Super-Peer %s: Connected to LeafNode %s", sp.Config.ID, leafNodeID)

    // Send PeerIDMessage upon connection
    peerIDMsg := PeerIDMessage{
        MessageType: MsgTypePeerID,
        PeerType:    SuperPeerType, // LeafNodes will interpret this correctly
        PeerID:      sp.Config.ID,
    }
    err := sendJSONMessage(conn, peerIDMsg)
    if err != nil {
        log.Printf("Super-Peer %s: Error sending PeerIDMessage to LeafNode %s: %v", sp.Config.ID, leafNodeID, err)
        conn.Close()
        sp.mu.Lock()
        delete(sp.LeafNodeConns, leafNodeID)
        sp.mu.Unlock()
        return
    }

    // Start listening for messages from this LeafNode
    go sp.listenToLeafNode(conn, leafNodeID)
}

// listenToSuperPeer continuously listens for messages from a connected SuperPeer
func (sp *SuperPeer) listenToSuperPeer(conn net.Conn, peerID string) {
    decoder := json.NewDecoder(conn)
    for {
        var msg map[string]interface{}
        err := decoder.Decode(&msg)
        if err != nil {
            log.Printf("Super-Peer %s: Connection to SuperPeer %s lost: %v", sp.Config.ID, peerID, err)
            sp.mu.Lock()
            delete(sp.NeighborConns, peerID)
            sp.mu.Unlock()
            conn.Close()
            return
        }

        messageType, ok := msg["message_type"].(string)
        if !ok {
            log.Printf("Super-Peer %s: Received message without message_type from SuperPeer %s", sp.Config.ID, peerID)
            continue
        }

        switch messageType {
        case MsgTypeFileRegistration:
            var regMsg FileRegistrationMessage
            err := mapToStruct(msg, &regMsg)
            if err != nil {
                log.Printf("Super-Peer %s: Error parsing FileRegistrationMessage from SuperPeer %s: %v", sp.Config.ID, peerID, err)
                continue
            }
            sp.handleFileRegistration(regMsg)
        case MsgTypeInvalidation:
            var invMsg InvalidationMessage
            err := mapToStruct(msg, &invMsg)
            if err != nil {
                log.Printf("Super-Peer %s: Error parsing InvalidationMessage from SuperPeer %s: %v", sp.Config.ID, peerID, err)
                continue
            }
            if sp.Config.EnablePush {
                sp.handleInvalidation(invMsg, conn)
            }
        // Handle other message types as needed
        default:
            log.Printf("Super-Peer %s: Unknown message_type '%s' from SuperPeer %s", sp.Config.ID, messageType, peerID)
        }
    }
}

// listenToLeafNode continuously listens for messages from a connected LeafNode
func (sp *SuperPeer) listenToLeafNode(conn net.Conn, leafNodeID string) {
    decoder := json.NewDecoder(conn)
    for {
        var msg map[string]interface{}
        err := decoder.Decode(&msg)
        if err != nil {
            log.Printf("Super-Peer %s: Connection to LeafNode %s lost: %v", sp.Config.ID, leafNodeID, err)
            sp.mu.Lock()
            delete(sp.LeafNodeConns, leafNodeID)
            sp.mu.Unlock()
            conn.Close()
            return
        }

        messageType, ok := msg["message_type"].(string)
        if !ok {
            log.Printf("Super-Peer %s: Received message without message_type from LeafNode %s", sp.Config.ID, leafNodeID)
            continue
        }

        switch messageType {
        case MsgTypeFileRegistration:
            var regMsg FileRegistrationMessage
            err := mapToStruct(msg, &regMsg)
            if err != nil {
                log.Printf("Super-Peer %s: Error parsing FileRegistrationMessage from LeafNode %s: %v", sp.Config.ID, leafNodeID, err)
                continue
            }
            sp.handleFileRegistration(regMsg)
        case MsgTypeInvalidation:
            var invMsg InvalidationMessage
            err := mapToStruct(msg, &invMsg)
            if err != nil {
                log.Printf("Super-Peer %s: Error parsing InvalidationMessage from LeafNode %s: %v", sp.Config.ID, leafNodeID, err)
                continue
            }
            if sp.Config.EnablePush {
                sp.handleInvalidation(invMsg, conn)
            }
        case MsgTypePollRequest:
            var pollReq PollRequestMessage
            err := mapToStruct(msg, &pollReq)
            if err != nil {
                log.Printf("Super-Peer %s: Error parsing PollRequestMessage from LeafNode %s: %v", sp.Config.ID, leafNodeID, err)
                continue
            }
            if sp.Config.EnablePull {
                sp.handlePollRequest(pollReq, conn)
            }
        case MsgTypeRefreshRequest:
            var refreshReq RefreshRequestMessage
            err := mapToStruct(msg, &refreshReq)
            if err != nil {
                log.Printf("Super-Peer %s: Error parsing RefreshRequestMessage from LeafNode %s: %v", sp.Config.ID, leafNodeID, err)
                continue
            }
            if sp.Config.EnablePull {
                sp.handleRefreshRequest(refreshReq, conn)
            }
        // Handle other message types as needed
        default:
            log.Printf("Super-Peer %s: Unknown message_type '%s' from LeafNode %s", sp.Config.ID, messageType, leafNodeID)
        }
    }
}

// handleFileRegistration processes a FileRegistrationMessage
func (sp *SuperPeer) handleFileRegistration(msg FileRegistrationMessage) {
    sp.mu.Lock()
    defer sp.mu.Unlock()

    for _, file := range msg.Files {
        if _, exists := sp.FileIndex[file.FileName]; !exists {
            sp.FileIndex[file.FileName] = make(map[string]struct{})
        }
        sp.FileIndex[file.FileName][msg.LeafNodeID] = struct{}{}
        log.Printf("Super-Peer %s: Registered file '%s' from LeafNode %s", sp.Config.ID, file.FileName, msg.LeafNodeID)

        // Initialize FileMetadataExtended if not present
        if _, exists := sp.FileMetadata[file.FileName]; !exists {
            // Ensure the owned directory exists
            ownedDir := fmt.Sprintf("./shared_files/%s/owned", sp.Config.ID)
            err := os.MkdirAll(ownedDir, os.ModePerm)
            if err != nil {
                log.Printf("Super-Peer %s: Error creating owned directory '%s': %v", sp.Config.ID, ownedDir, err)
                continue
            }

            sp.FileMetadata[file.FileName] = FileMetadataExtended{
                FileName:         file.FileName,
                VersionNumber:    1, // Start with version 1
                OriginServerID:   sp.Config.ID, // Assuming SP1 is the origin server
                ConsistencyState: "valid",
                LastModified:     time.Now().Format(time.RFC3339),
            }

            // Optionally, ensure the file exists on disk
            filePath := fmt.Sprintf("./shared_files/%s/owned/%s", sp.Config.ID, file.FileName)
            if _, err := os.Stat(filePath); os.IsNotExist(err) {
                initialContent := fmt.Sprintf("This is the initial content of %s.", file.FileName)
                err := ioutil.WriteFile(filePath, []byte(initialContent), 0644)
                if err != nil {
                    log.Printf("Super-Peer %s: Error creating file '%s': %v", sp.Config.ID, file.FileName, err)
                }
            }
        }
    }
}

// handleInvalidation processes an InvalidationMessage
func (sp *SuperPeer) handleInvalidation(msg InvalidationMessage, sourceConn net.Conn) {
    sp.mu.Lock()
    defer sp.mu.Unlock()

    log.Printf("Super-Peer %s: Received INVALIDATION for '%s' from Origin Server %s (Version: %d)", sp.Config.ID, msg.FileName, msg.OriginServerID, msg.VersionNumber)

    // Check if the SuperPeer has the file registered
    leafNodes, exists := sp.FileIndex[msg.FileName]
    if exists {
        // Deregister the file from the Origin Server
        delete(leafNodes, msg.OriginServerID)
        log.Printf("Super-Peer %s: Deregistered '%s' from Origin Server %s", sp.Config.ID, msg.FileName, msg.OriginServerID)

        // Update FileMetadataExtended if necessary
        fileMeta, metaExists := sp.FileMetadata[msg.FileName]
        if metaExists && fileMeta.OriginServerID == msg.OriginServerID {
            fileMeta.ConsistencyState = "invalid"
            sp.FileMetadata[msg.FileName] = fileMeta
        }

        // If no LeafNodes have the file, remove the entry from FileIndex and FileMetadata
        if len(leafNodes) == 0 {
            delete(sp.FileIndex, msg.FileName)
            delete(sp.FileMetadata, msg.FileName)
            log.Printf("Super-Peer %s: Removed '%s' from FileIndex and FileMetadata as no LeafNodes are hosting it", sp.Config.ID, msg.FileName)
        }

        // Notify connected LeafNodes to invalidate their cached copies
        for leafNodeID, conn := range sp.LeafNodeConns {
            invalidateMsg := msg // Reuse the same INVALIDATION message
            err := sendJSONMessage(conn, invalidateMsg)
            if err != nil {
                log.Printf("Super-Peer %s: Error sending INVALIDATION to LeafNode %s: %v", sp.Config.ID, leafNodeID, err)
                continue
            }
            log.Printf("Super-Peer %s: Sent INVALIDATION for '%s' to LeafNode %s", sp.Config.ID, msg.FileName, leafNodeID)
        }

        // Propagate the INVALIDATION message to neighboring SuperPeers
        for neighborID, neighborConn := range sp.NeighborConns {
            // Avoid sending back to the source SuperPeer to prevent loops
            if neighborConn == sourceConn {
                continue
            }

            // Implement message deduplication
            if _, processed := sp.MessageCache[msg.MsgID]; processed {
                continue
            }

            err := sendJSONMessage(neighborConn, msg)
            if err != nil {
                log.Printf("Super-Peer %s: Error forwarding INVALIDATION to SuperPeer %s: %v", sp.Config.ID, neighborID, err)
                continue
            }
            log.Printf("Super-Peer %s: Forwarded INVALIDATION for '%s' to SuperPeer %s", sp.Config.ID, msg.FileName, neighborID)

            // Add to MessageCache
            sp.MessageCache[msg.MsgID] = CacheEntry{
                OriginID:     msg.OriginServerID,
                UpstreamConn: neighborConn,
                Timestamp:    time.Now(),
            }
        }
    } else {
        log.Printf("Super-Peer %s: File '%s' not found in FileIndex", sp.Config.ID, msg.FileName)
    }
}

// handlePollRequest processes a PollRequestMessage from a LeafNode
func (sp *SuperPeer) handlePollRequest(msg PollRequestMessage, conn net.Conn) {
    sp.mu.RLock()
    defer sp.mu.RUnlock()

    fileMeta, exists := sp.FileMetadata[msg.FileName]
    if !exists {
        // File does not exist on origin server
        pollResp := PollResponseMessage{
            MessageType:      MsgTypePollResponse,
            MessageID:        msg.MessageID,
            FileName:         msg.FileName,
            Status:           "invalid",
            NewVersionNumber: -1,
        }
        sendJSONMessage(conn, pollResp)
        return
    }

    // Compare version numbers
    if msg.CurrentVersionNumber < fileMeta.VersionNumber {
        // File has been updated
        pollResp := PollResponseMessage{
            MessageType:      MsgTypePollResponse,
            MessageID:        msg.MessageID,
            FileName:         msg.FileName,
            Status:           "invalid",
            NewVersionNumber: fileMeta.VersionNumber,
        }
        sendJSONMessage(conn, pollResp)
    } else {
        // File is still valid
        pollResp := PollResponseMessage{
            MessageType:      MsgTypePollResponse,
            MessageID:        msg.MessageID,
            FileName:         msg.FileName,
            Status:           "valid",
            NewVersionNumber: fileMeta.VersionNumber,
        }
        sendJSONMessage(conn, pollResp)
    }
}

// handleRefreshRequest processes a RefreshRequestMessage from a LeafNode
func (sp *SuperPeer) handleRefreshRequest(msg RefreshRequestMessage, conn net.Conn) {
    sp.mu.RLock()
    defer sp.mu.RUnlock()

    log.Printf("Super-Peer %s: Received RefreshRequest for '%s' from LeafNode", sp.Config.ID, msg.FileName)

    fileMeta, exists := sp.FileMetadata[msg.FileName]
    if !exists {
        // File does not exist on origin server
        log.Printf("Super-Peer %s: File '%s' does not exist for RefreshRequest", sp.Config.ID, msg.FileName)
        // Send RefreshResponseMessage indicating file does not exist
        refreshResp := RefreshResponseMessage{
            MessageType:    MsgTypeRefreshResponse,
            MessageID:      msg.MessageID,
            FileName:       msg.FileName,
            FileData:       []byte{}, // Empty data
            VersionNumber:  -1,         // Indicates non-existence
            LastModified:   "",
        }
        sendJSONMessage(conn, refreshResp)
        return
    }

    // Read the file data from disk (assuming files are stored locally)
    filePath := fmt.Sprintf("./shared_files/%s/owned/%s", sp.Config.ID, msg.FileName)
    fileData, err := ioutil.ReadFile(filePath)
    if err != nil {
        log.Printf("Super-Peer %s: Error reading file '%s' for refresh: %v", sp.Config.ID, msg.FileName, err)
        // Optionally, send an error response or handle accordingly
        return
    }

    // Send RefreshResponseMessage with the latest file data
    refreshResp := RefreshResponseMessage{
        MessageType:    MsgTypeRefreshResponse,
        MessageID:      msg.MessageID,
        FileName:       msg.FileName,
        FileData:       fileData,
        VersionNumber:  fileMeta.VersionNumber,
        LastModified:   fileMeta.LastModified,
    }
    err = sendJSONMessage(conn, refreshResp)
    if err != nil {
        log.Printf("Super-Peer %s: Error sending RefreshResponse for '%s' to LeafNode: %v", sp.Config.ID, msg.FileName, err)
        return
    }

    log.Printf("Super-Peer %s: Sent RefreshResponse for '%s' to LeafNode", sp.Config.ID, msg.FileName)
}

// simulateFileModification simulates the modification of a file and broadcasts an invalidation
func (sp *SuperPeer) simulateFileModification(fileName string) {
    sp.mu.Lock()
    defer sp.mu.Unlock()

    // Update the file's version number and last modified time
    fileMeta, exists := sp.FileMetadata[fileName]
    if !exists {
        // Initialize metadata if not present
        fileMeta = FileMetadataExtended{
            FileName:         fileName,
            VersionNumber:    1,
            OriginServerID:   sp.Config.ID,
            ConsistencyState: "valid",
            LastModified:     time.Now().Format(time.RFC3339),
        }
    } else {
        fileMeta.VersionNumber += 1 // Increment version number
        fileMeta.LastModified = time.Now().Format(time.RFC3339)
    }
    sp.FileMetadata[fileName] = fileMeta

    // Optionally, modify the file on disk
    filePath := fmt.Sprintf("./shared_files/%s/owned/%s", sp.Config.ID, fileName)
    newContent := fmt.Sprintf("This is the updated content of %s at version %d.", fileName, fileMeta.VersionNumber)
    err := ioutil.WriteFile(filePath, []byte(newContent), 0644)
    if err != nil {
        log.Printf("Super-Peer %s: Error modifying file '%s': %v", sp.Config.ID, fileName, err)
        return
    }

    // Broadcast invalidation if Push-Based is enabled
    if sp.Config.EnablePush {
        sp.broadcastInvalidation(fileName, fileMeta.VersionNumber)
    }

    log.Printf("Super-Peer %s: Simulated modification of file '%s' to version %d", sp.Config.ID, fileName, fileMeta.VersionNumber)
}

// broadcastInvalidation broadcasts an InvalidationMessage to all connected peers
func (sp *SuperPeer) broadcastInvalidation(fileName string, versionNumber int) {
    msgID := uuid.New().String()
    invMsg := InvalidationMessage{
        MessageType:    MsgTypeInvalidation,
        MsgID:          msgID,
        OriginServerID: sp.Config.ID,
        FileName:       fileName,
        VersionNumber:  versionNumber,
    }

    // Send to all connected LeafNodes
    for leafNodeID, conn := range sp.LeafNodeConns {
        err := sendJSONMessage(conn, invMsg)
        if err != nil {
            log.Printf("Super-Peer %s: Error sending INVALIDATION to LeafNode %s: %v", sp.Config.ID, leafNodeID, err)
            continue
        }
        log.Printf("Super-Peer %s: Sent INVALIDATION for '%s' to LeafNode %s", sp.Config.ID, fileName, leafNodeID)
    }

    // Send to all connected SuperPeers
    for neighborID, neighborConn := range sp.NeighborConns {
        err := sendJSONMessage(neighborConn, invMsg)
        if err != nil {
            log.Printf("Super-Peer %s: Error sending INVALIDATION to SuperPeer %s: %v", sp.Config.ID, neighborID, err)
            continue
        }
        log.Printf("Super-Peer %s: Sent INVALIDATION for '%s' to SuperPeer %s", sp.Config.ID, fileName, neighborID)
    }

    // Add to MessageCache to prevent re-processing
    sp.MessageCache[msgID] = CacheEntry{
        OriginID:     sp.Config.ID,
        UpstreamConn: nil, // Origin server doesn't have an upstream connection
        Timestamp:    time.Now(),
    }
}

// getPortOffset returns a port offset based on the SuperPeer ID
func getPortOffset(peerID string) int {
    // Assuming peerID is in the format "SPX" where X is a number from 1 to 10
    var portOffset int
    n, err := fmt.Sscanf(peerID, "SP%d", &portOffset)
    if err != nil || n != 1 {
        log.Printf("Super-Peer %s: Invalid SuperPeer ID format '%s'. Defaulting to port 8001.", peerID, peerID)
        return 8001 // Default port if parsing fails
    }
    return 8000 + portOffset
}
