// leafnode.go
package main

import (
    "bufio"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "net"
    "os"
    "strings"
    "sync"
    "time"

    "github.com/google/uuid" // For generating unique message IDs
)

// LeafNodeConfig represents the configuration for a LeafNode
type LeafNodeConfig struct {
    ID          string `json:"id"`
    Address     string `json:"address"`
    Port        int    `json:"port"`
    SuperPeerID string `json:"super_peer"`
    EnablePush  bool   `json:"enable_push"`
    EnablePull  bool   `json:"enable_pull"`
    TTR         int    `json:"TTR"` // Time-To-Refresh in seconds
}

// FileMetadataClient stores metadata for each downloaded file
type FileMetadataClient struct {
    VersionNumber    int    `json:"version_number"`
    OriginServerID   string `json:"origin_server_id"`
    ConsistencyState string `json:"consistency_state"` // "valid" or "invalid"
    LastModified     string `json:"last_modified_time"`
}

// LeafNode represents a LeafNode in the network
type LeafNode struct {
    Config        LeafNodeConfig
    SuperPeerConn net.Conn
    Files         map[string]FileMetadataClient // FileName -> Metadata
    mu            sync.RWMutex                  // Mutex to protect shared resources
}

// NewLeafNode initializes a new LeafNode instance
func NewLeafNode(config LeafNodeConfig) *LeafNode {
    return &LeafNode{
        Config: config,
        Files:  make(map[string]FileMetadataClient),
    }
}

// Start launches the LeafNode's client to connect to the SuperPeer and handle messages
func (ln *LeafNode) Start(wg *sync.WaitGroup) {
    defer wg.Done()

    // Connect to SuperPeer
    spAddress := fmt.Sprintf("127.0.0.1:%d", getSuperPeerPort(ln.Config.SuperPeerID))
    conn, err := net.Dial("tcp", spAddress)
    if err != nil {
        log.Fatalf("Leaf-Node %s: Failed to connect to SuperPeer %s at %s: %v", ln.Config.ID, ln.Config.SuperPeerID, spAddress, err)
    }
    ln.SuperPeerConn = conn
    log.Printf("Leaf-Node %s: Connected to SuperPeer %s at %s", ln.Config.ID, ln.Config.SuperPeerID, spAddress)

    // Send PeerIDMessage upon connection
    peerIDMsg := PeerIDMessage{
        MessageType: MsgTypePeerID,
        PeerType:    LeafNodeType,
        PeerID:      ln.Config.ID,
    }
    err = sendJSONMessage(conn, peerIDMsg)
    if err != nil {
        log.Printf("Leaf-Node %s: Error sending PeerIDMessage: %v", ln.Config.ID, err)
    } else {
        log.Printf("Leaf-Node %s: Sent PeerIDMessage to SuperPeer", ln.Config.ID)
    }

    // Register files with the SuperPeer
    ln.registerFiles()

    // Start listening for messages from SuperPeer
    go ln.listenToSuperPeer(conn)

    // Start polling mechanism if Pull-Based consistency is enabled
    if ln.Config.EnablePull {
        go ln.startPolling()
    }

    // Command interface for refreshing files
    go func() {
        scanner := bufio.NewScanner(os.Stdin)
        fmt.Println("Enter 'refresh <filename>' to manually refresh an outdated file:")
        for scanner.Scan() {
            input := scanner.Text()
            parts := strings.Split(input, " ")
            if len(parts) != 2 || parts[0] != "refresh" {
                fmt.Println("Invalid command. Usage: refresh <filename>")
                continue
            }
            fileName := parts[1]
            ln.RefreshFile(fileName)
        }
    }()

    // Keep the main goroutine alive
    select {}
}

// registerFiles registers owned files with the SuperPeer
func (ln *LeafNode) registerFiles() {
    ln.mu.Lock()
    defer ln.mu.Unlock()

    // Read files from the owned directory
    ownedDir := fmt.Sprintf("../leafnode_shared/%s/owned", ln.Config.ID)
    files, err := ioutil.ReadDir(ownedDir)
    if err != nil {
        log.Printf("Leaf-Node %s: Error reading owned directory: %v", ln.Config.ID, err)
        return
    }

    var fileList []FileMetadata
    for _, file := range files {
        if !file.IsDir() {
            fileMeta := FileMetadata{
                FileName: file.Name(),
                FileSize: file.Size(),
            }
            fileList = append(fileList, fileMeta)

            // Initialize file metadata in ln.Files
            ln.Files[file.Name()] = FileMetadataClient{
                VersionNumber:    1,
                OriginServerID:   ln.Config.SuperPeerID,
                ConsistencyState: "valid",
                LastModified:     file.ModTime().Format(time.RFC3339),
            }
        }
    }

    // Send FileRegistrationMessage to SuperPeer
    regMsg := FileRegistrationMessage{
        MessageType: MsgTypeFileRegistration,
        LeafNodeID:  ln.Config.ID,
        Files:       fileList,
    }
    err = sendJSONMessage(ln.SuperPeerConn, regMsg)
    if err != nil {
        log.Printf("Leaf-Node %s: Error sending FileRegistrationMessage: %v", ln.Config.ID, err)
    } else {
        log.Printf("Leaf-Node %s: Registered %d files with SuperPeer", ln.Config.ID, len(fileList))
    }
}

// listenToSuperPeer continuously listens for messages from the connected SuperPeer
func (ln *LeafNode) listenToSuperPeer(conn net.Conn) {
    decoder := json.NewDecoder(conn)
    for {
        var msg map[string]interface{}
        err := decoder.Decode(&msg)
        if err != nil {
            log.Printf("Leaf-Node %s: Connection to SuperPeer lost: %v", ln.Config.ID, err)
            return
        }

        messageType, ok := msg["message_type"].(string)
        if !ok {
            log.Printf("Leaf-Node %s: Received message without message_type from SuperPeer", ln.Config.ID)
            continue
        }

        // Log the received message_type for debugging
        log.Printf("Leaf-Node %s: Received message_type '%s' from SuperPeer", ln.Config.ID, messageType)

        switch messageType {
        case MsgTypePeerID:
            var peerMsg PeerIDMessage
            err := mapToStruct(msg, &peerMsg)
            if err != nil {
                log.Printf("Leaf-Node %s: Error parsing PeerIDMessage: %v", ln.Config.ID, err)
                continue
            }
            ln.handlePeerIDMessage(peerMsg)
        case MsgTypeInvalidation:
            var invMsg InvalidationMessage
            err := mapToStruct(msg, &invMsg)
            if err != nil {
                log.Printf("Leaf-Node %s: Error parsing InvalidationMessage: %v", ln.Config.ID, err)
                continue
            }
            if ln.Config.EnablePush {
                ln.handleInvalidation(invMsg)
            }
        case MsgTypePollResponse:
            var pollResp PollResponseMessage
            err := mapToStruct(msg, &pollResp)
            if err != nil {
                log.Printf("Leaf-Node %s: Error parsing PollResponseMessage: %v", ln.Config.ID, err)
                continue
            }
            if ln.Config.EnablePull {
                ln.handlePollResponse(pollResp)
            }
        case MsgTypeRefreshResponse:
            var refreshResp RefreshResponseMessage
            err := mapToStruct(msg, &refreshResp)
            if err != nil {
                log.Printf("Leaf-Node %s: Error parsing RefreshResponseMessage: %v", ln.Config.ID, err)
                continue
            }
            ln.handleRefreshResponse(refreshResp)
        // Handle other message types as needed
        default:
            log.Printf("Leaf-Node %s: Unknown message_type '%s' from SuperPeer", ln.Config.ID, messageType)
        }
    }
}

// handlePeerIDMessage processes a PeerIDMessage from SuperPeer
func (ln *LeafNode) handlePeerIDMessage(msg PeerIDMessage) {
    log.Printf("Leaf-Node %s: Received PeerIDMessage from SuperPeer %s", ln.Config.ID, msg.PeerID)
    // Additional processing can be done here if needed
}

// handleInvalidation processes an InvalidationMessage from the SuperPeer
func (ln *LeafNode) handleInvalidation(msg InvalidationMessage) {
    ln.mu.Lock()
    defer ln.mu.Unlock()

    // Check if the LeafNode has a cached copy of the file
    fileMeta, exists := ln.Files[msg.FileName]
    if exists {
        // Mark the file as invalid
        fileMeta.ConsistencyState = "invalid"
        ln.Files[msg.FileName] = fileMeta

        log.Printf("Leaf-Node %s: Received INVALIDATION for '%s' from Origin Server %s (Version: %d)", ln.Config.ID, msg.FileName, msg.OriginServerID, msg.VersionNumber)

        // Notify the user
        fmt.Printf("*** ALERT: The file '%s' has been invalidated and is marked as INVALID in your cache. ***\n", msg.FileName)
    }
}

// startPolling initiates the periodic polling mechanism based on TTR
func (ln *LeafNode) startPolling() {
    ticker := time.NewTicker(time.Duration(ln.Config.TTR) * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            ln.pollOriginServer()
        }
    }
}

// pollOriginServer polls the origin server to check the validity of cached files
func (ln *LeafNode) pollOriginServer() {
    ln.mu.RLock()
    defer ln.mu.RUnlock()

    for fileName, meta := range ln.Files {
        if meta.ConsistencyState == "invalid" {
            continue // Skip already invalid files
        }

        // Generate a unique MessageID for the PollRequest
        messageID := uuid.New().String()

        // Send PollRequestMessage
        pollReq := PollRequestMessage{
            MessageType:          MsgTypePollRequest,
            MessageID:            messageID, // Include MessageID
            FileName:             fileName,
            CurrentVersionNumber: meta.VersionNumber,
        }
        err := sendJSONMessage(ln.SuperPeerConn, pollReq)
        if err != nil {
            log.Printf("Leaf-Node %s: Error sending PollRequest for '%s': %v", ln.Config.ID, fileName, err)
            continue
        }
        log.Printf("Leaf-Node %s: Sent PollRequest for '%s' (MessageID: %s)", ln.Config.ID, fileName, messageID)
    }
}

// handlePollResponse processes a PollResponseMessage from the SuperPeer
func (ln *LeafNode) handlePollResponse(msg PollResponseMessage) {
    ln.mu.Lock()
    defer ln.mu.Unlock()

    fileMeta, exists := ln.Files[msg.FileName]
    if !exists {
        log.Printf("Leaf-Node %s: Received PollResponse for unknown file '%s'", ln.Config.ID, msg.FileName)
        return
    }

    if msg.Status == "invalid" && msg.NewVersionNumber > fileMeta.VersionNumber {
        // Update the file's version and mark as invalid
        fileMeta.VersionNumber = msg.NewVersionNumber
        fileMeta.ConsistencyState = "invalid"
        ln.Files[msg.FileName] = fileMeta

        log.Printf("Leaf-Node %s: Received PollResponse. File '%s' is outdated (New Version: %d)", ln.Config.ID, msg.FileName, msg.NewVersionNumber)

        // Notify the user
        fmt.Printf("*** ALERT: The file '%s' is outdated and has been marked as INVALID. ***\n", msg.FileName)
    } else if msg.Status == "valid" {
        // Update TTR or perform any necessary actions
        log.Printf("Leaf-Node %s: Received PollResponse. File '%s' is still valid.", ln.Config.ID, msg.FileName)
    }
}

// RefreshFile allows the user to manually refresh an outdated file
func (ln *LeafNode) RefreshFile(fileName string) {
    ln.mu.RLock()
    meta, exists := ln.Files[fileName]
    ln.mu.RUnlock()

    if !exists {
        fmt.Printf("Leaf-Node %s: No such file '%s' to refresh.\n", ln.Config.ID, fileName)
        return
    }

    if meta.ConsistencyState != "invalid" {
        fmt.Printf("Leaf-Node %s: File '%s' is not outdated and does not need refresh.\n", ln.Config.ID, fileName)
        return
    }

    // Generate a unique MessageID for the RefreshRequest
    messageID := uuid.New().String()

    // Send RefreshRequestMessage
    refreshReq := RefreshRequestMessage{
        MessageType: MsgTypeRefreshRequest,
        MessageID:   messageID, // Include MessageID
        FileName:    fileName,
    }
    err := sendJSONMessage(ln.SuperPeerConn, refreshReq)
    if err != nil {
        log.Printf("Leaf-Node %s: Error sending RefreshRequest for '%s': %v", ln.Config.ID, fileName, err)
        return
    }
    log.Printf("Leaf-Node %s: Sent RefreshRequest for '%s' (MessageID: %s)", ln.Config.ID, fileName, messageID)
}

// handleRefreshResponse processes a RefreshResponseMessage from the SuperPeer
func (ln *LeafNode) handleRefreshResponse(msg RefreshResponseMessage) {
    ln.mu.Lock()
    defer ln.mu.Unlock()

    // Verify that the file exists in the cache
    fileMeta, exists := ln.Files[msg.FileName]
    if !exists {
        log.Printf("Leaf-Node %s: Received RefreshResponse for unknown file '%s'", ln.Config.ID, msg.FileName)
        return
    }

    if msg.VersionNumber == -1 {
        // SuperPeer indicates the file does not exist
        log.Printf("Leaf-Node %s: SuperPeer indicates that file '%s' does not exist.", ln.Config.ID, msg.FileName)
        fmt.Printf("*** ERROR: The file '%s' does not exist on the SuperPeer. ***\n", msg.FileName)
        return
    }

    // Update the file with new data
    filePath := fmt.Sprintf("../leafnode_shared/%s/cached/%s", ln.Config.ID, msg.FileName)
    err := ioutil.WriteFile(filePath, msg.FileData, 0644)
    if err != nil {
        log.Printf("Leaf-Node %s: Error writing refreshed file '%s': %v", ln.Config.ID, msg.FileName, err)
        return
    }

    // Update metadata
    fileMeta.VersionNumber = msg.VersionNumber
    fileMeta.LastModified = msg.LastModified
    fileMeta.ConsistencyState = "valid"

    ln.Files[msg.FileName] = fileMeta

    log.Printf("Leaf-Node %s: Successfully refreshed file '%s' to version %d", ln.Config.ID, msg.FileName, msg.VersionNumber)
    fmt.Printf("*** INFO: The file '%s' has been refreshed and is now valid. ***\n", msg.FileName)
}


// getSuperPeerPort returns the port number for a given SuperPeer ID
func getSuperPeerPort(peerID string) int {
    // Assuming peerID is in the format "SPX" where X is a number from 1 to 10
    var portOffset int
    n, err := fmt.Sscanf(peerID, "SP%d", &portOffset)
    if err != nil || n != 1 {
        log.Printf("Leaf-Node %s: Invalid SuperPeer ID format '%s'. Defaulting to port 8001.", peerID, peerID)
        return 8001 // Default port if parsing fails
    }
    return 8000 + portOffset
}