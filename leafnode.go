// leafnode.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/google/uuid" // Ensure this package is installed: go get github.com/google/uuid
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// LeafNode represents a leaf-node in the network
type LeafNode struct {
	Config             LeafNodeConfig
	SuperPeerConfig    SuperPeerConfig
	conn               net.Conn
	connMu             sync.Mutex // Mutex to protect writes to connection
	mu                 sync.Mutex
	responseTimes      []time.Duration
	startTimes         map[string]time.Time
	downloadPromptChan chan DownloadPrompt
	cachedFiles        map[string]*CachedFile // Initialized later
}

// CachedFile contains metadata about a cached file
type CachedFile struct {
	FileName        string           `json:"file_name"`
	VersionNumber   int              `json:"version_number"`
	OriginServerID  string           `json:"origin_server_id"`
	Consistency     ConsistencyState `json:"consistency_state"`
	LastModified    time.Time        `json:"last_modified_time"`
	TTR             time.Duration    `json:"ttr"` // For pull-based approach later
}

// ConsistencyState represents the state of a cached file
type ConsistencyState string

const (
	ConsistencyValid   ConsistencyState = "valid"
	ConsistencyInvalid ConsistencyState = "invalid"
)

// DownloadPrompt is used to prompt the user for downloading a file
type DownloadPrompt struct {
	QueryHitMessage QueryHitMessage
	ResponseChan    chan bool
}

// NewLeafNode creates a new LeafNode instance
func NewLeafNode(config LeafNodeConfig, globalConfig *Config) *LeafNode {
	var superPeerConfig SuperPeerConfig
	found := false
	for _, spConfig := range globalConfig.SuperPeers {
		if spConfig.ID == config.SuperPeer {
			superPeerConfig = spConfig
			found = true
			break
		}
	}
	if !found {
		log.Fatalf("Super-Peer ID %s for Leaf-Node %s not found in configuration", config.SuperPeer, config.ID)
	}

	return &LeafNode{
		Config:             config,
		SuperPeerConfig:    superPeerConfig,
		responseTimes:      []time.Duration{},
		startTimes:         make(map[string]time.Time),
		downloadPromptChan: make(chan DownloadPrompt),
		cachedFiles:        make(map[string]*CachedFile), // Initialize the cachedFiles map
	}
}

// Start initializes the leaf-node and connects to its super-peer
func (ln *LeafNode) Start() {
	// Specify the shared directory
	sharedDir := "./shared_files/" + ln.Config.ID

	// Discover shared files
	files, err := ln.discoverFiles(sharedDir)
	if err != nil {
		log.Fatalf("Leaf-Node %s failed to discover files: %v", ln.Config.ID, err)
	}

	log.Printf("Leaf-Node %s discovered %d files", ln.Config.ID, len(files))

	// Start the file server
	ln.startFileServer()

	// Connect to the super-peer with retry logic
	address := fmt.Sprintf("%s:%d", ln.SuperPeerConfig.Address, ln.SuperPeerConfig.Port)
	var conn net.Conn
	for {
		conn, err = net.Dial("tcp", address)
		if err != nil {
			log.Printf("Leaf-Node %s failed to connect to Super-Peer at %s: %v", ln.Config.ID, address, err)
			time.Sleep(5 * time.Second) // Retry after delay
			continue
		}
		break // Exit the loop upon successful connection
	}

	// Store the connection
	ln.conn = conn

	// Send identification message to the super-peer
	peerIDMsg := PeerIDMessage{
		MessageType: MsgTypePeerID,
		PeerType:    LeafNodeType,
		PeerID:      ln.Config.ID,
	}
	err = ln.sendJSONMessage(peerIDMsg)
	if err != nil {
		log.Fatalf("Leaf-Node %s failed to send ID to Super-Peer: %v", ln.Config.ID, err)
	}

	log.Printf("Leaf-Node %s connected to Super-Peer at %s", ln.Config.ID, address)

	// Send file registration message
	registrationMsg := FileRegistrationMessage{
		MessageType: MsgTypeFileRegistration,
		LeafNodeID:  ln.Config.ID,
		Files:       files,
	}
	err = ln.sendJSONMessage(registrationMsg)
	if err != nil {
		log.Fatalf("Leaf-Node %s failed to send file registration to Super-Peer: %v", ln.Config.ID, err)
	}

	// Handle communication with the super-peer
	go ln.handleSuperPeerConnection(conn)

	// Start the user interface
	ln.startUserInterface()
}

// discoverFiles scans the shared directory and returns metadata of shared files
func (ln *LeafNode) discoverFiles(sharedDir string) ([]FileMetadata, error) {
	var files []FileMetadata

	err := filepath.Walk(sharedDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, FileMetadata{
				FileName: info.Name(),
				FileSize: info.Size(),
			})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

// startFileServer starts an HTTP server to serve shared files
func (ln *LeafNode) startFileServer() {
	sharedDir := "./shared_files/" + ln.Config.ID
	fs := http.FileServer(http.Dir(sharedDir))
	http.Handle("/", fs)

	address := fmt.Sprintf("%s:%d", ln.Config.Address, ln.Config.Port)
	log.Printf("Leaf-Node %s starting file server at %s", ln.Config.ID, address)
	go func() {
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Fatalf("Leaf-Node %s file server error: %v", ln.Config.ID, err)
		}
	}()
}

// handleSuperPeerConnection manages incoming messages from the super-peer
func (ln *LeafNode) handleSuperPeerConnection(conn net.Conn) {
	decoder := json.NewDecoder(conn)
	for {
		var msg map[string]interface{}
		err := decoder.Decode(&msg)
		if err != nil {
			if err == io.EOF {
				log.Printf("Super-Peer disconnected")
			} else {
				log.Printf("Error decoding message from Super-Peer: %v", err)
			}
			break
		}

		messageType, ok := msg["message_type"].(string)
		if !ok {
			log.Printf("Invalid message from Super-Peer: missing message_type")
			continue
		}

		switch messageType {
		case MsgTypeQueryHit:
			// Decode the message as QueryHitMessage
			var queryHitMsg QueryHitMessage
			err := mapToStruct(msg, &queryHitMsg)
			if err != nil {
				log.Printf("Error decoding QueryHitMessage: %v", err)
				continue
			}
			go ln.handleQueryHit(queryHitMsg)
		case MsgTypeInvalidation:
			var invalidationMsg InvalidationMessage
			err := mapToStruct(msg, &invalidationMsg)
			if err != nil {
				log.Printf("Error decoding InvalidationMessage: %v", err)
				continue
			}
			go ln.handleInvalidation(invalidationMsg)
		default:
			log.Printf("Unknown message type '%s' from Super-Peer", messageType)
		}
	}
}

// handleInvalidation processes an INVALIDATION message from the super-peer
func (ln *LeafNode) handleInvalidation(msg InvalidationMessage) {
	// Check if the file exists in the cachedFiles map
	ln.mu.Lock()
	cachedFile, exists := ln.cachedFiles[msg.FileName]
	if !exists {
		ln.mu.Unlock()
		log.Printf("Leaf-Node %s: Received INVALIDATION for unknown file '%s'. Ignoring.", ln.Config.ID, msg.FileName)
		return
	}

	// Update the Consistency state to INVALID
	cachedFile.Consistency = ConsistencyInvalid
	ln.mu.Unlock()

	// Optionally, remove the file from the local cache
	sharedDir := "./shared_files/" + ln.Config.ID
	filePath := filepath.Join(sharedDir, msg.FileName)

	// Remove the file to invalidate the cached copy
	err := os.Remove(filePath)
	if err != nil {
		log.Printf("Leaf-Node %s: Failed to remove file '%s': %v", ln.Config.ID, msg.FileName, err)
		return
	}

	log.Printf("Leaf-Node %s: Invalidated and removed file '%s' due to INVALIDATION from Super-Peer %s", ln.Config.ID, msg.FileName, msg.OriginServerID)

	// Inform the user about the invalidation
	fmt.Printf("\n*** ALERT: The file '%s' has been invalidated and removed from your cache. ***\n", msg.FileName)
}

// startUserInterface handles user inputs and download prompts
func (ln *LeafNode) startUserInterface() {
	reader := bufio.NewReader(os.Stdin)
	for {
		select {
		case prompt := <-ln.downloadPromptChan:
			// Handle download prompt
			fmt.Printf("\nQuery Hit: File '%s' is available at Leaf-Node %s (%s:%d)\n",
				prompt.QueryHitMessage.FileName, prompt.QueryHitMessage.RespondingID,
				prompt.QueryHitMessage.Address, prompt.QueryHitMessage.Port)
			fmt.Printf("Do you want to download this file? (yes/no): ")
			response, _ := reader.ReadString('\n')
			response = strings.TrimSpace(strings.ToLower(response))
			if response == "yes" {
				ln.downloadFile(prompt.QueryHitMessage)
				prompt.ResponseChan <- true
			} else {
				prompt.ResponseChan <- false
			}
		default:
			// Prompt user for file search
			fmt.Printf("\nEnter file name to search (or 'exit' to quit): ")
			fileName, _ := reader.ReadString('\n')
			fileName = strings.TrimSpace(fileName)

			if fileName == "exit" {
				fmt.Println("Exiting...")
				ln.conn.Close()
				os.Exit(0)
			}

			if fileName != "" {
				ln.sendFileQuery(fileName)
			}
		}
	}
}

// sendFileQuery sends a file query to the super-peer
func (ln *LeafNode) sendFileQuery(fileName string) {
	messageID := ln.generateMessageID()
	queryMsg := FileQueryMessage{
		MessageType: MsgTypeFileQuery,
		MessageID:   messageID,
		OriginID:    ln.Config.ID,
		FileName:    fileName,
		TTL:         5, // Set an appropriate TTL value
	}

	// Record the start time
	startTime := time.Now()

	// Store the start time associated with the MessageID
	ln.mu.Lock()
	ln.startTimes[messageID] = startTime
	ln.mu.Unlock()

	err := ln.sendJSONMessage(queryMsg)
	if err != nil {
		log.Printf("Leaf-Node %s failed to send file query: %v", ln.Config.ID, err)
		return
	}

	log.Printf("Leaf-Node %s sent file query for '%s' with MessageID %s", ln.Config.ID, fileName, messageID)

	// Optionally, print the query issued in a nicely formatted manner
	fmt.Printf("Issued Query: Looking for file '%s' with MessageID %s\n", fileName, messageID)
}

// sendJSONMessage sends a JSON-encoded message to the super-peer
func (ln *LeafNode) sendJSONMessage(msg interface{}) error {
	ln.connMu.Lock()
	defer ln.connMu.Unlock()
	encoder := json.NewEncoder(ln.conn)
	return encoder.Encode(msg)
}

// generateMessageID generates a unique MessageID for each query using UUID
func (ln *LeafNode) generateMessageID() string {
	return fmt.Sprintf("%s-%s", ln.Config.ID, uuid.New().String())
}

// handleQueryHit processes a QueryHitMessage
func (ln *LeafNode) handleQueryHit(msg QueryHitMessage) {
	// Record the response time
	endTime := time.Now()

	ln.mu.Lock()
	startTime, exists := ln.startTimes[msg.MessageID]
	if exists {
		responseTime := endTime.Sub(startTime)
		ln.responseTimes = append(ln.responseTimes, responseTime)
		// Remove the startTime as it's no longer needed
		delete(ln.startTimes, msg.MessageID)
		log.Printf("Response time for MessageID %s: %v", msg.MessageID, responseTime)
	}
	ln.mu.Unlock()

	// Prepare to prompt the user
	responseChan := make(chan bool)
	prompt := DownloadPrompt{
		QueryHitMessage: msg,
		ResponseChan:    responseChan,
	}

	// Send the prompt to the main input loop
	ln.downloadPromptChan <- prompt
}

// downloadFile downloads the specified file from the responding leaf-node
func (ln *LeafNode) downloadFile(msg QueryHitMessage) {
	url := fmt.Sprintf("http://%s:%d/%s", msg.Address, msg.Port, msg.FileName)
	log.Printf("Downloading file from %s", url)

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error downloading file: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Failed to download file: %s", resp.Status)
		return
	}

	// Save the file to the local shared directory
	sharedDir := "./shared_files/" + ln.Config.ID
	filePath := filepath.Join(sharedDir, msg.FileName)

	// Check if the file already exists to prevent re-registration
	if _, err := os.Stat(filePath); err == nil {
		log.Printf("File '%s' already exists. Skipping download.", msg.FileName)
		return
	}

	outFile, err := os.Create(filePath)
	if err != nil {
		log.Printf("Error creating file: %v", err)
		return
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		log.Printf("Error saving file: %v", err)
		return
	}

	// Display the downloaded file message as per requirement
	fmt.Printf("display file '%s'\n", msg.FileName)

	log.Printf("File '%s' downloaded successfully", msg.FileName)

	// Re-register the new file only if it's newly downloaded
	newFile := FileMetadata{
		FileName: msg.FileName,
		FileSize: getFileSize(filePath),
	}

	registrationMsg := FileRegistrationMessage{
		MessageType: MsgTypeFileRegistration,
		LeafNodeID:  ln.Config.ID,
		Files:       []FileMetadata{newFile},
	}

	err = ln.sendJSONMessage(registrationMsg)
	if err != nil {
		log.Printf("Error re-registering file: %v", err)
	}

	// Update the cachedFiles map with metadata
	cachedFile := &CachedFile{
		FileName:        msg.FileName,
		VersionNumber:   1, // Initialize to version 1 or fetch from origin server if available
		OriginServerID:  msg.RespondingID,
		Consistency:     ConsistencyValid,
		LastModified:    time.Now(),
		TTR:             5 * time.Minute, // Example TTR value; adjust as needed
	}

	ln.mu.Lock()
	ln.cachedFiles[msg.FileName] = cachedFile
	ln.mu.Unlock()
}

// getFileSize returns the size of the file at the given path
func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
