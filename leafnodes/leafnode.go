// leafnode.go
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
  "github.com/google/uuid"
	"encoding/json"
)

// LeafNodeConfig represents the configuration for a LeafNode
type LeafNodeConfig struct {
	ID          string   `json:"id"`
	Address     string   `json:"address"`
	Port        int      `json:"port"`
	SuperPeer   string   `json:"super_peer"`   // ID of the connected SuperPeer
	OriginFiles []string `json:"origin_files"` // Files owned by this LeafNode (Origin Server)
}

// CachedFile represents metadata for a cached file
type CachedFile struct {
	FileName       string
	VersionNumber  int
	OriginServerID string
	Consistency    string // "valid" or "invalid"
	LastModified   time.Time
	TTR            time.Duration // For Pull-Based Consistency
}

// LeafNode represents a LeafNode in the network
type LeafNode struct {
	Config        LeafNodeConfig
	SuperPeerConn net.Conn              // Connection to SuperPeer
	cachedFiles   map[string]*CachedFile
	mu            sync.RWMutex          // Mutex to protect cachedFiles
	quitChan      chan bool             // Channel to signal shutdown
}

// NewLeafNode initializes a new LeafNode instance
func NewLeafNode(config LeafNodeConfig) *LeafNode {
	return &LeafNode{
		Config:      config,
		cachedFiles: make(map[string]*CachedFile),
		quitChan:    make(chan bool),
	}
}

// connectToSuperPeer establishes a TCP connection to the designated SuperPeer
func (ln *LeafNode) connectToSuperPeer(superPeerAddress string) error {
	conn, err := net.Dial("tcp", superPeerAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to SuperPeer at %s: %v", superPeerAddress, err)
	}
	ln.SuperPeerConn = conn
	log.Printf("Leaf-Node %s: Connected to SuperPeer at %s", ln.Config.ID, superPeerAddress)

	// Send PeerIDMessage to identify itself
	peerIDMsg := PeerIDMessage{
		MessageType: MsgTypePeerID,
		PeerType:    LeafNodeType,
		PeerID:      ln.Config.ID,
	}
	err = sendJSONMessage(conn, peerIDMsg)
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to send PeerIDMessage: %v", err)
	}

	// Register owned files
	err = ln.registerFiles()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to register files: %v", err)
	}

	// Start listening for incoming messages from SuperPeer
	go ln.listenToSuperPeer()

	return nil
}

// registerFiles sends a FileRegistrationMessage to the SuperPeer to register owned files
func (ln *LeafNode) registerFiles() error {
	var files []FileMetadata
	for _, fileName := range ln.Config.OriginFiles {
		filePath := filepath.Join("./shared_files", ln.Config.ID, "owned", fileName)
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			log.Printf("Leaf-Node %s: File '%s' does not exist. Skipping registration.", ln.Config.ID, fileName)
			continue
		}
		files = append(files, FileMetadata{
			FileName: fileName,
			FileSize: fileInfo.Size(),
		})

		// Initialize cachedFiles entry for owned files
		ln.mu.Lock()
		ln.cachedFiles[fileName] = &CachedFile{
			FileName:        fileName,
			VersionNumber:   1, // Initial version
			OriginServerID:  ln.Config.ID,
			Consistency:     "valid",
			LastModified:    fileInfo.ModTime(),
			TTR:             5 * time.Minute, // Example TTR value
		}
		ln.mu.Unlock()
	}

	regMsg := FileRegistrationMessage{
		MessageType: MsgTypeFileRegistration,
		LeafNodeID:  ln.Config.ID,
		Files:       files,
	}

	err := sendJSONMessage(ln.SuperPeerConn, regMsg)
	if err != nil {
		return fmt.Errorf("failed to send FileRegistrationMessage: %v", err)
	}
	log.Printf("Leaf-Node %s: Registered %d files with SuperPeer", ln.Config.ID, len(files))
	return nil
}

// listenToSuperPeer listens for incoming messages from the SuperPeer
func (ln *LeafNode) listenToSuperPeer() {
	decoder := json.NewDecoder(ln.SuperPeerConn)
	for {
		var msg map[string]interface{}
		err := decoder.Decode(&msg)
		if err != nil {
			log.Printf("Leaf-Node %s: Connection to SuperPeer lost: %v", ln.Config.ID, err)
			close(ln.quitChan)
			return
		}

		messageType, ok := msg["message_type"].(string)
		if !ok {
			log.Printf("Leaf-Node %s: Received message without message_type", ln.Config.ID)
			continue
		}

		switch messageType {
		case MsgTypeInvalidation:
			var invMsg InvalidationMessage
			err := mapToStruct(msg, &invMsg)
			if err != nil {
				log.Printf("Leaf-Node %s: Error parsing InvalidationMessage: %v", ln.Config.ID, err)
				continue
			}
			ln.handleInvalidation(invMsg)
		// Handle other message types as needed
		default:
			log.Printf("Leaf-Node %s: Received unknown message_type '%s'", ln.Config.ID, messageType)
		}
	}
}

// handleInvalidation processes an InvalidationMessage
func (ln *LeafNode) handleInvalidation(msg InvalidationMessage) {
	log.Printf("Leaf-Node %s: Received INVALIDATION for '%s' (version %d) from Origin Server %s", ln.Config.ID, msg.FileName, msg.VersionNumber, msg.OriginServerID)

	ln.mu.Lock()
	defer ln.mu.Unlock()

	cachedFile, exists := ln.cachedFiles[msg.FileName]
	if exists && cachedFile.OriginServerID == msg.OriginServerID {
		// Mark the file as invalid
		cachedFile.Consistency = "invalid"
		log.Printf("Leaf-Node %s: Marked '%s' as INVALID", ln.Config.ID, msg.FileName)

		// Optionally, remove the file from cache
		// ln.removeCachedFile(msg.FileName)

		// Notify the user
		fmt.Printf("\n*** ALERT: The file '%s' has been invalidated and marked as INVALID in your cache. ***\n", msg.FileName)
	} else {
		log.Printf("Leaf-Node %s: No cached copy of '%s' found or OriginServerID mismatch", ln.Config.ID, msg.FileName)
	}
}

// removeCachedFile removes a cached file from the local storage
func (ln *LeafNode) removeCachedFile(fileName string) {
	filePath := filepath.Join("./shared_files", ln.Config.ID, "cached", fileName)
	err := os.Remove(filePath)
	if err != nil {
		log.Printf("Leaf-Node %s: Failed to remove cached file '%s': %v", ln.Config.ID, fileName, err)
		return
	}
	delete(ln.cachedFiles, fileName)
	log.Printf("Leaf-Node %s: Removed cached file '%s'", ln.Config.ID, fileName)

	// Notify the user
	fmt.Printf("\n*** ALERT: The file '%s' has been removed from your cache. ***\n", fileName)
}

// modifyFile simulates modifying a file and broadcasts an INVALIDATION message
func (ln *LeafNode) modifyFile(fileName string, newVersion int) {
	ln.mu.Lock()
	defer ln.mu.Unlock()

	// Ensure the file is owned by this LeafNode
	if !contains(ln.Config.OriginFiles, fileName) {
		log.Printf("Leaf-Node %s: Cannot modify file '%s' as it is not owned.", ln.Config.ID, fileName)
		return
	}

	// Path to the owned file
	ownedDir := filepath.Join("./shared_files", ln.Config.ID, "owned")
	filePath := filepath.Join(ownedDir, fileName)

	// Check if the file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Printf("Leaf-Node %s: File '%s' does not exist. Cannot modify.", ln.Config.ID, fileName)
		return
	}

	// Simulate file modification by updating the file's last modified time
	currentTime := time.Now()
	err := os.Chtimes(filePath, currentTime, currentTime)
	if err != nil {
		log.Printf("Leaf-Node %s: Failed to modify file '%s': %v", ln.Config.ID, fileName, err)
		return
	}

	// Update cachedFiles entry for owned file
	cachedFile, exists := ln.cachedFiles[fileName]
	if exists {
		cachedFile.VersionNumber = newVersion
		cachedFile.Consistency = "valid"
		cachedFile.LastModified = currentTime
	}

	// Create the INVALIDATION message
	invalidationMsg := InvalidationMessage{
		MessageType:    MsgTypeInvalidation,
		MsgID:          uuid.New().String(),
		OriginServerID: ln.Config.ID,
		FileName:       fileName,
		VersionNumber:  newVersion,
	}

	// Broadcast the INVALIDATION message to the connected SuperPeer
	err = sendJSONMessage(ln.SuperPeerConn, invalidationMsg)
	if err != nil {
		log.Printf("Leaf-Node %s: Error sending INVALIDATION to SuperPeer: %v", ln.Config.ID, err)
		return
	}
	log.Printf("Leaf-Node %s: Sent INVALIDATION for '%s' to SuperPeer", ln.Config.ID, fileName)

	// Notify the user
	fmt.Printf("\n*** ALERT: You have modified '%s' to version %d. INVALIDATION message sent. ***\n", fileName, newVersion)
}

// startCLI starts a simple command-line interface for user interaction
func (ln *LeafNode) startCLI() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Enter command (modify [filename] [newVersion] or exit): ")
		if !scanner.Scan() {
			break
		}
		input := scanner.Text()
		if input == "exit" {
			fmt.Println("Exiting LeafNode...")
			ln.SuperPeerConn.Close()
			os.Exit(0)
		}

		parts := strings.Split(input, " ")
		if len(parts) == 3 && parts[0] == "modify" {
			fileName := parts[1]
			newVersion, err := strconv.Atoi(parts[2])
			if err != nil {
				fmt.Println("Invalid version number. Please enter an integer.")
				continue
			}
			ln.modifyFile(fileName, newVersion)
		} else {
			fmt.Println("Invalid command. Use 'modify [filename] [newVersion]' or 'exit'.")
		}
	}
}

// listen handles incoming messages from the SuperPeer
func (ln *LeafNode) listen() {
	// For Push-Based Consistency, all messages are handled in listenToSuperPeer
	// Additional listening can be implemented here for other functionalities
}

// Helper function to check if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
