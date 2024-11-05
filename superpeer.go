// superpeer.go
package main

import (
	"encoding/json"
	"fmt" // Added to resolve undefined: fmt
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// SuperPeer represents a super-peer in the network
type SuperPeer struct {
	Config          SuperPeerConfig
	GlobalConfig    *Config
	NeighborConns   map[string]net.Conn
	NeighborConfigs map[string]SuperPeerConfig
	LeafNodeConns   map[string]net.Conn
	LeafNodeConfigs map[string]LeafNodeConfig
	FileIndex       map[string]map[string]struct{} // FileName -> LeafNodeID set
	MessageCache    map[string]CacheEntry
	mu              sync.Mutex
	connMu          sync.Mutex
}

// CacheEntry stores information about forwarded messages
type CacheEntry struct {
	OriginID     string
	UpstreamConn net.Conn
	Timestamp    time.Time
}

// NewSuperPeer creates a new SuperPeer instance
func NewSuperPeer(config SuperPeerConfig, globalConfig *Config) *SuperPeer {
	neighborConfigs := make(map[string]SuperPeerConfig)
	for _, neighborID := range config.Neighbors {
		for _, spConfig := range globalConfig.SuperPeers {
			if spConfig.ID == neighborID {
				neighborConfigs[neighborID] = spConfig
				break
			}
		}
	}

	leafNodeConfigs := make(map[string]LeafNodeConfig)
	for _, lnID := range config.LeafNodes {
		for _, lnConfig := range globalConfig.LeafNodes {
			if lnConfig.ID == lnID {
				leafNodeConfigs[lnID] = lnConfig
				break
			}
		}
	}

	// Initialize FileIndex with maps to ensure uniqueness
	fileIndex := make(map[string]map[string]struct{})
	return &SuperPeer{
		Config:          config,
		GlobalConfig:    globalConfig,
		NeighborConns:   make(map[string]net.Conn),
		NeighborConfigs: neighborConfigs,
		LeafNodeConns:   make(map[string]net.Conn),
		LeafNodeConfigs: leafNodeConfigs,
		FileIndex:       fileIndex,
		MessageCache:    make(map[string]CacheEntry),
	}
}

// Start initializes the super-peer and begins listening for connections
func (sp *SuperPeer) Start() {
	// Start listening for incoming connections
	address := fmt.Sprintf("%s:%d", sp.Config.Address, sp.Config.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Super-Peer %s failed to listen on %s: %v", sp.Config.ID, address, err)
	}
	log.Printf("Super-Peer %s listening on %s", sp.Config.ID, address)

	// Connect to neighbor super-peers
	for neighborID, neighborConfig := range sp.NeighborConfigs {
		go sp.connectToNeighbor(neighborID, neighborConfig)
	}

	// Start a goroutine to accept incoming connections
	go sp.acceptConnections(listener)

	// Start the message cache cleanup routine
	go sp.cleanupMessageCache()

	// Start logging FileIndex status (optional)
	go sp.logFileIndex()

	// Keep the main function running indefinitely
	select {}
}

// connectToNeighbor establishes a connection to a neighbor super-peer
func (sp *SuperPeer) connectToNeighbor(neighborID string, neighborConfig SuperPeerConfig) {
	address := fmt.Sprintf("%s:%d", neighborConfig.Address, neighborConfig.Port)
	for {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			log.Printf("Super-Peer %s failed to connect to neighbor Super-Peer %s at %s: %v", sp.Config.ID, neighborID, address, err)
			time.Sleep(5 * time.Second) // Retry after delay
			continue
		}

		// Send identification message to the neighbor
		peerIDMsg := PeerIDMessage{
			MessageType: MsgTypePeerID,
			PeerType:    SuperPeerType,
			PeerID:      sp.Config.ID,
		}
		encoder := json.NewEncoder(conn)
		err = encoder.Encode(peerIDMsg)
		if err != nil {
			log.Printf("Super-Peer %s failed to send ID to neighbor Super-Peer %s: %v", sp.Config.ID, neighborID, err)
			conn.Close()
			time.Sleep(5 * time.Second) // Retry after delay
			continue
		}

		// Store the connection
		sp.mu.Lock()
		sp.NeighborConns[neighborID] = conn
		sp.mu.Unlock()

		// Handle communication with the neighbor
		go sp.handleNeighborConnection(conn, neighborID)
		break // Exit the loop upon successful connection
	}
}

// acceptConnections handles incoming connections to the super-peer
func (sp *SuperPeer) acceptConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Super-Peer %s failed to accept connection: %v", sp.Config.ID, err)
			continue
		}

		// Handle the connection in a separate goroutine
		go sp.handleIncomingConnection(conn)
	}
}

// handleIncomingConnection processes an incoming connection (either Super-Peer or Leaf-Node)
func (sp *SuperPeer) handleIncomingConnection(conn net.Conn) {
	decoder := json.NewDecoder(conn)
	var msg PeerIDMessage
	err := decoder.Decode(&msg)
	if err != nil {
		log.Printf("Super-Peer %s failed to decode message from %s: %v",
			sp.Config.ID, conn.RemoteAddr().String(), err)
		conn.Close()
		return
	}

	if msg.MessageType == MsgTypePeerID {
		if msg.PeerType == LeafNodeType {
			log.Printf("Super-Peer %s accepted connection from Leaf-Node %s", sp.Config.ID, msg.PeerID)
			// Handle leaf-node connection
			go sp.handleLeafNodeConnection(conn, msg.PeerID)
		} else if msg.PeerType == SuperPeerType {
			log.Printf("Super-Peer %s accepted connection from Super-Peer %s", sp.Config.ID, msg.PeerID)
			sp.mu.Lock()
			sp.NeighborConns[msg.PeerID] = conn
			sp.mu.Unlock()
			// Handle super-peer connection
			go sp.handleNeighborConnection(conn, msg.PeerID)
		} else {
			log.Printf("Super-Peer %s received unknown peer type from %s", sp.Config.ID, conn.RemoteAddr().String())
			conn.Close()
		}
	} else {
		log.Printf("Super-Peer %s received unknown message type from %s", sp.Config.ID, conn.RemoteAddr().String())
		conn.Close()
	}
}

// handleNeighborConnection manages communication with a neighbor Super-Peer
func (sp *SuperPeer) handleNeighborConnection(conn net.Conn, neighborID string) {
	log.Printf("Super-Peer %s handling neighbor connection with Super-Peer %s", sp.Config.ID, neighborID)

	decoder := json.NewDecoder(conn)
	for {
		var msg map[string]interface{}
		err := decoder.Decode(&msg)
		if err != nil {
			if err == io.EOF {
				log.Printf("Neighbor Super-Peer %s disconnected", neighborID)
			} else {
				log.Printf("Error decoding message from Neighbor Super-Peer %s: %v", neighborID, err)
			}
			break
		}

		messageType, ok := msg["message_type"].(string)
		if !ok {
			log.Printf("Invalid message from Neighbor Super-Peer %s: missing message_type", neighborID)
			continue
		}

		switch messageType {
		case MsgTypeFileQuery:
			var queryMsg FileQueryMessage
			err := mapToStruct(msg, &queryMsg)
			if err != nil {
				log.Printf("Error decoding FileQueryMessage from Neighbor Super-Peer %s: %v", neighborID, err)
				continue
			}
			log.Printf("Super-Peer %s received FileQueryMessage for '%s' from %s", sp.Config.ID, queryMsg.FileName, queryMsg.OriginID)
			sp.handleFileQuery(queryMsg, conn)
		case MsgTypeQueryHit:
			var queryHitMsg QueryHitMessage
			err := mapToStruct(msg, &queryHitMsg)
			if err != nil {
				log.Printf("Error decoding QueryHitMessage from Neighbor Super-Peer %s: %v", neighborID, err)
				continue
			}
			sp.forwardQueryHit(queryHitMsg)
		case MsgTypeInvalidation:
			var invalidationMsg InvalidationMessage
			err := mapToStruct(msg, &invalidationMsg)
			if err != nil {
				log.Printf("Error decoding InvalidationMessage from Neighbor Super-Peer %s: %v", neighborID, err)
				continue
			}
			log.Printf("Super-Peer %s received INVALIDATION for file '%s' with VersionNumber %d from Origin Server %s",
				sp.Config.ID, invalidationMsg.FileName, invalidationMsg.VersionNumber, invalidationMsg.OriginServerID)
			sp.handleInvalidation(invalidationMsg)
		default:
			log.Printf("Unknown message type '%s' from Neighbor Super-Peer %s", messageType, neighborID)
		}
	}

	// Remove the connection upon disconnection
	sp.mu.Lock()
	delete(sp.NeighborConns, neighborID)
	sp.mu.Unlock()
}

// handleLeafNodeConnection manages communication with a Leaf-Node
func (sp *SuperPeer) handleLeafNodeConnection(conn net.Conn, leafNodeID string) {
	log.Printf("Super-Peer %s handling connection with Leaf-Node %s", sp.Config.ID, leafNodeID)

	// Store the connection
	sp.mu.Lock()
	sp.LeafNodeConns[leafNodeID] = conn
	sp.mu.Unlock()

	decoder := json.NewDecoder(conn)
	for {
		var msg map[string]interface{}
		err := decoder.Decode(&msg)
		if err != nil {
			if err == io.EOF {
				log.Printf("Leaf-Node %s disconnected", leafNodeID)
			} else {
				log.Printf("Error decoding message from Leaf-Node %s: %v", leafNodeID, err)
			}
			break
		}

		messageType, ok := msg["message_type"].(string)
		if !ok {
			log.Printf("Invalid message from Leaf-Node %s: missing message_type", leafNodeID)
			continue
		}

		switch messageType {
		case MsgTypeFileRegistration:
			var registrationMsg FileRegistrationMessage
			err := mapToStruct(msg, &registrationMsg)
			if err != nil {
				log.Printf("Error decoding FileRegistrationMessage from Leaf-Node %s: %v", leafNodeID, err)
				continue
			}
			sp.handleFileRegistration(registrationMsg)
		case MsgTypeFileQuery:
			var queryMsg FileQueryMessage
			err := mapToStruct(msg, &queryMsg)
			if err != nil {
				log.Printf("Error decoding FileQueryMessage from Leaf-Node %s: %v", leafNodeID, err)
				continue
			}
			log.Printf("Super-Peer %s received FileQueryMessage for '%s' from %s", sp.Config.ID, queryMsg.FileName, queryMsg.OriginID)
			sp.handleFileQuery(queryMsg, conn)
		case MsgTypeInvalidation:
			var invalidationMsg InvalidationMessage
			err := mapToStruct(msg, &invalidationMsg)
			if err != nil {
				log.Printf("Error decoding InvalidationMessage from Leaf-Node %s: %v", leafNodeID, err)
				continue
			}
			log.Printf("Super-Peer %s received INVALIDATION for file '%s' with VersionNumber %d from Origin Server %s",
				sp.Config.ID, invalidationMsg.FileName, invalidationMsg.VersionNumber, invalidationMsg.OriginServerID)
			sp.handleInvalidation(invalidationMsg)
		default:
			log.Printf("Unknown message type '%s' from Leaf-Node %s", messageType, leafNodeID)
		}
	}

	// Remove the connection upon disconnection
	sp.mu.Lock()
	delete(sp.LeafNodeConns, leafNodeID)
	sp.mu.Unlock()
}

// sendJSONMessage sends a JSON-encoded message to a connection
func (sp *SuperPeer) sendJSONMessage(conn net.Conn, msg interface{}) error {
	sp.connMu.Lock()
	defer sp.connMu.Unlock()
	encoder := json.NewEncoder(conn)
	return encoder.Encode(msg)
}

// handleFileRegistration registers files from a Leaf-Node, ensuring no duplicates
func (sp *SuperPeer) handleFileRegistration(msg FileRegistrationMessage) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for _, file := range msg.Files {
		if sp.FileIndex[file.FileName] == nil {
			sp.FileIndex[file.FileName] = make(map[string]struct{})
		}

		if _, exists := sp.FileIndex[file.FileName][msg.LeafNodeID]; !exists {
			sp.FileIndex[file.FileName][msg.LeafNodeID] = struct{}{}
			log.Printf("Super-Peer %s registered file '%s' from Leaf-Node %s", sp.Config.ID, file.FileName, msg.LeafNodeID)
		} else {
			log.Printf("Super-Peer %s ignored duplicate registration of file '%s' from Leaf-Node %s", sp.Config.ID, file.FileName, msg.LeafNodeID)
		}
	}
}

// handleFileQuery processes a FileQueryMessage
func (sp *SuperPeer) handleFileQuery(msg FileQueryMessage, sourceConn net.Conn) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	// Check if the message ID is already processed
	if _, exists := sp.MessageCache[msg.MessageID]; exists {
		// Already processed, ignore
		return
	}

	// Store in MessageCache
	sp.MessageCache[msg.MessageID] = CacheEntry{
		OriginID:     msg.OriginID,
		UpstreamConn: sourceConn,
		Timestamp:    time.Now(),
	}

	// Check if the file exists in the local FileIndex
	leafNodeIDs, found := sp.FileIndex[msg.FileName]

	if found {
		// Send QueryHitMessages back to the originator, excluding the origin Leaf-Node
		for leafNodeID := range leafNodeIDs {
			if leafNodeID == msg.OriginID {
				// Skip the origin Leaf-Node
				continue
			}

			leafConfig, exists := sp.LeafNodeConfigs[leafNodeID]
			if !exists {
				continue
			}

			queryHitMsg := QueryHitMessage{
				MessageType:  MsgTypeQueryHit,
				MessageID:    msg.MessageID,
				TTL:          msg.TTL,
				RespondingID: leafNodeID,
				FileName:     msg.FileName,
				Address:      leafConfig.Address,
				Port:         leafConfig.Port,
			}

			err := sp.sendJSONMessage(sourceConn, queryHitMsg)
			if err != nil {
				log.Printf("Error sending QueryHitMessage to originator: %v", err)
			}
		}
	}

	// Forward the query to neighbors if TTL > 1
	if msg.TTL > 1 {
		msg.TTL--

		for neighborID, conn := range sp.NeighborConns {
			// Avoid sending back to the source if necessary
			if conn == sourceConn {
				continue
			}
			log.Printf("Super-Peer %s forwarding query for '%s' to neighbor Super-Peer %s", sp.Config.ID, msg.FileName, neighborID)
			err := sp.sendJSONMessage(conn, msg)
			if err != nil {
				log.Printf("Error forwarding query to neighbor Super-Peer %s: %v", neighborID, err)
			}
		}
	}
}

// forwardQueryHit forwards a QueryHitMessage to the originator
func (sp *SuperPeer) forwardQueryHit(msg QueryHitMessage) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	entry, exists := sp.MessageCache[msg.MessageID]
	if !exists {
		log.Printf("No origin connection found for MessageID %s", msg.MessageID)
		return
	}

	// Decrement TTL
	if msg.TTL > 1 {
		msg.TTL--
	}

	log.Printf("Super-Peer %s forwarding QueryHitMessage for MessageID %s to originator", sp.Config.ID, msg.MessageID)
	err := sp.sendJSONMessage(entry.UpstreamConn, msg)
	if err != nil {
		log.Printf("Error forwarding QueryHitMessage: %v", err)
	}
}

// handleInvalidation processes an INVALIDATION message
func (sp *SuperPeer) handleInvalidation(msg InvalidationMessage) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	fileName := msg.FileName
	// newVersion := msg.VersionNumber // Removed to resolve 'declared and not used' error

	// Check if the file exists in the FileIndex
	leafNodeIDs, exists := sp.FileIndex[fileName]
	if !exists {
		log.Printf("Super-Peer %s: Received INVALIDATION for unknown file '%s'. Ignoring.", sp.Config.ID, fileName)
		return
	}

	// Deregister the file by removing it from the FileIndex
	delete(sp.FileIndex, fileName)
	log.Printf("Super-Peer %s: Deregistered file '%s' due to INVALIDATION", sp.Config.ID, fileName)

	// Notify all leaf nodes that have this file to discard their cached copies
	for leafNodeID := range leafNodeIDs {
		leafConn, exists := sp.LeafNodeConns[leafNodeID]
		if !exists {
			log.Printf("Super-Peer %s: Leaf-Node %s connection not found. Cannot notify.", sp.Config.ID, leafNodeID)
			continue
		}

		// Reuse the InvalidationMessage structure for simplicity
		err := sp.sendJSONMessage(leafConn, msg)
		if err != nil {
			log.Printf("Super-Peer %s: Failed to send INVALIDATION to Leaf-Node %s: %v", sp.Config.ID, leafNodeID, err)
			continue
		}
		log.Printf("Super-Peer %s: Sent INVALIDATION to Leaf-Node %s for file '%s'", sp.Config.ID, leafNodeID, fileName)
	}

	// Propagate the INVALIDATION message to other neighbor super-peers
	for neighborID, neighborConn := range sp.NeighborConns {
		// Avoid sending back to the origin server to prevent loops
		if neighborID == msg.OriginServerID {
			continue
		}
		// Serialize the InvalidationMessage to JSON
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			log.Printf("Super-Peer %s: Failed to marshal INVALIDATION message for propagation: %v", sp.Config.ID, err)
			continue
		}
		_, err = neighborConn.Write(msgBytes)
		if err != nil {
			log.Printf("Super-Peer %s: Failed to send INVALIDATION to Neighbor Super-Peer %s: %v", sp.Config.ID, neighborID, err)
			continue
		}
		log.Printf("Super-Peer %s: Propagated INVALIDATION for file '%s' to Neighbor Super-Peer %s", sp.Config.ID, fileName, neighborID)
	}
}

// cleanupMessageCache periodically removes old entries from MessageCache
func (sp *SuperPeer) cleanupMessageCache() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		sp.mu.Lock()
		for msgID, entry := range sp.MessageCache {
			if time.Since(entry.Timestamp) > 30*time.Minute {
				delete(sp.MessageCache, msgID)
				log.Printf("Super-Peer %s removed MessageID %s from MessageCache", sp.Config.ID, msgID)
			}
		}
		sp.mu.Unlock()
	}
}

// logFileIndex periodically logs the current FileIndex for debugging
func (sp *SuperPeer) logFileIndex() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		sp.mu.Lock()
		log.Printf("Super-Peer %s FileIndex Status:", sp.Config.ID)
		for file, leafNodes := range sp.FileIndex {
			leafList := []string{}
			for ln := range leafNodes {
				leafList = append(leafList, ln)
			}
			log.Printf("  File: '%s' -> Leaf-Nodes: %v", file, leafList)
		}
		sp.mu.Unlock()
	}
}
