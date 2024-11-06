// superpeer.go
package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
	"encoding/json"
)

// SuperPeerConfig represents the configuration for a SuperPeer
type SuperPeerConfig struct {
	ID        string   `json:"id"`
	Address   string   `json:"address"`
	Port      int      `json:"port"`
	Neighbors []string `json:"neighbors"`  // IDs of neighboring SuperPeers
	LeafNodes []string `json:"leaf_nodes"` // IDs of connected LeafNodes
}

// CacheEntry represents an entry in the message cache to prevent duplicate processing
type CacheEntry struct {
	OriginID     string
	UpstreamConn net.Conn
	Timestamp    time.Time
}

// SuperPeer represents a SuperPeer in the network
type SuperPeer struct {
	Config            SuperPeerConfig
	NeighborConns     map[string]net.Conn            // Map of Neighbor SuperPeer ID to connection
	LeafNodeConns     map[string]net.Conn            // Map of LeafNode ID to connection
	FileIndex         map[string]map[string]struct{} // FileName -> Set of LeafNode IDs
	MessageCache      map[string]CacheEntry          // MessageID -> CacheEntry
	mu                sync.RWMutex                   // Mutex to protect shared resources
	LeafNodeAddresses map[string]LeafNodeInfo        // LeafNode ID -> Address and Port
	OriginServerMap   map[string]string              // FileName -> OriginServerID
	FileMetadataMap   map[string]map[string]FileMetadata
}

// LeafNodeInfo holds the address and port of a LeafNode
type LeafNodeInfo struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

// NewSuperPeer initializes a new SuperPeer instance
func NewSuperPeer(config SuperPeerConfig) *SuperPeer {
	return &SuperPeer{
		Config:            config,
		NeighborConns:     make(map[string]net.Conn),
		LeafNodeConns:     make(map[string]net.Conn),
		FileIndex:         make(map[string]map[string]struct{}),
		MessageCache:      make(map[string]CacheEntry),
		LeafNodeAddresses: make(map[string]LeafNodeInfo),
		OriginServerMap:   make(map[string]string),
		FileMetadataMap:   make(map[string]map[string]FileMetadata),
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

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Super-Peer %s: Error accepting connection: %v", sp.Config.ID, err)
			continue
		}
		go sp.handleConnection(conn)
	}
}

// handleConnection determines the type of peer (SuperPeer or LeafNode) and handles accordingly
func (sp *SuperPeer) handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	var peerIDMsg PeerIDMessage
	err := decoder.Decode(&peerIDMsg)
	if err != nil {
		log.Printf("Super-Peer %s: Error decoding PeerIDMessage: %v", sp.Config.ID, err)
		return
	}

	if peerIDMsg.MessageType != MsgTypePeerID {
		log.Printf("Super-Peer %s: Expected PeerIDMessage, received %s", sp.Config.ID, peerIDMsg.MessageType)
		return
	}

	switch peerIDMsg.PeerType {
	case SuperPeerType:
		sp.handleSuperPeer(conn, peerIDMsg.PeerID)
	case LeafNodeType:
		sp.handleLeafNode(conn, peerIDMsg.PeerID)
	default:
		log.Printf("Super-Peer %s: Unknown PeerType '%s' from PeerID '%s'", sp.Config.ID, peerIDMsg.PeerType, peerIDMsg.PeerID)
	}
}

// handleSuperPeer manages connections and message handling for neighboring SuperPeers
func (sp *SuperPeer) handleSuperPeer(conn net.Conn, peerID string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if _, exists := sp.NeighborConns[peerID]; exists {
		log.Printf("Super-Peer %s: SuperPeer %s already connected. Closing duplicate connection.", sp.Config.ID, peerID)
		return
	}

	sp.NeighborConns[peerID] = conn
	log.Printf("Super-Peer %s: Connected to SuperPeer %s", sp.Config.ID, peerID)

	// Start listening for messages from this SuperPeer
	go sp.listenToSuperPeer(conn, peerID)
}

// handleLeafNode manages connections and message handling for LeafNodes
func (sp *SuperPeer) handleLeafNode(conn net.Conn, leafNodeID string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if _, exists := sp.LeafNodeConns[leafNodeID]; exists {
		log.Printf("Super-Peer %s: LeafNode %s already connected. Closing duplicate connection.", sp.Config.ID, leafNodeID)
		return
	}

	sp.LeafNodeConns[leafNodeID] = conn
	log.Printf("Super-Peer %s: Connected to LeafNode %s", sp.Config.ID, leafNodeID)

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
			sp.handleFileRegistration(regMsg, peerID)
		case MsgTypeInvalidation:
			var invMsg InvalidationMessage
			err := mapToStruct(msg, &invMsg)
			if err != nil {
				log.Printf("Super-Peer %s: Error parsing InvalidationMessage from SuperPeer %s: %v", sp.Config.ID, peerID, err)
				continue
			}
			sp.handleInvalidation(invMsg, conn)
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
			sp.handleFileRegistration(regMsg, leafNodeID)
		case MsgTypeInvalidation:
			var invMsg InvalidationMessage
			err := mapToStruct(msg, &invMsg)
			if err != nil {
				log.Printf("Super-Peer %s: Error parsing InvalidationMessage from LeafNode %s: %v", sp.Config.ID, leafNodeID, err)
				continue
			}
			sp.handleInvalidation(invMsg, conn)
		// Handle other message types as needed
		default:
			log.Printf("Super-Peer %s: Unknown message_type '%s' from LeafNode %s", sp.Config.ID, messageType, leafNodeID)
		}
	}
}

// handleFileRegistration processes a FileRegistrationMessage
func (sp *SuperPeer) handleFileRegistration(msg FileRegistrationMessage, peerID string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for _, file := range msg.Files {
		if _, exists := sp.FileIndex[file.FileName]; !exists {
			sp.FileIndex[file.FileName] = make(map[string]struct{})
		}
		sp.FileIndex[file.FileName][msg.LeafNodeID] = struct{}{}
		log.Printf("Super-Peer %s: Registered file '%s' from LeafNode %s", sp.Config.ID, file.FileName, msg.LeafNodeID)
	}
}

// handleInvalidation processes an InvalidationMessage
func (sp *SuperPeer) handleInvalidation(msg InvalidationMessage, sourceConn net.Conn) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	log.Printf("Super-Peer %s: Received INVALIDATION for '%s' from Origin Server %s", sp.Config.ID, msg.FileName, msg.OriginServerID)

	// Check if the SuperPeer has the file registered
	leafNodes, exists := sp.FileIndex[msg.FileName]
	if exists {
		// Deregister the file from the Origin Server
		delete(leafNodes, msg.OriginServerID)
		log.Printf("Super-Peer %s: Deregistered '%s' from Origin Server %s", sp.Config.ID, msg.FileName, msg.OriginServerID)

		// If no LeafNodes have the file, remove the entry from FileIndex
		if len(leafNodes) == 0 {
			delete(sp.FileIndex, msg.FileName)
			log.Printf("Super-Peer %s: Removed '%s' from FileIndex as no LeafNodes are hosting it", sp.Config.ID, msg.FileName)
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

			// Implement message deduplication if necessary
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
