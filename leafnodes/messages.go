// messages.go
package main

import (
	
	"time"
	"encoding/json"
	"net"

)

// -----------------------------
// Message Types and Structs
// -----------------------------

// Message Types
const (
	MsgTypePeerID           = "peer_id"
	MsgTypeFileRegistration = "file_registration"
	MsgTypeFileQuery        = "file_query"
	MsgTypeQueryHit         = "query_hit"
	MsgTypeInvalidation     = "invalidation" // New Message Type
	MsgTypePollRequest      = "poll_request" // For Pull-Based Consistency
	MsgTypePollResponse     = "poll_response" // For Pull-Based Consistency
)

// Peer Types
const (
	SuperPeerType = "super_peer"
	LeafNodeType  = "leaf_node"
)

// PeerIDMessage is used by peers to identify themselves
type PeerIDMessage struct {
	MessageType string `json:"message_type"`
	PeerType    string `json:"peer_type"`
	PeerID      string `json:"peer_id"`
}

// FileMetadata contains information about a shared file
type FileMetadata struct {
	FileName string `json:"file_name"`
	FileSize int64  `json:"file_size"`
}

// FileRegistrationMessage is sent by leaf-nodes to register their files
type FileRegistrationMessage struct {
	MessageType string         `json:"message_type"`
	LeafNodeID  string         `json:"leaf_node_id"`
	Files       []FileMetadata `json:"files"`
}

// FileQueryMessage is used to query for files
type FileQueryMessage struct {
	MessageType string `json:"message_type"`
	MessageID   string `json:"message_id"`
	OriginID    string `json:"origin_id"`
	FileName    string `json:"file_name"`
	TTL         int    `json:"ttl"`
}

// QueryHitMessage is sent in response to a FileQueryMessage
type QueryHitMessage struct {
	MessageType  string    `json:"message_type"`
	MessageID    string    `json:"message_id"`
	TTL          int       `json:"ttl"`
	RespondingID string    `json:"responding_id"`
	FileName     string    `json:"file_name"`
	Address      string    `json:"address"`
	Port         int       `json:"port"`
	LastModified time.Time `json:"last_modified_time"` // New Field
	IsOrigin     bool      `json:"is_origin_server"`   // Optional Bit
}

// InvalidationMessage is used by the origin server to notify peers about file modifications
type InvalidationMessage struct {
	MessageType    string `json:"message_type"`     // Should be "invalidation"
	MsgID          string `json:"msg_id"`           // Unique identifier for the message
	OriginServerID string `json:"origin_server_id"` // ID of the origin server
	FileName       string `json:"file_name"`        // Name of the file being invalidated
	VersionNumber  int    `json:"version_number"`   // New version number of the file
}

// PollRequestMessage is used by leafnodes to poll the origin server for file validity (Pull-Based)
type PollRequestMessage struct {
	MessageType          string `json:"message_type"`
	MessageID            string `json:"message_id"`
	FileName             string `json:"file_name"`
	CurrentVersionNumber int    `json:"current_version_number"`
}

// PollResponseMessage is sent by the origin server in response to a PollRequestMessage (Pull-Based)
type PollResponseMessage struct {
	MessageType      string `json:"message_type"`
	MessageID        string `json:"message_id"`
	FileName         string `json:"file_name"`
	Status           string `json:"status"`             // "valid" or "invalid"
	NewVersionNumber int    `json:"new_version_number"` // If invalid
}

// Utility Functions

// sendJSONMessage encodes and sends a JSON message over the provided connection
func sendJSONMessage(conn net.Conn, msg interface{}) error {
	encoder := json.NewEncoder(conn)
	return encoder.Encode(msg)
}

// mapToStruct is a helper function to convert a map to a struct
func mapToStruct(m map[string]interface{}, s interface{}) error {
	temp, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(temp, s)
}
