// messages.go
package main

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
	MessageType  string `json:"message_type"`
	MessageID    string `json:"message_id"`
	TTL          int    `json:"ttl"`
	RespondingID string `json:"responding_id"`
	FileName     string `json:"file_name"`
	Address      string `json:"address"`
	Port         int    `json:"port"`
}

// InvalidationMessage is used by the origin server to notify peers about file modifications
type InvalidationMessage struct {
	MessageType    string `json:"message_type"`     // Should be "invalidation"
	MsgID          string `json:"msg_id"`           // Unique identifier for the message
	OriginServerID string `json:"origin_server_id"` // ID of the origin server
	FileName       string `json:"file_name"`        // Name of the file being invalidated
	VersionNumber  int    `json:"version_number"`   // New version number of the file
}
