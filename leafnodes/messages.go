// messages.go
package main

import (
    "encoding/json"
    "net"
)

// Message Types
const (
    MsgTypePeerID           = "peer_id"
    MsgTypeFileRegistration = "file_registration"
    MsgTypeFileQuery        = "file_query"
    MsgTypeQueryHit         = "query_hit"
    MsgTypeInvalidation     = "invalidation"
    MsgTypePollRequest      = "poll_request"
    MsgTypePollResponse     = "poll_response"
    MsgTypeRefreshRequest   = "refresh_request"
    MsgTypeRefreshResponse  = "refresh_response"
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

// FileRegistrationMessage is sent by LeafNodes to register their files
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
    LastModified string `json:"last_modified_time"`
    IsOrigin     bool   `json:"is_origin_server"`
}

// InvalidationMessage is used by the origin server to notify peers about file modifications
type InvalidationMessage struct {
    MessageType    string `json:"message_type"`
    MsgID          string `json:"msg_id"`
    OriginServerID string `json:"origin_server_id"`
    FileName       string `json:"file_name"`
    VersionNumber  int    `json:"version_number"`
}

// PollRequestMessage is used by LeafNodes to poll the origin server for file validity
type PollRequestMessage struct {
    MessageType          string `json:"message_type"`
    MessageID            string `json:"message_id"`
    FileName             string `json:"file_name"`
    CurrentVersionNumber int    `json:"current_version_number"`
}

// PollResponseMessage is sent by the origin server in response to a PollRequestMessage
type PollResponseMessage struct {
    MessageType      string `json:"message_type"`
    MessageID        string `json:"message_id"`
    FileName         string `json:"file_name"`
    Status           string `json:"status"`             // "valid" or "invalid"
    NewVersionNumber int    `json:"new_version_number"` // If invalid
}

// RefreshRequestMessage is used by LeafNodes to request a refresh of an outdated file
type RefreshRequestMessage struct {
    MessageType string `json:"message_type"`
    MessageID   string `json:"message_id"` // Added field
    FileName    string `json:"file_name"`
}

// RefreshResponseMessage is sent by the SuperPeer in response to a RefreshRequestMessage
type RefreshResponseMessage struct {
    MessageType    string `json:"message_type"`
    MessageID      string `json:"message_id"`          // To correlate with the request
    FileName       string `json:"file_name"`
    FileData       []byte `json:"file_data"`
    VersionNumber  int    `json:"version_number"`
    LastModified   string `json:"last_modified_time"` // RFC3339 formatted string
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
