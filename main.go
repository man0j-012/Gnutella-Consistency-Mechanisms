// main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

// -----------------------------
// Configuration Loading
// -----------------------------

// SuperPeerConfig contains configuration for a super-peer
type SuperPeerConfig struct {
	ID        string   `json:"id"`
	Address   string   `json:"address"`
	Port      int      `json:"port"`
	Neighbors []string `json:"neighbors"`
	LeafNodes []string `json:"leaf_nodes"`
}

// LeafNodeConfig contains configuration for a leaf-node
type LeafNodeConfig struct {
	ID        string `json:"id"`
	Address   string `json:"address"`
	Port      int    `json:"port"`
	SuperPeer string `json:"super_peer"`
}

// Config contains the overall configuration
type Config struct {
	SuperPeers []SuperPeerConfig `json:"super_peers"`
	LeafNodes  []LeafNodeConfig  `json:"leaf_nodes"`
}

// LoadConfig loads configuration from a JSON file
func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

// -----------------------------
// Helper Functions
// -----------------------------

// mapToStruct converts a map to a struct using JSON marshalling
func mapToStruct(m map[string]interface{}, result interface{}) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, result)
}

func main() {
	// Check if the config file path and peer ID are provided
	if len(os.Args) < 3 {
		log.Fatal("Usage: go run main.go [config file] [peer ID]")
	}

	// Get the config file path and peer ID from the command-line arguments
	configFile := os.Args[1]
	peerID := os.Args[2]

	// Load the configuration
	config, err := LoadConfig(configFile)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	// Determine if the peer is a super-peer or leaf-node
	isSuperPeer := false
	var superPeerConfig SuperPeerConfig
	var leafNodeConfig LeafNodeConfig

	// Search for the peer ID in the super-peers list
	for _, sp := range config.SuperPeers {
		if sp.ID == peerID {
			isSuperPeer = true
			superPeerConfig = sp
			break
		}
	}

	if !isSuperPeer {
		// If not found in super-peers, search in the leaf-nodes list
		found := false
		for _, ln := range config.LeafNodes {
			if ln.ID == peerID {
				leafNodeConfig = ln
				found = true
				break
			}
		}
		if !found {
			log.Fatalf("Peer ID %s not found in configuration", peerID)
		}
	}

	// Initialize the peer based on its role
	if isSuperPeer {
		fmt.Printf("Starting Super-Peer %s\n", superPeerConfig.ID)
		// Initialize and start the super-peer
		sp := NewSuperPeer(superPeerConfig, config)
		sp.Start()
	} else {
		fmt.Printf("Starting Leaf-Node %s\n", leafNodeConfig.ID)
		// Initialize and start the leaf-node
		ln := NewLeafNode(leafNodeConfig, config)
		ln.Start()
	}
}
