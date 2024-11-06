// main_leafnode.go
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
)

// loadLeafNodeConfig loads the LeafNode configuration from a JSON file
func loadLeafNodeConfig(filename string) (LeafNodeConfig, error) {
	var config LeafNodeConfig
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(data, &config)
	return config, err
}

func main() {
	// Parse command-line arguments for config file
	configFile := flag.String("config", "leafnode_config1.json", "Path to LeafNode config JSON file")
	flag.Parse()

	// Load LeafNode configuration
	config, err := loadLeafNodeConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load LeafNode config: %v", err)
	}

	// Initialize LeafNode
	leafNode := NewLeafNode(config)

	// Connect to SuperPeer
	superPeerAddress := fmt.Sprintf("%s:%d", leafNode.Config.Address, leafNode.Config.Port)
	err = leafNode.connectToSuperPeer(superPeerAddress)
	if err != nil {
		log.Fatalf("Leaf-Node %s: %v", leafNode.Config.ID, err)
	}

	// Start listening for incoming messages (handled in listenToSuperPeer)
	go leafNode.listen()

	// Start the CLI for user commands
	leafNode.startCLI()
}
