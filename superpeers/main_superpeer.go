// main_superpeer.go
package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"sync"
)

// loadSuperPeerConfig loads the SuperPeer configuration from a JSON file
func loadSuperPeerConfig(filename string) (SuperPeerConfig, error) {
	var config SuperPeerConfig
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(data, &config)
	return config, err
}

func main() {
	// Parse command-line arguments for config file
	configFile := flag.String("config", "superpeer_config1.json", "Path to SuperPeer config JSON file")
	flag.Parse()

	// Load SuperPeer configuration
	config, err := loadSuperPeerConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load SuperPeer config: %v", err)
	}

	// Initialize SuperPeer
	superPeer := NewSuperPeer(config)

	// Start the SuperPeer server
	var wg sync.WaitGroup
	wg.Add(1)
	go superPeer.Start(&wg)

	// Keep the main goroutine alive
	wg.Wait()
}
