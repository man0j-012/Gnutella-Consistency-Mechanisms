// main_superpeer.go
package main

import (
    "bufio"
    "encoding/json"
    "flag"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "strings"
    "sync"
)

// loadSuperPeerConfig loads the SuperPeer configuration from a JSON file
func loadSuperPeerConfig(filename string) (SuperPeerConfig, error) {
    fmt.Printf("os.Args: %v\n", os.Args)
    fmt.Printf("Filename received in loadSuperPeerConfig: %s\n", filename)
    fmt.Printf("Attempting to load configuration file: %s\n", filename)
    data, err := ioutil.ReadFile(filename)
    if err != nil {
        return SuperPeerConfig{}, err
    }
    var config SuperPeerConfig
    err = json.Unmarshal(data, &config)
    return config, err
}

func main() {
    // Parse command-line arguments for config file
    configFile := flag.String("config", "", "Path to SuperPeer config JSON file")
    flag.Parse()

    // Check if the config file is provided
    if *configFile == "" {
        fmt.Println("Usage: superpeer.exe -config=<config_file>")
        return
    }

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

    // Command interface for simulating file modifications
    go func() {
        scanner := bufio.NewScanner(os.Stdin)
        fmt.Println("Enter 'modify <filename>' to simulate file modification:")
        for scanner.Scan() {
            input := scanner.Text()
            parts := strings.Split(input, " ")
            if len(parts) != 2 || parts[0] != "modify" {
                fmt.Println("Invalid command. Usage: modify <filename>")
                continue
            }
            fileName := parts[1]
            superPeer.simulateFileModification(fileName)
        }
    }()

    // Keep the main goroutine alive
    wg.Wait()
}
