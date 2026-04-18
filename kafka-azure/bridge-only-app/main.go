package main

import "log"

func main() {
	cfg := loadConfig()
	if err := runBridge(cfg); err != nil {
		log.Fatalf("bridge failed: %v", err)
	}
}
