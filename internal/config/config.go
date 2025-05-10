package config

import (
	"flag"
	"log"
	"os"
	"strconv"
)

type Config struct {
	GRPCPort int
	LogLevel string
}

func Load() *Config {
	var cfg Config

	defaultGRPCPort := 9090
	defaultLogLevel := "INFO"

	if portStr := os.Getenv("GRPC_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			defaultGRPCPort = port
		} else {
			log.Printf("Warning: Invalid GRPC_PORT env var '%s', using default %d", portStr, defaultGRPCPort)
		}
	}
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		defaultLogLevel = level
	}

	flag.IntVar(&cfg.GRPCPort, "grpc-port", defaultGRPCPort, "gRPC server port")
	flag.StringVar(&cfg.LogLevel, "log-level", defaultLogLevel, "Log level (INFO, DEBUG, ERROR)")

	flag.Parse()

	if cfg.GRPCPort <= 0 || cfg.GRPCPort > 65535 {
		log.Fatalf("Invalid gRPC port: %d. Must be between 1 and 65535.", cfg.GRPCPort)
	}

	log.Printf("Configuration loaded: %+v", cfg)
	return &cfg
}
