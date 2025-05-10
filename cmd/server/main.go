package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"VK1/internal/config"
	grpcserver "VK1/internal/server"
	"VK1/pkg/subpub"
	"VK1/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	logger := log.New(os.Stdout, "GRPC-SUBPUB: ", log.LstdFlags|log.Lshortfile)

	logger.Println("Starting gRPC SubPub Service...")

	cfg := config.Load()

	sp := subpub.NewSubPub()
	logger.Println("SubPub system initialized.")

	pubSubServer := grpcserver.NewPubSubServer(sp, logger)
	logger.Println("PubSub gRPC server implementation created.")

	grpcSrv := grpc.NewServer()
	proto.RegisterPubSubServer(grpcSrv, pubSubServer)
	reflection.Register(grpcSrv)
	logger.Printf("gRPC server configured. Service registered.")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		logger.Fatalf("Failed to listen on port %d: %v", cfg.GRPCPort, err)
	}
	logger.Printf("Listening on :%d", cfg.GRPCPort)

	go func() {
		logger.Println("Starting gRPC server...")
		if err := grpcSrv.Serve(lis); err != nil {
			if !errors.Is(err, grpc.ErrServerStopped) {
				logger.Fatalf("Failed to serve gRPC: %v", err)
			}
			logger.Println("gRPC server stopped serving new connections.")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Println("Shutdown signal received. Initiating graceful shutdown...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	logger.Println("Attempting to gracefully stop gRPC server...")
	grpcSrv.GracefulStop()
	logger.Println("gRPC server stopped gracefully.")

	logger.Println("Closing SubPub system...")
	if err := sp.Close(shutdownCtx); err != nil {
		logger.Printf("Error closing SubPub system: %v", err)
	} else {
		logger.Println("SubPub system closed successfully.")
	}

	logger.Println("Service shut down gracefully.")
}
