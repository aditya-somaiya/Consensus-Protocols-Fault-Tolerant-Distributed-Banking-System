package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func main() {
	// Define clients and servers
	clients := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	servers := []string{"S1", "S2", "S3", "S4", "S5", "S6", "S7"} // Adjust based on your setup

	// Create directories if they don't exist
	if err := os.MkdirAll("client_keys", 0700); err != nil {
		log.Fatalf("Failed to create client_keys directory: %v", err)
	}
	if err := os.MkdirAll("server_keys", 0700); err != nil {
		log.Fatalf("Failed to create server_keys directory: %v", err)
	}

	// Generate keys for clients
	for _, id := range clients {
		generateKeyPair(fmt.Sprintf("client_keys/%s_private.pem", id), fmt.Sprintf("client_keys/%s_public.pem", id))
	}

	// Generate keys for servers
	for _, id := range servers {
		generateKeyPair(fmt.Sprintf("server_keys/%s_private.pem", id), fmt.Sprintf("server_keys/%s_public.pem", id))
	}

	fmt.Println("Key generation completed successfully.")
}

func generateKeyPair(privatePath, publicPath string) {
	// Generate RSA private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatalf("Failed to generate RSA key: %v", err)
	}

	// Encode and save private key
	privateFile, err := os.Create(privatePath)
	if err != nil {
		log.Fatalf("Failed to create private key file %s: %v", privatePath, err)
	}
	defer privateFile.Close()

	privatePEM := pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}

	if err := pem.Encode(privateFile, &privatePEM); err != nil {
		log.Fatalf("Failed to write private key to %s: %v", privatePath, err)
	}

	// Encode and save public key
	pubASN1 := x509.MarshalPKCS1PublicKey(&privateKey.PublicKey)
	publicPEM := pem.Block{
		Type:  "RSA PUBLIC KEY",
		Bytes: pubASN1,
	}

	publicFile, err := os.Create(publicPath)
	if err != nil {
		log.Fatalf("Failed to create public key file %s: %v", publicPath, err)
	}
	defer publicFile.Close()

	if err := pem.Encode(publicFile, &publicPEM); err != nil {
		log.Fatalf("Failed to write public key to %s: %v", publicPath, err)
	}

	fmt.Printf("Generated key pair:\n  Private: %s\n  Public: %s\n", filepath.Base(privatePath), filepath.Base(publicPath))
}
