package main

import (
	"bufio"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/csv"
	"encoding/gob"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Transaction represents a transaction between clients
type Transaction struct {
	SetNumber int
	Sender    string
	Receiver  string
	Amount    float64
	Timestamp time.Time
}

// TransactionSet represents a set of transactions along with active servers
type TransactionSet struct {
	SetNumber        int
	Transactions     []Transaction
	LiveServers      []string
	ByzantineServers []string
}

// Request is the structure used for sending transaction requests
type Request struct {
	Type        string
	ViewNumber  int // Add this field
	Transaction Transaction
	Timestamp   time.Time
	ClientID    string
	Signature   string
}

var (
	currentViewNumber int = 1
)

// Response is the structure for receiving responses from nodes
type Response struct {
	ViewNumber int
	Timestamp  time.Time
	ClientID   string
	NodeID     string
	Result     string
	Signature  string
}

// Client represents a client with a public-private key pair
type Client struct {
	ID         string
	PrivateKey *rsa.PrivateKey
	PublicKey  *rsa.PublicKey
}

// Node represents a node with a public-private key pair
type Node struct {
	ID         string
	PrivateKey *rsa.PrivateKey
	PublicKey  *rsa.PublicKey
}

// Reply is the structure for replies from the server
type Reply struct {
	Type       string
	ViewNumber int
	Timestamp  time.Time
	ClientID   string
	NodeID     string
	Result     string
	Signature  string
}

// ClientRPC handles RPC calls from servers
type ClientRPC struct {
	ID string
}

// StatusUpdate represents the status information sent to servers
type StatusUpdate struct {
	ActiveServers    []string
	ByzantineServers []string
}

func (c *ClientRPC) ReceiveReply(reply Reply, ack *bool) error {
	// log.Printf("Client %s: Received reply: clientID - %s, nodeID - %s, timeStamp - %s, viewNumber - %d", c.ID, reply.ClientID, reply.NodeID, reply.Timestamp, reply.ViewNumber)

	// Update currentViewNumber and leader if the view number has increased
	if reply.ViewNumber > currentViewNumber {
		currentViewNumber = reply.ViewNumber
		leader = getPrimaryID(currentViewNumber)
		leaderServerAddr = allServers[leader]
		log.Printf("Client %s: Updated to new view number %d. New leader is %s.", c.ID, currentViewNumber, leader)
	}

	// Find the TransactionContext
	key := fmt.Sprintf("%s:%d", reply.ClientID, reply.Timestamp.UnixNano())
	pendingTransactionsMutex.Lock()
	txnCtx, exists := pendingTransactions[key]
	pendingTransactionsMutex.Unlock()

	if exists {
		txnCtx.replyCh <- Response{
			ViewNumber: reply.ViewNumber,
			Timestamp:  reply.Timestamp,
			ClientID:   reply.ClientID,
			NodeID:     reply.NodeID,
			Result:     reply.Result,
			Signature:  reply.Signature,
		}
	}
	*ack = true
	return nil
}

type TransactionContext struct {
	request Request
	replyCh chan Response
}

var (
	allServers               = make(map[string]string)
	activeServers            = make(map[string]string)
	activeServersMutex       sync.RWMutex
	rpcClients               = make(map[string]*rpc.Client)
	rpcClientsMutex          sync.Mutex
	f                        = 2
	leader                   = "S1"
	leaderServerAddr         string
	clients                  = make(map[string]Client)
	nodes                    = make(map[string]Node)
	pendingTransactions      = make(map[string]*TransactionContext)
	pendingTransactionsMutex sync.Mutex

	byzantineServers      = make(map[string]string) // New map for Byzantine servers
	byzantineServersMutex sync.RWMutex

	// Performance metrics
	totalTransactions int
	totalLatency      time.Duration
	latencyData       []time.Duration
	performanceMutex  sync.Mutex
	testStartTime     time.Time
	testEndTime       time.Time
)

func init() {
	// Initialize server addresses and client/node keys
	basePort := 1231
	for i := 1; i <= (3*f)+1; i++ {
		serverID := fmt.Sprintf("S%d", i)
		address := fmt.Sprintf("localhost:%d", basePort+i-1)
		allServers[serverID] = address
	}
	leaderServerAddr = allServers[leader]

	clientIDs := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	for _, id := range clientIDs {
		privateKeyPath := fmt.Sprintf("../client_keys/%s_private.pem", id)
		publicKeyPath := fmt.Sprintf("../client_keys/%s_public.pem", id)

		privateKey, err := loadPrivateKeyFromFile(privateKeyPath)
		if err != nil {
			log.Fatalf("Error loading private key for client %s: %v", id, err)
		}
		publicKey, err := loadPublicKeyFromFile(publicKeyPath)
		if err != nil {
			log.Fatalf("Error loading public key for client %s: %v", id, err)
		}
		clients[id] = Client{ID: id, PrivateKey: privateKey, PublicKey: publicKey}
	}
	for serverID := range allServers {
		privateKeyPath := fmt.Sprintf("../server_keys/%s_private.pem", serverID)
		publicKeyPath := fmt.Sprintf("../server_keys/%s_public.pem", serverID)

		privateKey, err := loadPrivateKeyFromFile(privateKeyPath)
		if err != nil {
			log.Fatalf("Error loading private key for server %s: %v", serverID, err)
		}
		publicKey, err := loadPublicKeyFromFile(publicKeyPath)
		if err != nil {
			log.Fatalf("Error loading public key for server %s: %v", serverID, err)
		}
		nodes[serverID] = Node{ID: serverID, PrivateKey: privateKey, PublicKey: publicKey}
	}
	leader = getPrimaryID(currentViewNumber)
	leaderServerAddr = allServers[leader]
}

func getPrimaryID(viewNumber int) string {
	totalServers := len(allServers) // Dynamically get total servers
	primaryIndex := (viewNumber - 1) % totalServers
	if primaryIndex < 0 {
		primaryIndex += totalServers
	}
	return fmt.Sprintf("S%d", primaryIndex+1)
}

// readInputCSV reads the input CSV file and returns transactions
func readInputCSV(filePath string) []TransactionSet {
	log.Printf("Reading input CSV file from %s\n", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	var transactionSets []TransactionSet
	var currentSetNumber int
	var currentTransactions []Transaction
	var currentLiveServers []string
	var currentByzServers []string // To store Byzantine servers

	lineNumber := 1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			log.Println("End of CSV file reached.")
			break
		}
		if err != nil {
			log.Fatalf("Error reading CSV at line %d: %v", lineNumber, err)
		}
		if len(record) < 4 { // Expecting 4 fields
			log.Printf("Skipping incomplete record at line %d: %v", lineNumber, record)
			lineNumber++
			continue
		}
		setNumberStr := strings.TrimSpace(record[0])
		transactionStr := strings.TrimSpace(record[1])
		liveServersStr := strings.TrimSpace(record[2])
		byzServersStr := strings.TrimSpace(record[3]) // 4th field for Byzantine servers

		if setNumberStr != "" {
			if currentSetNumber != 0 {
				transactionSets = append(transactionSets, TransactionSet{
					SetNumber:        currentSetNumber,
					Transactions:     currentTransactions,
					LiveServers:      currentLiveServers,
					ByzantineServers: currentByzServers, // Assign Byzantine servers
				})
				currentTransactions = []Transaction{}
			}
			currentSetNumber, _ = strconv.Atoi(setNumberStr)
			currentLiveServers = parseLiveServers(liveServersStr)
			currentByzServers = parseByzantineServers(byzServersStr) // Parse Byzantine servers
		}
		tx := parseTransaction(transactionStr)
		currentTransactions = append(currentTransactions, tx)
		lineNumber++
	}
	if currentSetNumber != 0 && len(currentTransactions) > 0 {
		transactionSets = append(transactionSets, TransactionSet{
			SetNumber:        currentSetNumber,
			Transactions:     currentTransactions,
			LiveServers:      currentLiveServers,
			ByzantineServers: currentByzServers,
		})
	}
	return transactionSets
}

func main() {
	gob.Register(Transaction{})
	gob.Register(Reply{})

	// Start the RPC server for the client
	clientRPC := &ClientRPC{ID: "Client"}

	rpcServer := rpc.NewServer()
	err := rpcServer.Register(clientRPC)
	if err != nil {
		log.Fatalf("Client: Failed to register RPC methods: %v", err)
	}

	clientListener, err := net.Listen("tcp", "localhost:1230")
	if err != nil {
		log.Fatalf("Client: Failed to start RPC server on localhost:1230: %v", err)
	}
	defer clientListener.Close()
	log.Println("Client RPC server started on localhost:1230.")

	go func() {
		for {
			conn, err := clientListener.Accept()
			if err != nil {
				log.Printf("Client: Error accepting connection: %v", err)
				continue
			}
			go rpcServer.ServeConn(conn)
		}
	}()

	// transactionSets := readInputCSV("../lab_test.csv")
	transactionSets := readInputCSVFromURL("https://drive.google.com/uc?export=download&id=1Cj06sjBfH7BVFPcMA65v7gBmzR42RQkP")

	// Initialize clientQueues without clientWGs
	clientQueues := make(map[string]chan Transaction)
	for clientID := range clients {
		clientQueues[clientID] = make(chan Transaction, 100)
		go processClientTransactions(clientID, clientQueues[clientID])
	}

	// Process transaction sets
	for _, tSet := range transactionSets {
		// **Reset performance metrics before starting the new set**
		resetPerformanceMetrics()

		// **Clear all pending transactions before processing a new set**
		clearPendingTransactions()

		// Update active and Byzantine servers
		updateActiveServers(tSet.LiveServers)
		updateByzantineServers(tSet.ByzantineServers)
		log.Printf("Processing Transaction Set %d",
			tSet.SetNumber)

		// Send status update to active servers
		sendStatusUpdate()
		time.Sleep(5 * time.Millisecond)

		var setWG sync.WaitGroup

		performanceMutex.Lock()
		testStartTime = time.Now() // Record start time for performance measurement
		performanceMutex.Unlock()

		for _, transaction := range tSet.Transactions {
			clientID := transaction.Sender
			queue, exists := clientQueues[clientID]
			if exists {
				setWG.Add(1)
				// Send the transaction to the client's queue
				go func(tx Transaction) {
					defer setWG.Done()
					queue <- tx
				}(transaction)
			} else {
				log.Printf("No queue found for client %s", clientID)
			}
		}

		// Wait for all transactions in the set to complete
		setWG.Wait()
		// log.Printf("All transactions in set %d completed.", tSet.SetNumber)

		performanceMutex.Lock()
		testEndTime = time.Now() // Record end time for performance measurement
		performanceMutex.Unlock()

		// After processing each set, present options to the user
		presentOptions()

		// Reset the system state before processing the next set
		resetSystemState()
	}

	// Close client queues
	for _, queue := range clientQueues {
		close(queue)
	}

	log.Println("All transactions processed.")
}

func presentOptions() {
	time.Sleep(2 * time.Second)
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("\nSelect an option:")
		fmt.Println("1. PrintLog")
		fmt.Println("2. PrintDB")
		fmt.Println("3. PrintStatus")
		fmt.Println("4. Continue to next transaction set")
		fmt.Println("5. PrintView")
		fmt.Println("6. Performance")
		fmt.Print("Enter your choice (default is 4): ")

		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input. Please try again.")
			continue
		}
		input = strings.TrimSpace(input)

		if input == "" {
			fmt.Println("Continuing to next transaction set.")
			return
		}

		choice, err := strconv.Atoi(input)
		if err != nil {
			fmt.Println("Invalid input. Please enter a number.")
			continue
		}

		switch choice {
		case 1:
			fmt.Print("Enter Server ID (e.g., S1): ")
			serverID, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading Server ID. Please try again.")
				continue
			}
			serverID = strings.TrimSpace(serverID)
			printLog(serverID)
		case 2:
			printDB()
		case 3:
			fmt.Print("Enter Sequence Number: ")
			seqStr, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading Sequence Number. Please try again.")
				continue
			}
			seqStr = strings.TrimSpace(seqStr)
			seqNum, err := strconv.ParseInt(seqStr, 10, 64)
			if err != nil {
				fmt.Println("Invalid sequence number. Please enter a valid integer.")
				continue
			}
			printStatus(seqNum)
		case 4:
			return
		case 5:
			printView()
		case 6:
			performance()
		default:
			fmt.Println("Invalid choice. Please try again.")
		}
	}
}

// printLog prints the log of a given server
func printLog(serverID string) {
	address, exists := allServers[serverID]
	if !exists {
		fmt.Printf("Server %s does not exist.\n", serverID)
		return
	}

	client, err := getRPCClient(address)
	if err != nil {
		fmt.Printf("Failed to connect to server %s at %s: %v\n", serverID, address, err)
		return
	}

	var reply string
	err = client.Call("Server.PrintLog", struct{}{}, &reply)
	if err != nil {
		fmt.Printf("Error calling PrintLog on server %s: %v\n", serverID, err)
		return
	}

	fmt.Printf("Log of Server %s:\n%s\n", serverID, reply)
}

// printDB queries all servers for their datastore and prints the responses sorted by nodeID
func printDB() {
	var wg sync.WaitGroup
	results := make(map[string]string)
	var resultsMutex sync.Mutex

	activeServersMutex.RLock()
	serverIDs := make([]string, 0, len(allServers))
	for serverID := range allServers {
		serverIDs = append(serverIDs, serverID)
	}
	activeServersMutex.RUnlock()

	wg.Add(len(serverIDs))

	for _, serverID := range serverIDs {
		go func(serverID string) {
			defer wg.Done()
			address, exists := allServers[serverID]
			if !exists {
				log.Printf("Server %s does not exist.", serverID)
				return
			}

			client, err := getRPCClient(address)
			if err != nil {
				log.Printf("Failed to connect to server %s at %s: %v", serverID, address, err)
				return
			}

			var reply string
			err = client.Call("Server.PrintDB", struct{}{}, &reply)
			if err != nil {
				log.Printf("Error calling PrintDB on server %s: %v", serverID, err)
				return
			}

			resultsMutex.Lock()
			results[serverID] = reply
			resultsMutex.Unlock()
		}(serverID)
	}

	wg.Wait()

	// Sort server IDs
	sort.Strings(serverIDs)

	// Print results
	for _, serverID := range serverIDs {
		reply, exists := results[serverID]
		if exists {
			fmt.Printf("Datastore of Server %s:\n%s\n", serverID, reply)
		} else {
			fmt.Printf("No response from Server %s.\n", serverID)
		}
	}
}

// printStatus queries all servers for the status of a given sequence number and prints the responses sorted by ServerID
func printStatus(sequenceNumber int64) {
	var wg sync.WaitGroup
	results := make(map[string]string)
	var resultsMutex sync.Mutex

	// Collect the list of active server IDs
	activeServersMutex.RLock()
	serverIDs := make([]string, 0, len(allServers))
	for serverID := range allServers {
		serverIDs = append(serverIDs, serverID)
	}
	activeServersMutex.RUnlock()

	// Initialize the WaitGroup
	wg.Add(len(serverIDs))

	// Define a mapping from status codes to descriptions
	statusDescriptions := map[string]string{
		"PP": "Pre-prepared",
		"P":  "Prepared",
		"C":  "Committed",
		"E":  "Executed",
		"X":  "No Status",
	}

	// Launch goroutines to query each server concurrently
	for _, serverID := range serverIDs {
		go func(serverID string) {
			defer wg.Done()

			address, exists := allServers[serverID]
			if !exists {
				log.Printf("Server %s does not exist in allServers map.", serverID)
				return
			}

			client, err := getRPCClient(address)
			if err != nil {
				log.Printf("Failed to connect to server %s at %s: %v", serverID, address, err)
				return
			}

			var reply string
			err = client.Call("Server.PrintStatus", sequenceNumber, &reply)
			if err != nil {
				log.Printf("Error calling PrintStatus on server %s: %v", serverID, err)
				return
			}

			// Map the status code to its description
			statusDesc, valid := statusDescriptions[reply]
			if !valid {
				statusDesc = "Unknown Status"
			}

			// Safely store the result
			resultsMutex.Lock()
			results[serverID] = statusDesc
			resultsMutex.Unlock()
		}(serverID)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Sort the server IDs for orderly display
	sort.Strings(serverIDs)

	// Print the results in a tabular format
	fmt.Printf("\nStatus of Sequence Number %d Across All Servers:\n", sequenceNumber)
	fmt.Println("---------------------------------------------------------")
	fmt.Printf("%-10s | %-15s\n", "ServerID", "Status")
	fmt.Println("---------------------------------------------------------")
	for _, serverID := range serverIDs {
		status, exists := results[serverID]
		if exists {
			fmt.Printf("%-10s | %-15s\n", serverID, status)
		} else {
			fmt.Printf("%-10s | %-15s\n", serverID, "No Response/X")
		}
	}
	fmt.Println("---------------------------------------------------------")
}

// printView queries all servers for their NEW-VIEW messages and displays them
func printView() {
	var wg sync.WaitGroup
	results := make(map[string][]string)
	var resultsMutex sync.Mutex

	// Collect the list of server IDs
	serverIDs := make([]string, 0, len(allServers))
	for serverID := range allServers {
		serverIDs = append(serverIDs, serverID)
	}

	wg.Add(len(serverIDs))

	for _, serverID := range serverIDs {
		go func(serverID string) {
			defer wg.Done()
			address, exists := allServers[serverID]
			if !exists {
				log.Printf("Server %s does not exist.", serverID)
				return
			}

			client, err := getRPCClient(address)
			if err != nil {
				log.Printf("Failed to connect to server %s at %s: %v", serverID, address, err)
				return
			}

			var reply []string
			err = client.Call("Server.PrintView", struct{}{}, &reply)
			if err != nil {
				log.Printf("Error calling PrintView on server %s: %v", serverID, err)
				return
			}

			resultsMutex.Lock()
			results[serverID] = reply
			resultsMutex.Unlock()
		}(serverID)
	}

	wg.Wait()

	// Sort server IDs
	sort.Strings(serverIDs)

	// Print results
	for _, serverID := range serverIDs {
		messages, exists := results[serverID]
		if exists {
			fmt.Printf("NEW-VIEW messages of Server %s:\n", serverID)
			for _, msg := range messages {
				fmt.Println(msg)
			}
			fmt.Println()
		} else {
			fmt.Printf("No NEW-VIEW messages from Server %s.\n", serverID)
		}
	}
}

// performance calculates and displays throughput and latency
func performance() {
	performanceMutex.Lock()
	defer performanceMutex.Unlock()

	// Ensure testEndTime is set
	if testEndTime.Before(testStartTime) {
		testEndTime = time.Now()
	}

	totalTime := testEndTime.Sub(testStartTime)
	if totalTime <= 0 {
		fmt.Println("Insufficient data to calculate performance metrics.")
		return
	}

	avgLatency := time.Duration(0)
	if totalTransactions > 0 {
		avgLatency = totalLatency / time.Duration(totalTransactions)
	}

	throughput := float64(totalTransactions) / totalTime.Seconds()

	fmt.Println("\nPerformance Metrics:")
	fmt.Printf("Total Transactions: %d\n", totalTransactions)
	fmt.Printf("Total Time: %.2f seconds\n", totalTime.Seconds())
	fmt.Printf("Throughput: %.2f transactions/second\n", throughput)
	fmt.Printf("Average Latency: %.2f milliseconds\n", avgLatency.Seconds()*1000)
}

// resetSystemState resets the system state on all servers and the client
func resetSystemState() {
	var wg sync.WaitGroup

	activeServersMutex.RLock()
	serverIDs := make([]string, 0, len(activeServers))
	for serverID := range activeServers {
		serverIDs = append(serverIDs, serverID)
	}
	activeServersMutex.RUnlock()

	wg.Add(len(serverIDs))

	for _, serverID := range serverIDs {
		go func(serverID string) {
			defer wg.Done()
			address, exists := allServers[serverID]
			if !exists {
				log.Printf("Server %s does not exist.", serverID)
				return
			}

			client, err := getRPCClient(address)
			if err != nil {
				log.Printf("Failed to connect to server %s at %s: %v", serverID, address, err)
				return
			}

			var reply bool
			err = client.Call("Server.Reset", struct{}{}, &reply)
			if err != nil {
				log.Printf("Error calling Reset on server %s: %v", serverID, err)
				return
			}

			if reply {
				// log.Printf("Server %s reset successfully.", serverID)
			} else {
				log.Printf("Server %s failed to reset.", serverID)
			}
		}(serverID)
	}

	wg.Wait()
	// log.Println("All active servers have been reset.")

	// **Reset Client's View Number and Leader**
	currentViewNumber = 1
	leader = "S1"
	leaderServerAddr = allServers[leader]
	// log.Printf("Client: Reset currentViewNumber to %d and leader to %s.", currentViewNumber, leader)
}

func loadPrivateKeyFromFile(filePath string) (*rsa.PrivateKey, error) {
	keyData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(keyData)
	if block == nil || block.Type != "RSA PRIVATE KEY" {
		return nil, errors.New("failed to decode PEM block containing private key")
	}
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	return privateKey, nil
}

func loadPublicKeyFromFile(filePath string) (*rsa.PublicKey, error) {
	keyData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(keyData)
	if block == nil || (block.Type != "RSA PUBLIC KEY" && block.Type != "PUBLIC KEY") {
		return nil, errors.New("failed to decode PEM block containing public key")
	}
	publicKey, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		log.Printf("Trying x509.ParsePKIXPublicKey for key at %s", filePath)
		parsedKey, err2 := x509.ParsePKIXPublicKey(block.Bytes)
		if err2 != nil {
			log.Printf("Failed to parse public key: %v", err2)
			return nil, err
		}
		if pk, ok := parsedKey.(*rsa.PublicKey); ok {
			return pk, nil
		} else {
			return nil, errors.New("parsed key is not an RSA public key")
		}
	}
	return publicKey, nil
}

// readInputCSVFromURL reads the input CSV file from a URL and returns transactions
func readInputCSVFromURL(url string) []TransactionSet {
	log.Printf("Reading input CSV file from URL %s\n", url)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Failed to fetch CSV file from URL: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Failed to fetch CSV file from URL: %s", resp.Status)
	}

	reader := csv.NewReader(resp.Body)
	var transactionSets []TransactionSet
	var currentSetNumber int
	var currentTransactions []Transaction
	var currentLiveServers []string
	var currentByzServers []string // To store Byzantine servers

	lineNumber := 1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			log.Println("End of CSV file reached.")
			break
		}
		if err != nil {
			log.Fatalf("Error reading CSV at line %d: %v", lineNumber, err)
		}
		if len(record) < 4 { // Expecting 4 fields
			log.Printf("Skipping incomplete record at line %d: %v", lineNumber, record)
			lineNumber++
			continue
		}
		setNumberStr := strings.TrimSpace(record[0])
		transactionStr := strings.TrimSpace(record[1])
		liveServersStr := strings.TrimSpace(record[2])
		byzServersStr := strings.TrimSpace(record[3]) // 4th field for Byzantine servers

		if setNumberStr != "" {
			if currentSetNumber != 0 {
				transactionSets = append(transactionSets, TransactionSet{
					SetNumber:        currentSetNumber,
					Transactions:     currentTransactions,
					LiveServers:      currentLiveServers,
					ByzantineServers: currentByzServers, // Assign Byzantine servers
				})
				currentTransactions = []Transaction{}
			}
			currentSetNumber, _ = strconv.Atoi(setNumberStr)
			currentLiveServers = parseLiveServers(liveServersStr)
			currentByzServers = parseByzantineServers(byzServersStr) // Parse Byzantine servers
		}
		tx := parseTransaction(transactionStr)
		currentTransactions = append(currentTransactions, tx)
		lineNumber++
	}
	if currentSetNumber != 0 && len(currentTransactions) > 0 {
		transactionSets = append(transactionSets, TransactionSet{
			SetNumber:        currentSetNumber,
			Transactions:     currentTransactions,
			LiveServers:      currentLiveServers,
			ByzantineServers: currentByzServers,
		})
	}
	return transactionSets
}

// parseLiveServers parses live servers string into a slice of server IDs
func parseLiveServers(liveServersStr string) []string {
	liveServersStr = strings.Trim(liveServersStr, "[]")
	if liveServersStr == "" {
		log.Println("No live servers specified.")
		return []string{}
	}
	liveServers := strings.Split(liveServersStr, ",")
	for i := range liveServers {
		liveServers[i] = strings.TrimSpace(liveServers[i])
	}
	return liveServers
}

// updateActiveServers updates the activeServers map based on the provided list
func updateActiveServers(liveServers []string) {
	activeServersMutex.Lock()
	defer activeServersMutex.Unlock()
	activeServers = make(map[string]string)
	for _, serverID := range liveServers {
		serverID = strings.TrimSpace(serverID)
		address, exists := allServers[serverID]
		if exists {
			activeServers[serverID] = address
		}
	}
}

// updateByzantineServers updates the byzantineServers map based on the provided list
func updateByzantineServers(byzServers []string) {
	byzantineServersMutex.Lock()
	defer byzantineServersMutex.Unlock()
	byzantineServers = make(map[string]string)
	for _, serverID := range byzServers {
		serverID = strings.TrimSpace(serverID)
		address, exists := allServers[serverID]
		if exists {
			byzantineServers[serverID] = address
		}
	}
}

// parseByzantineServers parses Byzantine servers string into a slice of server IDs
func parseByzantineServers(byzServersStr string) []string {
	byzServersStr = strings.Trim(byzServersStr, "[]")
	if byzServersStr == "" {
		log.Println("No Byzantine servers specified.")
		return []string{}
	}
	byzServers := strings.Split(byzServersStr, ",")
	for i := range byzServers {
		byzServers[i] = strings.TrimSpace(byzServers[i])
	}
	return byzServers
}

// processClientTransactions processes transactions for a client sequentially
func processClientTransactions(clientID string, transactionQueue chan Transaction) {
	for transaction := range transactionQueue {
		sendTransaction(clientID, transaction)
	}
}

func sendTransaction(clientID string, transaction Transaction) {
	client, exists := clients[clientID]
	if !exists {
		log.Printf("Client %s does not exist. Skipping transaction.", clientID)
		return
	}

	timestamp := time.Now()
	startTime := time.Now() // Start time for latency measurement

	signedTx, err := signTransaction(client, transaction, timestamp)
	if err != nil {
		log.Printf("Error signing transaction for Client %s: %v", client.ID, err)
		return
	}

	request := Request{
		Type:        "REQUEST",
		ViewNumber:  currentViewNumber,
		Transaction: transaction,
		Timestamp:   timestamp,
		ClientID:    client.ID,
		Signature:   signedTx,
	}
	// log.Printf("Prepared Request: %+v", request)

	// Create TransactionContext
	txnCtx := &TransactionContext{
		request: request,
		replyCh: make(chan Response, len(activeServers)),
	}

	// Add to pendingTransactions
	key := fmt.Sprintf("%s:%d", client.ID, timestamp.UnixNano())
	pendingTransactionsMutex.Lock()
	pendingTransactions[key] = txnCtx
	pendingTransactionsMutex.Unlock()

	// Initialize a map to track unique node responses to prevent double-counting
	respondedNodes := make(map[string]bool)
	validReplies := 0 // Initialize validReplies

	for {
		// Attempt to send transaction to the leader
		success := attemptTransaction(leaderServerAddr, request)
		if !success {
			// log.Printf("Leader %s failed. Broadcasting transaction to all active servers.", leader)
			broadcastTransaction(request)
		}

		// Start a 1-second timer
		timer := time.NewTimer(2 * time.Second)

	waitLoop:
		for {
			select {
			case response, ok := <-txnCtx.replyCh:
				if !ok {
					// Channel has been closed, abort waiting for this transaction
					// log.Printf("Transaction from %s to %s amount %.2f aborted due to pending transaction clearance.",
					// 	transaction.Sender, transaction.Receiver, transaction.Amount)
					// Remove from pendingTransactions
					pendingTransactionsMutex.Lock()
					delete(pendingTransactions, key)
					pendingTransactionsMutex.Unlock()
					return
				}
				if validateNodeResponse(response, request) {
					// Ensure each node's response is counted only once
					if !respondedNodes[response.NodeID] {
						validReplies++
						respondedNodes[response.NodeID] = true
						// log.Printf("Valid reply %d/%d from Node %s for transaction from %s to %s amount %.2f.",
						// validReplies, f+1, response.NodeID, transaction.Sender, transaction.Receiver, transaction.Amount)
						if validReplies >= f+1 {
							// log.Printf("Transaction from %s to %s amount %.2f received required %d valid replies.",
							// 	transaction.Sender, transaction.Receiver, transaction.Amount, f+1)
							// Remove from pendingTransactions
							pendingTransactionsMutex.Lock()
							delete(pendingTransactions, key)
							pendingTransactionsMutex.Unlock()

							endTime := time.Now()
							latency := endTime.Sub(startTime)

							// Collect performance data
							performanceMutex.Lock()
							totalTransactions++
							totalLatency += latency
							latencyData = append(latencyData, latency)
							performanceMutex.Unlock()

							return
						}
					} else {
						log.Printf("Duplicate response from Node %s ignored.", response.NodeID)
					}
				}
			case <-timer.C:
				if validReplies < f+1 {
					// log.Printf("1 second elapsed without receiving enough replies for transaction from %s to %s amount %.2f. Broadcasting again.",
					// 	transaction.Sender, transaction.Receiver, transaction.Amount)
					broadcastTransaction(request)
				}
				break waitLoop
			}
		}

		// Check if transaction has been executed after the timer
		if validReplies >= f+1 {
			// log.Printf("Going here to the edge case")
			return
		}
	}
}

func attemptTransaction(serverAddress string, request Request) bool {
	client, err := getRPCClient(serverAddress)
	if err != nil {
		log.Printf("Failed to connect to server at %s: %v", serverAddress, err)
		return false
	}

	var reply Reply
	err = client.Call("Server.ReceiveTransaction", request, &reply)
	if err != nil {
		log.Printf("RPC call to server at %s failed: %v", serverAddress, err)
		return false
	}

	// If the server responds with a higher view number, update the client view number and leader
	if reply.ViewNumber > currentViewNumber {
		currentViewNumber = reply.ViewNumber
		leader = getPrimaryID(currentViewNumber)
		leaderServerAddr = allServers[leader]
		// log.Printf("Client: Updated to new view number %d. New leader is %s.", currentViewNumber, leader)
		return false // Indicate that the transaction should be retried with the new leader
	}

	return true
}

// broadcastTransaction sends the transaction to all active servers
func broadcastTransaction(request Request) {
	// log.Printf("Broadcasting transaction to all active servers: %+v", request)
	var wg sync.WaitGroup

	activeServersMutex.RLock()
	defer activeServersMutex.RUnlock()

	for serverID, serverAddress := range activeServers {
		wg.Add(1)
		go func(id, address string) {
			defer wg.Done()
			client, err := getRPCClient(address)
			if err != nil {
				log.Printf("Failed to connect to server %s at %s: %v", id, address, err)
				return
			}
			var reply Reply
			// log.Printf("Sending RPC call to server %s at %s", id, address)
			err = client.Call("Server.ReceiveTransaction", request, &reply)
			if err != nil {
				log.Printf("RPC call to server %s at %s failed: %v", id, address, err)
				return
			}
			// log.Printf("Received initial reply from server %s: %+v", id, reply)
			// Initial response handling if necessary
		}(serverID, serverAddress)
	}
	wg.Wait()
}

func signTransaction(client Client, transaction Transaction, timestamp time.Time) (string, error) {
	data := fmt.Sprintf("<REQUEST,%s,%s,%.2f,%d,%s>",
		transaction.Sender, transaction.Receiver, transaction.Amount,
		timestamp.UnixNano(), client.ID)
	hash := sha256.Sum256([]byte(data))

	signature, err := rsa.SignPKCS1v15(rand.Reader, client.PrivateKey, crypto.SHA256, hash[:])
	if err != nil {
		log.Printf("Error signing transaction: %v", err)
		return "", err
	}
	return base64.StdEncoding.EncodeToString(signature), nil
}

// validateNodeResponse verifies the node's response signature and timestamp
func validateNodeResponse(response Response, request Request) bool {
	// log.Printf("Validating response from Node %s", response.NodeID)
	node, exists := nodes[response.NodeID]
	if !exists {
		log.Printf("Node %s does not exist. Invalid response.", response.NodeID)
		return false
	}
	data := fmt.Sprintf("<REPLY,%d,%d,%s,%s,%s>", response.ViewNumber, response.Timestamp.UnixNano(),
		request.ClientID, response.NodeID, response.Result)
	// log.Printf("Client: Data for signature verification: %s", data)
	hash := sha256.Sum256([]byte(data))

	signature, err := base64.StdEncoding.DecodeString(response.Signature)
	if err != nil {
		log.Printf("Failed to decode signature from Node %s: %v", response.NodeID, err)
		return false
	}
	err = rsa.VerifyPKCS1v15(node.PublicKey, crypto.SHA256, hash[:], signature)
	if err != nil {
		log.Printf("Signature verification failed for Node %s: %v; clientID: %s", response.NodeID, err, response.ClientID)
		return false
	}
	if response.Timestamp.UnixNano() != request.Timestamp.UnixNano() {
		log.Printf("Timestamp mismatch for response from Node %s", response.NodeID)
		return false
	}
	// log.Printf("Response from Node %s is valid", response.NodeID)
	return true
}

// parseTransaction parses a transaction string into a Transaction struct
func parseTransaction(transactionStr string) Transaction {
	txParts := strings.Split(strings.Trim(transactionStr, "()"), ",")
	if len(txParts) < 3 {
		log.Printf("Invalid transaction format: %s", transactionStr)
		return Transaction{}
	}
	amount, err := strconv.ParseFloat(strings.TrimSpace(txParts[2]), 64)
	if err != nil {
		log.Printf("Invalid amount in transaction: %s", txParts[2])
		amount = 0.0
	}
	tx := Transaction{
		Sender:    strings.TrimSpace(txParts[0]),
		Receiver:  strings.TrimSpace(txParts[1]),
		Amount:    amount,
		Timestamp: time.Now(),
	}
	return tx
}
func resetPerformanceMetrics() {
	performanceMutex.Lock()
	defer performanceMutex.Unlock()
	totalTransactions = 0
	totalLatency = 0
	latencyData = []time.Duration{}
	testStartTime = time.Now()
	testEndTime = testStartTime
}

// getRPCClient creates or retrieves an RPC client
func getRPCClient(serverAddress string) (*rpc.Client, error) {
	rpcClientsMutex.Lock()
	defer rpcClientsMutex.Unlock()

	if client, exists := rpcClients[serverAddress]; exists {
		return client, nil
	}
	// log.Printf("Creating new RPC client for address %s", serverAddress)
	client, err := rpc.Dial("tcp", serverAddress)
	if err != nil {
		log.Printf("Failed to dial RPC server at %s: %v", serverAddress, err)
		return nil, err
	}
	rpcClients[serverAddress] = client
	return client, nil
}

// clearPendingTransactions safely clears all pending transactions
func clearPendingTransactions() {
	pendingTransactionsMutex.Lock()
	defer pendingTransactionsMutex.Unlock()

	// Close all reply channels to unblock any waiting goroutines
	for key, txnCtx := range pendingTransactions {
		close(txnCtx.replyCh)
		delete(pendingTransactions, key)
	}

	// log.Println("Cleared all pending transactions.")
}

// sendStatusUpdate sends the active and Byzantine server statuses to all servers
func sendStatusUpdate() {
	// Prepare the status update
	activeServersMutex.RLock()
	activeList := make([]string, 0, len(activeServers))
	for serverID := range activeServers {
		activeList = append(activeList, serverID)
	}
	activeServersMutex.RUnlock()

	byzantineServersMutex.RLock()
	byzList := make([]string, 0, len(byzantineServers))
	for serverID := range byzantineServers {
		byzList = append(byzList, serverID)
	}
	byzantineServersMutex.RUnlock()

	// Log the status update being sent
	// log.Printf("Sending StatusUpdate: ActiveServers: %v, ByzantineServers: %v", activeList, byzList)

	statusUpdate := StatusUpdate{
		ActiveServers:    activeList,
		ByzantineServers: byzList,
	}

	// Send the status update to all servers
	var wg sync.WaitGroup

	wg.Add(len(allServers))

	for serverID, serverAddress := range allServers {
		go func(id, address string) {
			defer wg.Done()
			client, err := getRPCClient(address)
			if err != nil {
				log.Printf("Failed to connect to server %s at %s: %v", id, address, err)
				return
			}

			var reply bool
			// log.Printf("Client: Sending StatusUpdate to server %s at %s: ActiveServers: %v, ByzantineServers: %v",
			// id, address, statusUpdate.ActiveServers, statusUpdate.ByzantineServers)
			err = client.Call("Server.UpdateStatus", statusUpdate, &reply)
			if err != nil {
				log.Printf("Error calling UpdateStatus on server %s: %v", id, err)
				return
			}
			if reply {
				// log.Printf("Server %s acknowledged status update.", id)
			} else {
				log.Printf("Server %s failed to acknowledge status update.", id)
			}
		}(serverID, serverAddress)
	}

	wg.Wait()
	// log.Println("Completed sending StatusUpdates to all servers.")
}
