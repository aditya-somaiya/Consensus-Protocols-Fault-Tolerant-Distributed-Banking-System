package main

import (
	"bufio"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
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
	ViewNumber  int
	Transaction Transaction
	Timestamp   time.Time
	ClientID    string
	// Removed Signature as signing is no longer needed
}

// Response is the structure for receiving responses from nodes
type Response struct {
	ViewNumber int
	Timestamp  time.Time
	ClientID   string
	NodeID     string
	Result     string
	// Removed Signature as validation is no longer performed
}

// Reply is the structure for replies from the server
type Reply struct {
	Type       string
	ViewNumber int
	Timestamp  time.Time
	ClientID   string
	NodeID     string
	Result     string
	// Removed Signature as validation is no longer performed
}

// Client represents a client without a public-private key pair
type Client struct {
	ID string
	// Removed PrivateKey and PublicKey as they are no longer needed
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

// Node represents a server node
type Node struct {
	ID string
	// Removed PublicKey as it's no longer needed
}

type DatastoreRecord struct {
	Transaction Transaction
	Type        string
}

func init() {
	gob.Register(Transaction{})
	gob.Register(Reply{})
	gob.Register(Response{})
	gob.Register(Request{})
}

// Global Variables
var (
	currentViewNumber     int = 1
	f                         = 1 // Assuming f=1 for simplicity; adjust as needed
	leader                    = "S1"
	leaderServerAddr      string
	allServers            = make(map[string]string)
	activeServers         = make(map[string]string)
	byzantineServers      = make(map[string]string)
	activeServersMutex    sync.RWMutex
	byzantineServersMutex sync.RWMutex
	rpcClientsMutex       sync.Mutex
	rpcClients            = make(map[string]*rpc.Client)
	clients               = make(map[string]Client)
	// Removed nodes map as public keys are no longer used
	pendingTransactions      = make(map[string]*TransactionContext)
	pendingTransactionsMutex sync.Mutex
	totalTransactions        int
	totalLatency             time.Duration
	latencyData              []time.Duration
	performanceMutex         sync.Mutex
	testStartTime            time.Time
	testEndTime              time.Time
)

// TransactionContext holds the request and reply channel
type TransactionContext struct {
	request Request
	replyCh chan Response
}

// LoadServerPublicKeys removes the loading of public keys as it's no longer needed
func LoadServerPublicKeys() {
	// No longer loading public keys
}

// Removed loadPublicKeyFromFile and parsePublicKey functions as they are no longer needed

func init() {
	// Initialize allServers map with server IDs and their addresses
	basePort := 1231
	for i := 1; i <= 12; i++ {
		serverID := fmt.Sprintf("S%d", i)
		address := fmt.Sprintf("localhost:%d", basePort+i-1)
		allServers[serverID] = address
	}
	leader = getPrimaryID(currentViewNumber)
	leaderServerAddr = allServers[leader]

	// Dynamically generate client IDs from C1 to C3000
	for i := 1; i <= 3000; i++ {
		clientID := fmt.Sprintf("%d", i)
		clients[clientID] = Client{ID: clientID}
	}

	// Removed loading servers' public keys as signature validation is disabled
}

// getPrimaryID determines the primary server based on the current view number
func getPrimaryID(viewNumber int) string {
	totalServers := len(allServers)
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
	gob.Register(Response{})
	gob.Register(Request{})

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
	transactionSets := readInputCSV("../lab_test_cross.csv")

	resetSystemState()
	time.Sleep(50 * time.Millisecond)

	for _, tSet := range transactionSets {

		// Clear all pending transactions before new set
		clearPendingTransactions()

		updateActiveServers(tSet.LiveServers)
		updateByzantineServers(tSet.ByzantineServers)
		log.Printf("Processing Transaction Set %d", tSet.SetNumber)

		// Send status update to active servers
		sendStatusUpdate()
		time.Sleep(5 * time.Millisecond)

		var setWG sync.WaitGroup

		// Iterate through transactions and send them
		for _, transaction := range tSet.Transactions {
			setWG.Add(1)
			go func(tx Transaction) {
				defer setWG.Done()
				sendTransaction(tx.Sender, tx) // Sending the transaction
			}(transaction)

			time.Sleep(2 * time.Millisecond)
		}

		presentOptions()
		// resetSystemState()
	}

	// Once all sets are processed
	log.Println("All transactions processed.")
}

// presentOptions provides interactive options after processing a transaction set
func presentOptions() {
	time.Sleep(2 * time.Second)
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("\nSelect an option:")
		fmt.Println("1. PrintDataStore")
		fmt.Println("2. PrintDB")
		fmt.Println("3.Continue to next transaction set")

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
			printDataStore()
		case 2:
			printDB()
		case 3:
			return
		default:
			fmt.Println("Invalid choice. Please try again.")
		}
	}
}

// printDB queries all servers for their datastore and prints the responses sorted by nodeID
// printDB prompts for a ClientID and prints the balance of that client on all servers in its cluster
func printDB() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter Client ID to view balances: ")
	clientID, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading Client ID. Please try again.")
		return
	}
	clientID = strings.TrimSpace(clientID)
	if clientID == "" {
		fmt.Println("Client ID cannot be empty.")
		return
	}

	cluster := getClusterForClient(clientID)
	if cluster == -1 {
		fmt.Printf("Client ID %s is out of valid range.\n", clientID)
		return
	}

	servers := getServersOfCluster(cluster)
	if len(servers) == 0 {
		fmt.Printf("No servers found for cluster %d.\n", cluster)
		return
	}

	sort.Strings(servers) // Sort servers for ordered output

	fmt.Printf("\nBalances for Client %s in Cluster %d:\n", clientID, cluster)
	fmt.Println("----------------------------------------")

	for _, serverID := range servers {
		address, exists := allServers[serverID]
		if !exists {
			fmt.Printf("Server %s does not exist.\n", serverID)
			continue
		}

		client, err := getRPCClient(address)
		if err != nil {
			fmt.Printf("Failed to connect to Server %s at %s: %v\n", serverID, address, err)
			continue
		}

		var dbReply string
		err = client.Call("Server.PrintDB", struct{}{}, &dbReply)
		if err != nil {
			fmt.Printf("Error calling PrintDB on Server %s: %v\n", serverID, err)
			continue
		}

		// Parse the reply to find the balance of the specified client
		balance := parseBalanceFromDBReply(dbReply, clientID)
		if balance == -1 {
			fmt.Printf("Client %s not found on Server %s.\n", clientID, serverID)
		} else {
			fmt.Printf("Server %s: Client %s Balance: %.2f\n", serverID, clientID, balance)
		}

		time.Sleep(100 * time.Millisecond) // Small sleep to ensure orderly replies
	}

	fmt.Println("----------------------------------------")
}

// getClusterForClient determines the cluster number based on ClientID
func getClusterForClient(clientID string) int {
	num, err := strconv.Atoi(clientID)
	if err != nil {
		return -1
	}

	switch {
	case num >= 1 && num <= 1000:
		return 1
	case num >= 1001 && num <= 2000:
		return 2
	case num >= 2001 && num <= 3000:
		return 3
	default:
		return -1
	}
}

// getServersOfCluster retrieves the list of server IDs belonging to a specific cluster
func getServersOfCluster(cluster int) []string {
	switch cluster {
	case 1:
		return []string{"S1", "S2", "S3", "S4"}
	case 2:
		return []string{"S5", "S6", "S7", "S8"}
	case 3:
		return []string{"S9", "S10", "S11", "S12"}
	default:
		return []string{}
	}
}

// parseBalanceFromDBReply extracts the balance of a specific client from the server's PrintDB reply
// Returns -1 if the client is not found
func parseBalanceFromDBReply(dbReply, clientID string) float64 {
	lines := strings.Split(dbReply, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "Client "+clientID+":") {
			parts := strings.Split(line, ":")
			if len(parts) != 2 {
				continue
			}
			balanceStr := strings.TrimSpace(parts[1])
			balance, err := strconv.ParseFloat(balanceStr, 64)
			if err != nil {
				continue
			}
			return balance
		}
	}
	return -1
}

// printDataStore queries all servers for their datastore and prints them in sorted order
func printDataStore() {
	serverIDs := make([]string, 0, len(allServers))
	for serverID := range allServers {
		serverIDs = append(serverIDs, serverID)
	}
	sort.Strings(serverIDs) // Sort servers for ordered output

	fmt.Println("\nDatastore Entries from All Servers:")
	fmt.Println("====================================")

	counterMap := make(map[string]int) // To keep track of counters per cluster

	for _, serverID := range serverIDs {
		address, exists := allServers[serverID]
		if !exists {
			fmt.Printf("Server %s does not exist.\n", serverID)
			continue
		}

		client, err := getRPCClient(address)
		if err != nil {
			fmt.Printf("Failed to connect to Server %s at %s: %v\n", serverID, address, err)
			continue
		}

		var datastore []DatastoreRecord
		err = client.Call("Server.GetDatastore", struct{}{}, &datastore)
		if err != nil {
			fmt.Printf("Error calling GetDatastore on Server %s: %v\n", serverID, err)
			continue
		}

		cluster := getClusterFromServerID(serverID)
		if cluster == -1 {
			fmt.Printf("Invalid cluster for Server %s.\n", serverID)
			continue
		}

		fmt.Printf("\nDatastore of Server %s (Cluster %d):\n", serverID, cluster)
		fmt.Println("----------------------------------------")

		counterMap[serverID] = 1
		for _, record := range datastore {
			fmt.Printf("<%d, %d> %s: Sender=%s, Receiver=%s, Amount=%.2f, Type=%s\n",
				counterMap[serverID],
				cluster,
				"Transaction",
				record.Transaction.Sender,
				record.Transaction.Receiver,
				record.Transaction.Amount,
				record.Type)
			counterMap[serverID]++
		}

		if len(datastore) == 0 {
			fmt.Println("No transactions recorded.")
		}

		fmt.Println("----------------------------------------")
		time.Sleep(100 * time.Millisecond) // Small sleep to ensure orderly replies
	}

	fmt.Println("====================================\n")
}

// getClusterFromServerID determines the cluster number based on ServerID
func getClusterFromServerID(serverID string) int {
	switch serverID {
	case "S1", "S2", "S3", "S4":
		return 1
	case "S5", "S6", "S7", "S8":
		return 2
	case "S9", "S10", "S11", "S12":
		return 3
	default:
		return -1
	}
}

// resetSystemState resets the client-side state
func resetSystemState() {
	var wg sync.WaitGroup
	var failedServers []string
	var failedMutex sync.Mutex

	// Collect the list of all server IDs (not just active servers)
	serverIDs := make([]string, 0, len(allServers))
	for serverID := range allServers {
		serverIDs = append(serverIDs, serverID)
	}

	// Add all servers to the wait group
	wg.Add(len(serverIDs))

	for _, serverID := range serverIDs {
		go func(serverID string) {
			defer wg.Done()
			address, exists := allServers[serverID]
			if !exists {
				log.Printf("Server %s does not exist in the allServers map.", serverID)
				failedMutex.Lock()
				failedServers = append(failedServers, serverID)
				failedMutex.Unlock()
				return
			}

			client, err := getRPCClient(address)
			if err != nil {
				log.Printf("Failed to connect to server %s at %s: %v", serverID, address, err)
				failedMutex.Lock()
				failedServers = append(failedServers, serverID)
				failedMutex.Unlock()
				return
			}

			var reply bool
			err = client.Call("Server.Reset", struct{}{}, &reply)
			if err != nil || !reply {
				log.Printf("Error resetting server %s: %v", serverID, err)
				failedMutex.Lock()
				failedServers = append(failedServers, serverID)
				failedMutex.Unlock()
				return
			}

			// log.Printf("Server %s reset successfully.", serverID)
		}(serverID)
	}

	// Wait for all servers to complete the reset process
	wg.Wait()

	// Handle failed resets
	if len(failedServers) > 0 {
		log.Printf("The following servers failed to reset: %v", failedServers)
	} else {
		// log.Println("All servers (active and inactive) have been successfully reset.")
	}

	// Reset client-side state
	currentViewNumber = 1
	leader = getPrimaryID(currentViewNumber)
	leaderServerAddr = allServers[leader]
	// log.Printf("Client: Reset currentViewNumber to %d and leader to %s.", currentViewNumber, leader)
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

// sendTransaction sends a transaction to the appropriate leader based on sender's client ID
func sendTransaction(clientID string, transaction Transaction) {
	// Validate client exists
	client, exists := clients[clientID]
	if !exists {
		log.Printf("Client %s does not exist. Skipping transaction.", clientID)
		return
	}

	timestamp := time.Now()
	startTime := time.Now()

	leaderAddress := getLeaderAddress(client.ID)
	if leaderAddress == "" {
		log.Printf("No leader found for client %s. Skipping transaction.", client.ID)
		return
	}

	request := Request{
		Type:        "REQUEST",
		ViewNumber:  currentViewNumber,
		Transaction: transaction,
		Timestamp:   timestamp,
		ClientID:    client.ID,
	}

	txnCtx := &TransactionContext{
		request: request,
		replyCh: make(chan Response, len(activeServers)),
	}

	key := fmt.Sprintf("%s:%d", client.ID, timestamp.UnixNano())
	pendingTransactionsMutex.Lock()
	pendingTransactions[key] = txnCtx
	pendingTransactionsMutex.Unlock()

	respondedNodes := make(map[string]bool)
	validReplies := 0

	isCrossShard := transaction.Sender != transaction.Receiver
	requiredReplies := 1 // For cross-shard transactions, only one reply is needed
	if !isCrossShard {
		requiredReplies = f + 1 // For same-shard transactions, require f + 1 replies
	}

	for {
		success := attemptTransaction(leaderAddress, request)
		if !success {
			// Retry sending to the leader
		}

		timer := time.NewTimer(9000 * time.Millisecond)

	waitLoop:
		for {
			select {
			case response, ok := <-txnCtx.replyCh:
				if !ok {
					pendingTransactionsMutex.Lock()
					delete(pendingTransactions, key)
					pendingTransactionsMutex.Unlock()
					return
				}
				if !respondedNodes[response.NodeID] {
					validReplies++
					respondedNodes[response.NodeID] = true
					if validReplies >= requiredReplies {
						pendingTransactionsMutex.Lock()
						delete(pendingTransactions, key)
						pendingTransactionsMutex.Unlock()

						endTime := time.Now()
						latency := endTime.Sub(startTime)

						performanceMutex.Lock()
						totalTransactions++
						totalLatency += latency
						latencyData = append(latencyData, latency)
						performanceMutex.Unlock()

						return
					}
				}
			case <-timer.C:
				if validReplies < requiredReplies {
					// Retry sending to the leader
					success = attemptTransaction(leaderAddress, request)
					if !success {
						// Optionally, log the failure to send to the leader
					}
				}
				break waitLoop
			}
		}

		if validReplies >= requiredReplies {
			return
		}
	}
}

// attemptTransaction attempts to send the transaction to the leader
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
		log.Printf("Client: Updated to new view number %d. New leader is %s.", currentViewNumber, leader)
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
			// Initial response handling if necessary
		}(serverID, serverAddress)
	}
	wg.Wait()
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

// ReceiveReply is the RPC method that servers call to send replies to the client
func (c *ClientRPC) ReceiveReply(reply Reply, ack *bool) error {
	*ack = true
	key := fmt.Sprintf("%s:%d", reply.ClientID, reply.Timestamp.UnixNano())
	log.Printf("Received Reply: Key=%s, Reply timestamp: %d", key, reply.Timestamp.UnixNano())
	pendingTransactionsMutex.Lock()
	txnCtx, exists := pendingTransactions[key]
	pendingTransactionsMutex.Unlock()
	if !exists {
		log.Printf("No pending transaction found for key %s", key)
		return nil
	}

	response := Response{
		ViewNumber: reply.ViewNumber,
		Timestamp:  reply.Timestamp,
		ClientID:   reply.ClientID,
		NodeID:     reply.NodeID,
		Result:     reply.Result,
		// Signature is removed
	}

	txnCtx.replyCh <- response
	return nil
}

// getLeaderAddress determines the leader's address based on the clientID
func getLeaderAddress(clientID string) string {
	num, err := strconv.Atoi(clientID)
	log.Printf("Client ID: %s, num: %d", clientID, num)
	if err != nil {
		log.Printf("Invalid client ID format: %s", clientID)
		return ""
	}

	switch {
	case num >= 1 && num <= 1000:
		return allServers["S1"]
	case num >= 1001 && num <= 2000:
		return allServers["S5"]
	case num >= 2001 && num <= 3000:
		return allServers["S9"]
	default:
		log.Printf("Client ID %s out of range.", clientID)
		return ""
	}
}
