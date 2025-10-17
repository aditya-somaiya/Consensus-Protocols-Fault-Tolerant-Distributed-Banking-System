package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid" // Imported UUID package
)

// Constants for configuration
const (
	DATA_ITEMS_PER_SHARD = 1000
	NUM_SERVERS_TOTAL    = 9 // Total number of servers across all clusters
)

// sortedServers defines the order in which servers should be displayed
var sortedServers = []string{"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9"}

// Transaction represents a banking transaction
type Transaction struct {
	ID  string // Unique identifier for the transaction
	X   int    // Sender
	Y   int    // Receiver
	Amt int    // Amount to transfer
}

// ServerResponse represents the response from a server
type ServerResponse struct {
	Status string // "prepared", "committed", "aborted"
}

// CommitCrossShardArgs represents the arguments for the CommitCrossShardTransaction RPC call
type CommitCrossShardArgs struct {
	TransactionID string // Unique Transaction ID
	IsForwarded   bool
}

// CommitCrossShardReply represents the reply for the CommitCrossShardTransaction RPC call
type CommitCrossShardReply struct {
	Success bool // Indicates if the commit was successful
}

var DesignatedServers map[int]string

// AbortCrossShardArgs represents the arguments for the AbortCrossShardTransaction RPC call
type AbortCrossShardArgs struct {
	TransactionID string // Unique Transaction ID
	IsForwarded   bool
}

// AbortCrossShardReply represents the reply for the AbortCrossShardTransaction RPC call
type AbortCrossShardReply struct {
	Success bool // Indicates if the abort was successful
}

// TransactionSet represents a set of transactions with active servers
type TransactionSet struct {
	SetNumber         int
	Transactions      []Transaction
	ActiveServers     []string // e.g., ["S1", "S2", ...]
	DesignatedServers []string
}

// SetActiveArgs represents the arguments for the SetActive RPC call
type SetActiveArgs struct {
	Active bool // Indicates if the server should be active or inactive
}

// SetActiveReply represents the reply for the SetActive RPC call (empty in this case)
type SetActiveReply struct{}

// ServerAddressMap maps server labels to their addresses
var ServerAddressMap = map[string]string{
	"S1": "localhost:5001",
	"S2": "localhost:5002",
	"S3": "localhost:5003",
	"S4": "localhost:5004",
	"S5": "localhost:5005",
	"S6": "localhost:5006",
	"S7": "localhost:5007",
	"S8": "localhost:5008",
	"S9": "localhost:5009",
}

// ClusterServers represents the current active servers per cluster
// Each inner slice contains server labels sorted by server ID
var ClusterServers = [][]string{
	{}, // Cluster 0
	{}, // Cluster 1
	{}, // Cluster 2
}

// Performance metrics
var (
	totalTransactions int
	totalLatency      time.Duration
	mu                sync.Mutex
)

func init() {
	// Configure the logger to include timestamps
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	// log.Println("Client initialization started.")
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Initialize DesignatedServers
	DesignatedServers = make(map[int]string)

	// Read transaction sets from the input file
	transactionSets, err := readTransactionSets("../test_cases.csv")
	if err != nil {
		log.Fatalf("Error reading transaction sets: %v", err)
	}
	// log.Printf("Successfully read %d transaction sets.", len(transactionSets))

	reader := bufio.NewReader(os.Stdin)

	for _, set := range transactionSets {
		// log.Printf("Processing Set #%d", set.SetNumber)

		// Update active servers
		updateActiveServers(set.ActiveServers)
		// log.Printf("Active servers for Set #%d: %v", set.SetNumber, set.ActiveServers)

		updateDesignatedServers(set.DesignatedServers)

		// Process each transaction in the set concurrently
		var wg sync.WaitGroup
		for idx, tx := range set.Transactions {
			wg.Add(1)
			go func(idx int, tx Transaction) {
				defer wg.Done()
				// log.Printf("Processing transaction #%d: ID=%s, %+v", idx+1, tx.ID, tx)
				startTime := time.Now()
				processTransaction(tx)
				endTime := time.Now()

				// Update performance metrics
				mu.Lock()
				totalTransactions++
				totalLatency += endTime.Sub(startTime)
				mu.Unlock()
				// log.Printf("Transaction #%d processed in %v. ID=%s", idx+1, endTime.Sub(startTime), tx.ID)
			}(idx, tx)

			// **First Change: Introduce a small delay between dispatching transactions**
			time.Sleep(1 * time.Millisecond) // Added delay to ensure ordered dispatch
		}

		// Wait for all transactions to be processed
		wg.Wait()
		// log.Printf("All transactions in Set #%d have been processed.", set.SetNumber)

		// Prompt user for next action
		for {
			fmt.Println("\nChoose an action:")
			fmt.Println("1. Proceed to the next set")
			fmt.Println("2. Print balance of client ID(s)")
			fmt.Println("3. Print performance metrics")
			fmt.Println("4. Print committed transactions logs")
			fmt.Println("5. Exit")
			fmt.Print("Enter choice (1-5) [Press Enter to proceed]: ") // Updated prompt to indicate default action

			choice, _ := reader.ReadString('\n')
			choice = strings.TrimSpace(choice)

			// **Second Change: Allow pressing Enter to proceed to the next set**
			if choice == "" {
				choice = "1"
			}

			switch choice {
			case "1":
				goto NextSet
			case "2":
				// Allow multiple client IDs separated by spaces
				fmt.Print("Enter Client ID(s) to print balance (separated by spaces): ")
				clientIDsStr, _ := reader.ReadString('\n')
				clientIDsStr = strings.TrimSpace(clientIDsStr)
				if clientIDsStr == "" {
					fmt.Println("No Client IDs entered. Please try again.")
					continue
				}
				// Split the input by spaces
				clientIDParts := strings.Fields(clientIDsStr)
				var clientIDs []int
				var invalidInputs []string
				for _, part := range clientIDParts {
					clientID, err := strconv.Atoi(part)
					if err != nil {
						invalidInputs = append(invalidInputs, part)
						continue
					}
					clientIDs = append(clientIDs, clientID)
				}
				if len(invalidInputs) > 0 {
					fmt.Printf("Invalid Client ID(s) skipped: %s\n", strings.Join(invalidInputs, ", "))
				}
				if len(clientIDs) == 0 {
					fmt.Println("No valid Client IDs to process. Please try again.")
					continue
				}
				PrintBalances(clientIDs)
			case "3":
				PrintPerformance()
			case "4":
				PrintDatastore()
			case "5":
				fmt.Println("Exiting the client.")
				os.Exit(0)
			default:
				fmt.Println("Invalid choice. Please enter a number between 1 and 5.")
			}
		}
	NextSet:
		continue
	}

	// After all sets are processed, print final performance metrics
	// log.Println("All transaction sets have been processed.")
	// PrintPerformance()
	// log.Println("Main function completed.")
}

func updateDesignatedServers(designatedServerLabels []string) {
	// log.Println("Updating designated servers based on the current transaction set.")
	mu.Lock()
	defer mu.Unlock()
	DesignatedServers = make(map[int]string)
	for _, label := range designatedServerLabels {
		cluster := getClusterForServerLabel(label)
		if cluster == -1 {
			log.Printf("Invalid cluster for server label %s. Skipping.", label)
			continue
		}
		DesignatedServers[cluster] = label
	}
}

// readTransactionSets reads transaction sets from a CSV file
func readTransactionSets(filename string) ([]TransactionSet, error) {
	// log.Printf("Reading transaction sets from file: %s", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Failed to open file %s: %v", filename, err)
		return nil, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Error closing file %s: %v", filename, err)
		} else {
			// log.Printf("File %s closed successfully.", filename)
		}
	}()

	var transactionSets []TransactionSet
	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true
	records, err := reader.ReadAll()
	if err != nil {
		log.Printf("Error reading CSV records from file %s: %v", filename, err)
		return nil, err
	}
	// log.Printf("Total records read from CSV: %d", len(records))

	var currentSet *TransactionSet

	for i, record := range records {
		// Skip empty records
		if len(record) == 0 {
			continue
		}

		// Check if the first field is set number
		if record[0] != "" {
			// Start of a new set
			if len(record) < 4 {
				log.Printf("Skipping invalid set header at record #%d: %v", i+1, record)
				continue
			}
			setNumber, err := strconv.Atoi(record[0])
			if err != nil {
				log.Printf("Invalid set number at record #%d: %v", i+1, record[0])
				continue
			}

			// Parse transactions
			txStr := record[1]
			tx, err := parseTransactionString(txStr)
			if err != nil {
				log.Printf("Error parsing transaction at record #%d: %v", i+1, err)
				continue
			}

			// Parse active servers
			activeServersStr := record[2]
			activeServers, err := parseActiveServers(activeServersStr)
			if err != nil {
				log.Printf("Error parsing active servers at record #%d: %v", i+1, err)
				continue
			}

			// Parse designated servers
			designatedServersStr := record[3]
			designatedServers, err := parseDesignatedServers(designatedServersStr)
			if err != nil {
				log.Printf("Error parsing designated servers at record #%d: %v", i+1, err)
				continue
			}

			// Initialize a new TransactionSet
			currentSet = &TransactionSet{
				SetNumber:         setNumber,
				Transactions:      []Transaction{tx},
				ActiveServers:     activeServers,
				DesignatedServers: designatedServers,
			}
			transactionSets = append(transactionSets, *currentSet)
			// log.Printf("Initialized Set #%d with first transaction (ID=%s) and active servers: %v", setNumber, tx.ID, activeServers)
		} else {
			// Continuation of the current set
			if currentSet == nil {
				log.Printf("Skipping transaction without a set at record #%d: %v", i+1, record)
				continue
			}
			if len(record) < 2 || record[1] == "" {
				log.Printf("Skipping invalid transaction at record #%d: %v", i+1, record)
				continue
			}
			txStr := record[1]
			tx, err := parseTransactionString(txStr)
			if err != nil {
				log.Printf("Error parsing transaction at record #%d: %v", i+1, err)
				continue
			}
			currentSet.Transactions = append(currentSet.Transactions, tx)
			transactionSets[len(transactionSets)-1] = *currentSet
			// log.Printf("Added transaction to Set #%d: ID=%s, %+v", currentSet.SetNumber, tx.ID, tx)
		}
	}

	// log.Printf("Total valid transaction sets parsed: %d", len(transactionSets))
	return transactionSets, nil
}

func parseDesignatedServers(serversStr string) ([]string, error) {
	serversStr = strings.TrimSpace(serversStr)
	serversStr = strings.TrimPrefix(serversStr, "[")
	serversStr = strings.TrimSuffix(serversStr, "]")
	serverLabels := strings.Split(serversStr, ",")
	var designatedServers []string
	for _, label := range serverLabels {
		label = strings.TrimSpace(label)
		if _, exists := ServerAddressMap[label]; exists {
			designatedServers = append(designatedServers, label)
		} else {
			log.Printf("Unknown server label: %s. Skipping.", label)
		}
	}
	return designatedServers, nil
}

// parseTransactionString parses a transaction string in the format "(X,Y,Amt)"
func parseTransactionString(txStr string) (Transaction, error) {
	txStr = strings.TrimSpace(txStr)
	txStr = strings.TrimPrefix(txStr, "(")
	txStr = strings.TrimSuffix(txStr, ")")
	parts := strings.Split(txStr, ",")
	if len(parts) != 3 {
		return Transaction{}, fmt.Errorf("invalid transaction format: %s", txStr)
	}
	x, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return Transaction{}, fmt.Errorf("invalid sender ID: %s", parts[0])
	}
	y, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return Transaction{}, fmt.Errorf("invalid receiver ID: %s", parts[1])
	}
	amt, err := strconv.Atoi(strings.TrimSpace(parts[2]))
	if err != nil {
		return Transaction{}, fmt.Errorf("invalid amount: %s", parts[2])
	}

	// Generate UUID for the transaction
	uuid := uuid.New().String()

	return Transaction{ID: uuid, X: x, Y: y, Amt: amt}, nil
}

// parseActiveServers parses a server list string in the format "[S1,S2,...]"
func parseActiveServers(serversStr string) ([]string, error) {
	serversStr = strings.TrimSpace(serversStr)
	serversStr = strings.TrimPrefix(serversStr, "[")
	serversStr = strings.TrimSuffix(serversStr, "]")
	serverLabels := strings.Split(serversStr, ",")
	var activeServers []string
	for _, label := range serverLabels {
		label = strings.TrimSpace(label)
		if _, exists := ServerAddressMap[label]; exists {
			activeServers = append(activeServers, label)
		} else {
			log.Printf("Unknown server label: %s. Skipping.", label)
		}
	}
	return activeServers, nil
}

// updateActiveServers updates the ClusterServers based on the active server labels
// and sets their active statuses via SetActive RPCs
func updateActiveServers(activeServerLabels []string) {
	// log.Println("Updating active server statuses based on the current transaction set.")

	// Determine which servers should be active
	activeServersMap := make(map[string]bool)
	for _, label := range activeServerLabels {
		activeServersMap[label] = true
	}

	// Iterate over all servers and set their active status accordingly
	var wg sync.WaitGroup
	for label, addr := range ServerAddressMap {
		wg.Add(1)
		go func(label, addr string) {
			defer wg.Done()
			// Set active if in activeServerLabels, else set inactive
			shouldBeActive := activeServersMap[label]
			SetServerActiveStatus(label, shouldBeActive)

			// Update ClusterServers variable for client-side use
			cluster := getClusterForServerLabel(label)
			if cluster == -1 {
				log.Printf("Invalid cluster for server label %s. Skipping ClusterServers update.", label)
				return
			}
			if shouldBeActive {
				// Append to ClusterServers[cluster] if not already present
				mu.Lock()
				if !contains(ClusterServers[cluster], label) {
					ClusterServers[cluster] = append(ClusterServers[cluster], label)
				}
				mu.Unlock()
			} else {
				// Remove from ClusterServers[cluster] if present
				mu.Lock()
				ClusterServers[cluster] = removeFromSlice(ClusterServers[cluster], label)
				mu.Unlock()
			}
		}(label, addr)
	}
	wg.Wait()
	// log.Println("Completed updating active server statuses.")

	// Sort each cluster's servers by server ID to ensure smallest server ID is first
	for cluster := 0; cluster < len(ClusterServers); cluster++ {
		ClusterServers[cluster] = sortServerLabels(ClusterServers[cluster])
	}
}

// contains checks if a slice contains a specific string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// removeFromSlice removes a specific string from a slice
func removeFromSlice(slice []string, item string) []string {
	newSlice := []string{}
	for _, s := range slice {
		if s != item {
			newSlice = append(newSlice, s)
		}
	}
	return newSlice
}

// sortServerLabels sorts server labels in ascending order based on server ID
func sortServerLabels(servers []string) []string {
	sorted := make([]string, len(servers))
	copy(sorted, servers)
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			idI := getServerID(sorted[i])
			idJ := getServerID(sorted[j])
			if idI > idJ {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	return sorted
}

// getServerID extracts the numerical part of the server label
func getServerID(label string) int {
	id, err := strconv.Atoi(strings.TrimPrefix(label, "S"))
	if err != nil {
		return -1
	}
	return id
}

// getClusterForServerLabel returns the cluster index for a given server label
func getClusterForServerLabel(label string) int {
	// Assuming servers S1-S3 are in Cluster 0, S4-S6 in Cluster 1, S7-S9 in Cluster 2
	serverNum, err := strconv.Atoi(strings.TrimPrefix(label, "S"))
	if err != nil || serverNum < 1 || serverNum > NUM_SERVERS_TOTAL {
		return -1
	}
	if serverNum >= 1 && serverNum <= 3 {
		return 0
	} else if serverNum >= 4 && serverNum <= 6 {
		return 1
	} else if serverNum >= 7 && serverNum <= 9 {
		return 2
	}
	return -1
}

func processTransaction(tx Transaction) {
	// log.Printf("Starting processing of transaction: ID=%s, %+v", tx.ID, tx)
	clusterX := getClusterForDataItem(tx.X)
	clusterY := getClusterForDataItem(tx.Y)

	if clusterX == -1 || clusterY == -1 {
		log.Printf("Invalid cluster for data items in transaction: ID=%s, %+v", tx.ID, tx)
		return
	}

	// Access the designated servers
	mu.Lock()
	serverLabelX, existsX := DesignatedServers[clusterX]
	serverLabelY, existsY := DesignatedServers[clusterY]
	mu.Unlock()

	if clusterX == clusterY {
		// Intra-shard transaction
		if !existsX {
			log.Printf("No designated server for Cluster %d in transaction ID=%s", clusterX, tx.ID)
			return
		}
		serverAddr := ServerAddressMap[serverLabelX]
		// log.Printf("Intra-shard transaction using designated server: %s for cluster %d, Transaction ID=%s", serverAddr, clusterX, tx.ID)
		sendIntraShardTransaction(serverAddr, tx)
	} else {
		// Cross-shard transaction
		if !existsX || !existsY {
			log.Printf("No designated server for one of the clusters (%d or %d) in transaction ID=%s", clusterX, clusterY, tx.ID)
			return
		}
		serverAddrX := ServerAddressMap[serverLabelX]
		serverAddrY := ServerAddressMap[serverLabelY]
		// log.Printf("Cross-shard transaction using designated servers: %s (Cluster %d) and %s (Cluster %d), Transaction ID=%s", serverAddrX, clusterX, serverAddrY, clusterY, tx.ID)
		sendCrossShardTransaction(serverAddrX, serverAddrY, tx)
	}
	// log.Printf("Completed processing of transaction: ID=%s, %+v", tx.ID, tx)
}

// getClusterForDataItem returns the cluster index for a given data item (client ID)
func getClusterForDataItem(dataItem int) int {
	cluster := (dataItem - 1) / DATA_ITEMS_PER_SHARD
	if cluster >= len(ClusterServers) {
		return -1
	}
	return cluster
}

// sendIntraShardTransaction sends an intra-shard transaction to a server
func sendIntraShardTransaction(serverAddr string, tx Transaction) {
	// log.Printf("Sending intra-shard transaction to server %s: ID=%s, %+v", serverAddr, tx.ID, tx)
	client, err := rpc.Dial("tcp", serverAddr)
	if err != nil {
		log.Printf("Error connecting to server %s: %v", serverAddr, err)
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("Error closing RPC connection to server %s: %v", serverAddr, err)
		} else {
			// log.Printf("RPC connection to server %s closed.", serverAddr)
		}
	}()

	var reply ServerResponse
	err = client.Call("Server.HandleIntraShardTransaction", tx, &reply)
	if err != nil {
		log.Printf("RPC error while handling intra-shard transaction on server %s: %v", serverAddr, err)
		return
	}

	if reply.Status == "committed" {
		// log.Printf("Intra-shard transaction committed on server %s: ID=%s, %+v", serverAddr, tx.ID, tx)
	} else {
		// log.Printf("Intra-shard transaction aborted on server %s: ID=%s, %+v, Status: %s", serverAddr, tx.ID, tx, reply.Status)
	}
}

// sendCrossShardTransaction coordinates a cross-shard transaction using 2PC
func sendCrossShardTransaction(serverAddrX, serverAddrY string, tx Transaction) {
	// log.Printf("Initiating cross-shard transaction between servers %s and %s: ID=%s, %+v", serverAddrX, serverAddrY, tx.ID, tx)

	// Step 1: Send transaction to both servers and wait for "prepared" or "aborted" status
	var wg sync.WaitGroup
	var respX, respY ServerResponse

	wg.Add(2)
	go func() {
		defer wg.Done()
		// log.Printf("Sending cross-shard transaction to server %s: ID=%s, %+v", serverAddrX, tx.ID, tx)
		clientX, err := rpc.Dial("tcp", serverAddrX)
		if err != nil {
			log.Printf("Error connecting to server %s: %v", serverAddrX, err)
			return
		}
		defer func() {
			if err := clientX.Close(); err != nil {
				log.Printf("Error closing RPC connection to server %s: %v", serverAddrX, err)
			} else {
				// log.Printf("RPC connection to server %s closed.", serverAddrX)
			}
		}()

		err = clientX.Call("Server.HandleCrossShardTransaction", tx, &respX)
		if err != nil {
			log.Printf("RPC error while handling cross-shard transaction on server %s: %v", serverAddrX, err)
			return
		}

		// log.Printf("Server %s responded with status: %s, Transaction ID=%s", serverAddrX, respX.Status, tx.ID)
	}()

	go func() {
		defer wg.Done()
		// log.Printf("Sending cross-shard transaction to server %s: ID=%s, %+v", serverAddrY, tx.ID, tx)
		clientY, err := rpc.Dial("tcp", serverAddrY)
		if err != nil {
			log.Printf("Error connecting to server %s: %v", serverAddrY, err)
			return
		}
		defer func() {
			if err := clientY.Close(); err != nil {
				log.Printf("Error closing RPC connection to server %s: %v", serverAddrY, err)
			} else {
				// log.Printf("RPC connection to server %s closed.", serverAddrY)
			}
		}()

		err = clientY.Call("Server.HandleCrossShardTransaction", tx, &respY)
		if err != nil {
			log.Printf("RPC error while handling cross-shard transaction on server %s: %v", serverAddrY, err)
			return
		}

		// log.Printf("Server %s responded with status: %s, Transaction ID=%s", serverAddrY, respY.Status, tx.ID)
	}()
	wg.Wait()

	// Step 2: Decide to commit or abort based on responses
	if respX.Status == "prepared" && respY.Status == "prepared" {
		// log.Printf("Both servers prepared successfully. Initiating commit for transaction ID=%s.", tx.ID)

		// Prepare Commit arguments
		commitArgs := CommitCrossShardArgs{
			TransactionID: tx.ID,
			IsForwarded:   false,
		}

		// Prepare replies
		var commitRespX, commitRespY CommitCrossShardReply
		var commitErrX, commitErrY error

		wg.Add(2)
		go func() {
			defer wg.Done()
			// log.Printf("Sending CommitCrossShardTransaction to server %s for Transaction ID=%s.", serverAddrX, tx.ID)
			clientX, err := rpc.Dial("tcp", serverAddrX)
			if err != nil {
				commitErrX = err
				log.Printf("Error connecting to server %s for commit: %v", serverAddrX, err)
				return
			}
			defer func() {
				if err := clientX.Close(); err != nil {
					log.Printf("Error closing RPC connection to server %s after commit: %v", serverAddrX, err)
				} else {
					// log.Printf("RPC connection to server %s closed after commit.", serverAddrX)
				}
			}()

			err = clientX.Call("Server.CommitCrossShardTransaction", commitArgs, &commitRespX)
			if err != nil {
				// log.Printf("RPC error during CommitCrossShardTransaction on server %s: %v", serverAddrX, err)
				return
			}

			if commitRespX.Success {
				// log.Printf("CommitCrossShardTransaction succeeded on server %s for Transaction ID=%s.", serverAddrX, tx.ID)
			} else {
				// log.Printf("CommitCrossShardTransaction failed on server %s for Transaction ID=%s.", serverAddrX, tx.ID)
			}
		}()

		go func() {
			defer wg.Done()
			// log.Printf("Sending CommitCrossShardTransaction to server %s for Transaction ID=%s.", serverAddrY, tx.ID)
			clientY, err := rpc.Dial("tcp", serverAddrY)
			if err != nil {
				commitErrY = err
				log.Printf("Error connecting to server %s for commit: %v", serverAddrY, err)
				return
			}
			defer func() {
				if err := clientY.Close(); err != nil {
					log.Printf("Error closing RPC connection to server %s after commit: %v", serverAddrY, err)
				} else {
					// log.Printf("RPC connection to server %s closed after commit.", serverAddrY)
				}
			}()

			err = clientY.Call("Server.CommitCrossShardTransaction", commitArgs, &commitRespY)
			if err != nil {
				// log.Printf("RPC error during CommitCrossShardTransaction on server %s: %v", serverAddrY, err)
				return
			}

			if commitRespY.Success {
				// log.Printf("CommitCrossShardTransaction succeeded on server %s for Transaction ID=%s.", serverAddrY, tx.ID)
			} else {
				// log.Printf("CommitCrossShardTransaction failed on server %s for Transaction ID=%s.", serverAddrY, tx.ID)
			}
		}()
		wg.Wait()

		// Check for any errors during commit
		if commitErrX != nil || commitErrY != nil {
			log.Printf("Errors occurred during commit phase for Transaction ID=%s. Manual intervention may be required.", tx.ID)
			return
		}

		// log.Printf("Transaction ID=%s committed successfully across both servers.", tx.ID)
	} else {
		// log.Printf("At least one server aborted the transaction ID=%s. Initiating abort.", tx.ID)

		// Prepare Abort arguments
		abortArgs := AbortCrossShardArgs{
			TransactionID: tx.ID,
			IsForwarded:   false,
		}

		// Prepare replies
		var abortRespX, abortRespY AbortCrossShardReply
		var abortErrX, abortErrY error

		wg.Add(2)
		go func() {
			defer wg.Done()
			// log.Printf("Sending AbortCrossShardTransaction to server %s for Transaction ID=%s.", serverAddrX, tx.ID)
			clientX, err := rpc.Dial("tcp", serverAddrX)
			if err != nil {
				abortErrX = err
				log.Printf("Error connecting to server %s for abort: %v", serverAddrX, err)
				return
			}
			defer func() {
				if err := clientX.Close(); err != nil {
					log.Printf("Error closing RPC connection to server %s after abort: %v", serverAddrX, err)
				} else {
					// log.Printf("RPC connection to server %s closed after abort.", serverAddrX)
				}
			}()

			err = clientX.Call("Server.AbortCrossShardTransaction", abortArgs, &abortRespX)
			if err != nil {
				// log.Printf("RPC error during AbortCrossShardTransaction on server %s: %v", serverAddrX, err)
				return
			}

			if abortRespX.Success {
				// log.Printf("AbortCrossShardTransaction succeeded on server %s for Transaction ID=%s.", serverAddrX, tx.ID)
			} else {
				// log.Printf("AbortCrossShardTransaction failed on server %s for Transaction ID=%s.", serverAddrX, tx.ID)
			}
		}()

		go func() {
			defer wg.Done()
			// log.Printf("Sending AbortCrossShardTransaction to server %s for Transaction ID=%s.", serverAddrY, tx.ID)
			clientY, err := rpc.Dial("tcp", serverAddrY)
			if err != nil {
				abortErrY = err
				log.Printf("Error connecting to server %s for abort: %v", serverAddrY, err)
				return
			}
			defer func() {
				if err := clientY.Close(); err != nil {
					log.Printf("Error closing RPC connection to server %s after abort: %v", serverAddrY, err)
				} else {
					// log.Printf("RPC connection to server %s closed after abort.", serverAddrY)
				}
			}()

			err = clientY.Call("Server.AbortCrossShardTransaction", abortArgs, &abortRespY)
			if err != nil {
				log.Printf("RPC error during AbortCrossShardTransaction on server %s: %v", serverAddrY, err)
				return
			}

			if abortRespY.Success {
				// log.Printf("AbortCrossShardTransaction succeeded on server %s for Transaction ID=%s.", serverAddrY, tx.ID)
			} else {
				log.Printf("AbortCrossShardTransaction failed on server %s for Transaction ID=%s.", serverAddrY, tx.ID)
			}
		}()
		wg.Wait()

		// Check for any errors during abort
		if abortErrX != nil || abortErrY != nil {
			log.Printf("Errors occurred during abort phase for Transaction ID=%s. Manual intervention may be required.", tx.ID)
			return
		}

		// log.Printf("Transaction ID=%s aborted successfully across both servers.", tx.ID)
	}
}

// SetServerActiveStatus sets the active status of a single server
func SetServerActiveStatus(serverLabel string, active bool) {
	// log.Printf("Setting active status of server %s (%s) to %v.", serverLabel, addr, active)
	addr, exists := ServerAddressMap[serverLabel]
	if !exists {
		log.Printf("Unknown server label: %s. Skipping SetActive.", serverLabel)
		return
	}

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Printf("Error connecting to server %s (%s): %v", serverLabel, addr, err)
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("Error closing RPC connection to server %s (%s): %v", serverLabel, addr, err)
		} else {
			// log.Printf("RPC connection to server %s (%s) closed.", serverLabel, addr)
		}
	}()

	setActiveArgs := SetActiveArgs{
		Active: active,
	}
	var setActiveReply SetActiveReply

	err = client.Call("Server.SetActive", setActiveArgs, &setActiveReply)
	if err != nil {
		log.Printf("Error calling SetActive on server %s (%s): %v", serverLabel, addr, err)
	} else {
		// log.Printf("Server %s (%s) active status set to %v.", serverLabel, addr, active)
	}
}

// PrintBalances reads and prints the balances of specified data items in server-wise order
func PrintBalances(dataItems []int) {
	fmt.Println("\nBalances:")
	var wg sync.WaitGroup

	// Iterate over each data item
	for _, item := range dataItems {
		wg.Add(1)
		go func(item int) {
			defer wg.Done()
			cluster := getClusterForDataItem(item)
			if cluster == -1 {
				fmt.Printf("Data Item %d: Invalid cluster.\n", item)
				return
			}

			// Determine all servers in the cluster
			var serversInCluster []string
			for _, serverLabel := range sortedServers {
				if getClusterForServerLabel(serverLabel) == cluster {
					serversInCluster = append(serversInCluster, serverLabel)
				}
			}

			// Collect balance results in a map for ordered printing
			balanceResults := make(map[string]string)
			var muResults sync.Mutex
			var wgServers sync.WaitGroup

			for _, serverLabel := range serversInCluster {
				wgServers.Add(1)
				go func(serverLabel string) {
					defer wgServers.Done()
					serverAddr := ServerAddressMap[serverLabel]
					client, err := rpc.Dial("tcp", serverAddr)
					if err != nil {
						muResults.Lock()
						balanceResults[serverLabel] = "Disconnected"
						muResults.Unlock()
						return
					}
					defer func() {
						if err := client.Close(); err != nil {
							log.Printf("Error closing RPC connection to server %s: %v", serverAddr, err)
						}
					}()

					var balance int
					err = client.Call("Server.GetBalance", item, &balance)
					if err != nil {
						muResults.Lock()
						balanceResults[serverLabel] = "RPC Error"
						muResults.Unlock()
						return
					}

					muResults.Lock()
					balanceResults[serverLabel] = strconv.Itoa(balance)
					muResults.Unlock()
				}(serverLabel)
			}

			wgServers.Wait()

			// Display results for this data item in sorted server order
			fmt.Printf("\nBalances for Data Item %d:\n", item)
			for _, serverLabel := range sortedServers {
				if contains(serversInCluster, serverLabel) {
					fmt.Printf("  %s -> %s\n", serverLabel, balanceResults[serverLabel])
				}
			}
		}(item)
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()
}

// PrintPerformance prints the performance metrics
func PrintPerformance() {
	// log.Println("Printing performance metrics.")
	mu.Lock()
	defer mu.Unlock()
	if totalTransactions > 0 {
		averageLatency := totalLatency / time.Duration(totalTransactions)
		fmt.Println("\nPerformance Metrics:")
		fmt.Printf("Total Transactions: %d\n", totalTransactions)
		fmt.Printf("Total Latency: %v\n", totalLatency)
		fmt.Printf("Average Latency: %v\n", averageLatency)
		if totalLatency.Seconds() > 0 {
			fmt.Printf("Throughput: %.2f transactions/sec\n", float64(totalTransactions)/totalLatency.Seconds())
		} else {
			fmt.Printf("Throughput: Undefined (total latency is zero)\n")
		}
	} else {
		fmt.Println("\nPerformance Metrics:")
		fmt.Println("No transactions were processed.")
	}
	// log.Println("Performance metrics printed.")
}

// DatastoreEntry represents a single entry in the datastore
type DatastoreEntry struct {
	BallotNum   int
	Type        string // 'C' for committed, 'P' for prepared, 'A' for aborted, empty for intra-shard
	Transaction Transaction
}

// GetDatastoreArgs represents the arguments for the GetDatastore RPC call
type GetDatastoreArgs struct{}

// GetDatastoreReply represents the reply for the GetDatastore RPC call
type GetDatastoreReply struct {
	Entries []DatastoreEntry
}

// PrintDatastore retrieves and prints the committed transactions from each server in server-wise order
func PrintDatastore() {
	fmt.Println("\nServer Datastore")
	for _, serverLabel := range sortedServers {
		serverAddr, exists := ServerAddressMap[serverLabel]
		if !exists {
			fmt.Printf("%s\n  [Unknown Server]\n", serverLabel)
			continue
		}

		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			fmt.Printf("%s\n  [Disconnected]\n", serverLabel)
			continue
		}

		var reply GetDatastoreReply
		err = client.Call("Server.GetDatastore", GetDatastoreArgs{}, &reply)
		if err != nil {
			fmt.Printf("%s\n  [RPC Error]\n", serverLabel)
			client.Close()
			continue
		}

		entries := reply.Entries

		if len(entries) == 0 {
			fmt.Printf("%s\n  [No Transactions]\n", serverLabel)
		} else {
			fmt.Printf("%s\n ", serverLabel)
			// For each entry, format and print
			for i, entry := range entries {
				var entryStr string
				clusterID := getClusterForServerLabel(serverLabel) + 1 // Cluster IDs are 1-based
				if entry.Type != "" {
					// Cross-shard transaction with status
					entryStr = fmt.Sprintf("[<%d,%d>, %s, (%d, %d, %d)]", entry.BallotNum, clusterID, entry.Type, entry.Transaction.X, entry.Transaction.Y, entry.Transaction.Amt)
				} else {
					// Intra-shard transaction
					entryStr = fmt.Sprintf("[<%d,%d>, (%d, %d, %d)]", entry.BallotNum, clusterID, entry.Transaction.X, entry.Transaction.Y, entry.Transaction.Amt)
				}
				if i < len(entries)-1 {
					entryStr += " →"
				}
				fmt.Print(entryStr)
			}
			fmt.Println()
		}
		client.Close()
	}
}
