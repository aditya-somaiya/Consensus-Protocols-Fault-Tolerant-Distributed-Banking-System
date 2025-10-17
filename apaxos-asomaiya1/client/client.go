package main

import (
	"bufio"
	"dslab/models" // Import the shared models package
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TransactionSet represents a set of transactions along with live servers.
type TransactionSet struct {
	SetNumber    int
	Transactions []models.Transaction
	LiveServers  []string
}

// Global map of all servers and their addresses.
var allServers = map[string]string{
	"S1": "localhost:1231",
	"S2": "localhost:1232",
	"S3": "localhost:1233",
	"S4": "localhost:1234",
	"S5": "localhost:1235",
}

// Map to keep track of active servers for the current set.
var activeServers = make(map[string]string)

// Global map to store persistent RPC clients
var rpcClients = make(map[string]*rpc.Client)
var rpcClientsMutex sync.Mutex

func main() {
	gob.Register(models.Transaction{}) // Register Transaction with gob
	gob.Register(models.PerformanceMetrics{})

	defer func() {
		rpcClientsMutex.Lock()
		defer rpcClientsMutex.Unlock()
		for address, client := range rpcClients {
			err := client.Close()
			if err != nil {
				log.Printf("Error closing RPC client for %s: %v", address, err)
			}
		}
	}()

	// Read the transactions from the CSV file.
	transactionSets := readInputCSV("../lab_test.csv")

	for _, tSet := range transactionSets {
		fmt.Printf("\nProcessing Set Number: %d\n", tSet.SetNumber)
		updateActiveServers(tSet.LiveServers)

		processTransactionSet(tSet)

		userInteraction()

		// fmt.Print("\nPress Enter to process the next set...")
		// bufio.NewReader(os.Stdin).ReadBytes('\n')
	}
	fmt.Println("\nAll transaction sets have been processed.")
}

// readInputCSV reads the input CSV file and returns a slice of TransactionSets.
func readInputCSV(filePath string) []TransactionSet {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true

	var transactionSets []TransactionSet
	var currentSetNumber int
	var currentTransactions []models.Transaction
	var currentLiveServers []string

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break // End of file
		}
		if err != nil {
			log.Fatalf("Failed to read CSV file: %v", err)
		}

		// Ensure the record has at least 2 fields (SetNumber can be empty)
		if len(record) < 2 {
			log.Fatalf("Invalid record length: %v", record)
		}

		// Extract fields
		setNumberStr := strings.TrimSpace(record[0])
		transactionStr := strings.TrimSpace(record[1])
		liveServersStr := ""
		if len(record) >= 3 {
			liveServersStr = strings.TrimSpace(record[2])
		}

		// If setNumberStr is not empty, it's a new set
		if setNumberStr != "" {
			// If there is a current set, append it to transactionSets
			if currentSetNumber != 0 {
				transactionSets = append(transactionSets, TransactionSet{
					SetNumber:    currentSetNumber,
					Transactions: currentTransactions,
					LiveServers:  currentLiveServers,
				})
				currentTransactions = []models.Transaction{}
			}

			// Parse set number
			parsedSetNumber, err := strconv.Atoi(setNumberStr)
			if err != nil {
				log.Fatalf("Error parsing Set Number '%s': %v", setNumberStr, err)
			}
			currentSetNumber = parsedSetNumber

			// Parse liveServersStr, e.g., "[S1, S2, S3, S4, S5]"
			if liveServersStr != "" {
				liveServersStr = strings.Trim(liveServersStr, "[]")
				liveServers := strings.Split(liveServersStr, ",")
				for i := range liveServers {
					liveServers[i] = strings.TrimSpace(liveServers[i])
				}
				currentLiveServers = liveServers
			} else {
				currentLiveServers = []string{}
			}
		}

		// Parse transactionStr, e.g., "(S1, S3, 45)"
		if transactionStr == "" {
			log.Fatalf("Empty transaction field in record: %v", record)
		}
		transactionStr = strings.Trim(transactionStr, "()")
		txParts := strings.Split(transactionStr, ",")
		if len(txParts) != 3 {
			log.Fatalf("Invalid transaction format '%s' in record: %v", transactionStr, record)
		}

		sender := strings.TrimSpace(txParts[0])
		receiver := strings.TrimSpace(txParts[1])
		amountStr := strings.TrimSpace(txParts[2])
		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			log.Fatalf("Error parsing amount '%s' in record: %v", amountStr, err)
		}

		transaction := models.Transaction{
			SetNumber: currentSetNumber,
			Sender:    sender,
			Receiver:  receiver,
			Amount:    amount,
			Timestamp: time.Now(),
		}
		currentTransactions = append(currentTransactions, transaction)
	}

	// Add the last set if present
	if currentSetNumber != 0 {
		transactionSets = append(transactionSets, TransactionSet{
			SetNumber:    currentSetNumber,
			Transactions: currentTransactions,
			LiveServers:  currentLiveServers,
		})
	}

	return transactionSets
}

// getRPCClient retrieves an existing RPC client or creates a new one if it doesn't exist.
func getRPCClient(serverAddress string) (*rpc.Client, error) {
	rpcClientsMutex.Lock()
	defer rpcClientsMutex.Unlock()

	// Check if the client already exists
	if client, exists := rpcClients[serverAddress]; exists {
		return client, nil
	}

	// Dial a new RPC client
	client, err := rpc.Dial("tcp", serverAddress)
	if err != nil {
		return nil, err
	}

	// Store the new client in the map
	rpcClients[serverAddress] = client
	return client, nil
}

// updateActiveServers updates the activeServers map based on the provided list.
func updateActiveServers(liveServers []string) {
	activeServers = make(map[string]string)
	for _, serverID := range liveServers {
		serverID = strings.TrimSpace(serverID)
		address, exists := allServers[serverID]
		if exists {
			activeServers[serverID] = address
		} else {
			fmt.Printf("Server %s does not exist.\n", serverID)
		}
	}
	fmt.Printf("Active servers for this set: %v\n", liveServers)
}

// SendAliveStatus informs servers whether they are alive for the current transaction set.
func SendAliveStatus(tSet TransactionSet) {
	for _, serverID := range tSet.LiveServers {
		serverAddress, isActive := activeServers[serverID]
		if !isActive {
			continue
		}
		client, err := getRPCClient(serverAddress)
		if err != nil {
			log.Printf("Error connecting to server %s: %v", serverAddress, err)
			continue
		}

		var reply bool
		aliveStatus := true
		err = client.Call("Server.SetAliveStatus", aliveStatus, &reply)
		if err != nil {
			log.Printf("Error sending alive status to server %s: %v", serverAddress, err)
		}
		// Removed client.Close() here to keep the connection persistent
	}

	// Mark other servers as not alive
	for serverID, serverAddress := range allServers {
		if _, isActive := activeServers[serverID]; !isActive {
			client, err := getRPCClient(serverAddress)
			if err != nil {
				log.Printf("Error connecting to server %s: %v", serverAddress, err)
				continue
			}

			var reply bool
			aliveStatus := false
			err = client.Call("Server.SetAliveStatus", aliveStatus, &reply)
			if err != nil {
				log.Printf("Error sending alive status to server %s: %v", serverAddress, err)
			}
			// Removed client.Close() here to keep the connection persistent
		}
	}
}

// processTransactionSet processes a single TransactionSet.
func processTransactionSet(tSet TransactionSet) {
	// Inform servers of their alive state.
	SendAliveStatus(tSet)

	for _, transaction := range tSet.Transactions {
		senderServerID := transaction.Sender
		serverAddress, exists := allServers[senderServerID]
		if !exists {
			log.Printf("Sender server %s does not exist. Skipping transaction: %+v", senderServerID, transaction)
			continue
		}

		sendTransaction(serverAddress, transaction)
	}
}

// sendTransaction sends a transaction to the appropriate server based on the sender.
func sendTransaction(serverAddress string, transaction models.Transaction) {
	client, err := getRPCClient(serverAddress)
	if err != nil {
		log.Printf("Error connecting to server %s: %v", serverAddress, err)
		return
	}

	var reply string
	err = client.Call("Server.ReceiveTransaction", transaction, &reply)
	if err != nil {
		log.Printf("Error calling ReceiveTransaction on server %s: %v", serverAddress, err)
		return
	}
	// Removed client.Close() here to keep the connection persistent

	// fmt.Printf("Server %s response: %s\n", transaction.Sender, reply)
}

// userInteraction allows the user to invoke functions before processing the next set.
func userInteraction() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("\nAvailable commands:")
		fmt.Println("1. Print Balance")
		fmt.Println("2. PrintLog")
		fmt.Println("3. Print Datastore <server ID>")
		fmt.Println("4. Performance")
		fmt.Println("5. Continue to next set")
		fmt.Print("Enter command number (1-5) or press Enter to continue: ")

		input, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading input: %v", err)
			continue
		}

		input = strings.TrimSpace(input)

		// If input is empty, default to option 5
		if input == "" {
			input = "5"
		}

		switch input {
		case "1":
			PrintAllBalances()
		case "2":
			PrintAllLogs()
		case "3":
			// Prompt the user for ServerID
			fmt.Print("Enter ServerID: ")
			serverInput, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("Error reading ServerID: %v", err)
				continue
			}
			serverID := strings.TrimSpace(strings.ToUpper(serverInput))
			if serverID == "" {
				fmt.Println("ServerID cannot be empty.")
				continue
			}
			PrintDatastore(serverID)
		case "4":
			Performance()
		case "5":
			// fmt.Println("Continuing to next set...")
			return // Exit the userInteraction function to continue
		default:
			fmt.Println("Invalid command. Please enter a number between 1 and 5.")
		}
	}
}

// PrintAllBalances retrieves and prints the balance from all servers.
func PrintAllBalances() {
	var wg sync.WaitGroup
	balanceCh := make(chan string, len(allServers))

	for serverID, serverAddress := range allServers {
		wg.Add(1)
		go func(id, address string) {
			defer wg.Done()
			client, err := getRPCClient(address)
			if err != nil {
				log.Printf("Error connecting to server %s (%s): %v", id, address, err)
				balanceCh <- fmt.Sprintf("Server %s: Connection error: %v", id, err)
				return
			}

			var balance float64
			err = client.Call("Server.GetBalance", struct{}{}, &balance)
			if err != nil {
				log.Printf("Error calling GetBalance on server %s: %v", id, err)
				balanceCh <- fmt.Sprintf("Server %s: RPC error: %v", id, err)
				return
			}

			balanceCh <- fmt.Sprintf("Server %s: Balance = %.2f", id, balance)
			// Removed client.Close() here to keep the connection persistent
		}(serverID, serverAddress)
	}

	wg.Wait()
	close(balanceCh)

	fmt.Println("Balances of All Servers:")
	for balanceInfo := range balanceCh {
		fmt.Println(balanceInfo)
	}
}

// PrintAllLogs retrieves and prints the transaction logs from all servers.
func PrintAllLogs() {
	var wg sync.WaitGroup
	logCh := make(chan string, len(allServers))

	for serverID, serverAddress := range allServers {
		wg.Add(1)
		go func(id, address string) {
			defer wg.Done()
			client, err := getRPCClient(address)
			if err != nil {
				log.Printf("Error connecting to server %s (%s): %v", id, address, err)
				logCh <- fmt.Sprintf("Server %s: Connection error: %v", id, err)
				return
			}

			var logEntries []models.Transaction
			err = client.Call("Server.GetLog", struct{}{}, &logEntries)
			if err != nil {
				log.Printf("Error calling GetLog on server %s: %v", id, err)
				logCh <- fmt.Sprintf("Server %s: RPC error: %v", id, err)
				return
			}

			var sb strings.Builder
			sb.WriteString(fmt.Sprintf("Local Log of Server %s:\n", id))
			for _, tx := range logEntries {
				sb.WriteString(fmt.Sprintf("  - Sender: %s, Receiver: %s, Amount: %.2f\n",
					tx.Sender, tx.Receiver, tx.Amount))
			}
			logCh <- sb.String()
			// Removed client.Close() here to keep the connection persistent
		}(serverID, serverAddress)
	}

	wg.Wait()
	close(logCh)

	fmt.Println("Transaction Logs of All Servers:")
	for logInfo := range logCh {
		fmt.Println(logInfo)
	}
}

// PrintDatastore retrieves and prints the datastore of a specific server.
func PrintDatastore(serverID string) {
	serverAddress, exists := allServers[serverID]
	if !exists {
		fmt.Printf("Server ID %s does not exist.\n", serverID)
		return
	}

	client, err := getRPCClient(serverAddress)
	if err != nil {
		log.Printf("Error connecting to server %s (%s): %v", serverID, serverAddress, err)
		return
	}

	var datastore []models.MajorBlock
	// Corrected: Pass struct{}{} instead of ""
	err = client.Call("Server.GetDatastore", struct{}{}, &datastore)
	if err != nil {
		log.Printf("Error calling GetDatastore on server %s: %v", serverID, err)
		return
	}

	fmt.Printf("\nDatastore of server %s:\n", serverID)
	for _, block := range datastore {
		fmt.Printf("MajorBlock (BallotNumber: %d, SequenceNumber: %d)\n", block.BallotNumber, block.SequenceNumber)
		for _, tx := range block.Transactions {
			fmt.Printf("  Transaction: Sender: %s, Receiver: %s, Amount: %.2f\n",
				tx.Sender, tx.Receiver, tx.Amount)
		}
	}
	// Removed client.Close() here to keep the connection persistent
}

// Performance retrieves and prints the performance metrics (stub for now).
func Performance() {
	var wg sync.WaitGroup
	metricsCh := make(chan string, len(allServers))

	for serverID, serverAddress := range allServers {
		wg.Add(1)
		go func(id, address string) {
			defer wg.Done()
			client, err := getRPCClient(address)
			if err != nil {
				log.Printf("Error connecting to server %s (%s): %v", id, address, err)
				metricsCh <- fmt.Sprintf("Server %s: Connection error: %v", id, err)
				return
			}

			var metrics models.PerformanceMetrics
			err = client.Call("Server.GetPerformanceMetrics", struct{}{}, &metrics)
			if err != nil {
				log.Printf("Error calling  GetPerformanceMetrics on server %s: %v", id, err)
				metricsCh <- fmt.Sprintf("Server %s: RPC error: %v", id, err)
				return
			}

			metricsCh <- fmt.Sprintf("Server %s: Average Latency = %v, Throughput = %.2f transactions/sec", id, metrics.AverageLatency, metrics.Throughput)
			// Removed client.Close() here to keep the connection persistent
		}(serverID, serverAddress)
	}

	wg.Wait()
	close(metricsCh)

	fmt.Println("Performance Metrics of All Servers:")
	for metricsInfo := range metricsCh {
		fmt.Println(metricsInfo)
	}
}

// // printBalance retrieves and prints the balance of a client.
// func printBalance(serverID string) {
// 	serverAddress, _ := allServers[serverID]

// 	client, err := rpc.Dial("tcp", serverAddress)
// 	if err != nil {
// 		log.Printf("Error connecting to server %s: %v", serverAddress, err)
// 		return
// 	}
// 	defer client.Close()

// 	var balance float64
// 	err = client.Call("Server.GetBalance", struct{}{}, &balance)
// 	if err != nil {
// 		log.Printf("Error calling GetBalance on server %s: %v", serverID, err)
// 		return
// 	}

// 	fmt.Printf("Current balance of server %s: %.2f\n", serverID, balance)
// }

// // printLog retrieves and prints the local log of a server.
// func printLog(serverID string) {
// 	serverAddress, _ := allServers[serverID]

// 	client, err := rpc.Dial("tcp", serverAddress)
// 	if err != nil {
// 		log.Printf("Error connecting to server %s: %v", serverAddress, err)
// 		return
// 	}
// 	defer client.Close()

// 	var logEntries []models.Transaction
// 	err = client.Call("Server.GetLog", struct{}{}, &logEntries)
// 	if err != nil {
// 		log.Printf("Error calling GetLog on server %s: %v", serverID, err)
// 		return
// 	}

// 	fmt.Printf("Local log of server %s:\n", serverID)
// 	for _, tx := range logEntries {
// 		fmt.Printf("Transaction: Sender: %s, Receiver: %s, Amount: %.2f\n", tx.Sender, tx.Receiver, tx.Amount)
// 	}
// }
