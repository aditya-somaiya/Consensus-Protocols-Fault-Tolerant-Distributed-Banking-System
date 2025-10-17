package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
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

// Constants for configuration
const (
	NUM_CLUSTERS          = 3
	DATA_ITEMS_PER_SHARD  = 1000
	NUM_SERVERS_PER_SHARD = 3
	MAJORITY              = 2 // For clusters of size 3
)

// Transaction represents a banking transaction
type Transaction struct {
	ID  string // Unique Transaction ID provided by the client
	X   int    // Sender
	Y   int    // Receiver
	Amt int    // Amount to transfer
}

// ServerResponse represents the response from a server
type ServerResponse struct {
	Status        string // "prepared", "committed", "aborted", "server busy"
	TransactionID string // Unique Transaction ID assigned by the client
}

// PrepareArgs are the arguments for the Prepare RPC call
type PrepareArgs struct {
	TransactionID string
	BallotNum     int
	Tx            Transaction
	CallerAddress string // To identify the caller's address
}

// PrepareReply is the reply for the Prepare RPC call
type PrepareReply struct {
	Promise          bool
	AcceptNum        int
	AcceptedValue    Transaction
	HighestBallotNum int
}

// AcceptArgs are the arguments for the Accept RPC call
type AcceptArgs struct {
	TransactionID string
	BallotNum     int
	Tx            Transaction
}

// AcceptReply is the reply for the Accept RPC call
type AcceptReply struct {
	Accepted         bool
	HighestBallotNum int
}

// DatastoreEntry represents a single entry in the datastore
type DatastoreEntry struct {
	BallotNum   int
	Type        string // 'P' or 'C' for cross-shard transactions, empty string for intra-shard
	Transaction Transaction
}

// CommitArgs are the arguments for the Commit RPC call
type CommitArgs struct {
	Tx           Transaction
	IsCrossShard bool
}

// CommitReply is the reply for the Commit RPC call
type CommitReply struct {
	Success bool
}

// SetActiveArgs represents the arguments for the SetActive RPC call
type SetActiveArgs struct {
	Active bool // Indicates if the server should be active or inactive
}

// SetActiveReply represents the reply for the SetActive RPC call (empty in this case)
type SetActiveReply struct{}

// GetCommittedArgs represents the arguments for the GetCommittedTransactions RPC call
type GetCommittedArgs struct {
	FromBallotNum int // Fetch transactions with BallotNum > FromBallotNum
}

// GetCommittedReply represents the reply for the GetCommittedTransactions RPC call
type GetCommittedReply struct {
	CommittedTxs []CommittedTransaction
}

// CommittedTransaction pairs a Transaction with its BallotNum
type CommittedTransaction struct {
	BallotNum int
	Tx        Transaction
}

// PeerPrepareReply associates a PrepareReply with its originating peer
type PeerPrepareReply struct {
	Peer  string
	Reply PrepareReply
}

// TryMutex is a mutex that supports TryLock
type TryMutex struct {
	ch chan struct{}
}

func NewTryMutex() *TryMutex {
	return &TryMutex{ch: make(chan struct{}, 1)}
}

func (m *TryMutex) Lock() {
	m.ch <- struct{}{}
}

func (m *TryMutex) Unlock() {
	select {
	case <-m.ch:
	default:
	}
}

func (m *TryMutex) TryLock() bool {
	select {
	case m.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

// Server represents a server in the cluster
type Server struct {
	mu sync.Mutex

	// Server identity
	ClusterID int
	ServerID  int
	Address   string

	// Active status
	active      bool
	activeMutex sync.RWMutex

	// Paxos states
	paxosStates map[string]*PaxosState

	// Database
	db *bolt.DB

	// Peers
	peers []string

	// Locks for data items
	locks map[int]*TryMutex

	transactionQueue chan TransactionRequest

	// Global Ballot Number
	ballotNum   int
	ballotMutex sync.Mutex
}

// TransactionRequest represents a transaction and a channel to send back the response
type TransactionRequest struct {
	Tx           Transaction
	IsCrossShard bool
	Reply        chan ServerResponse
}

// PaxosState represents the Paxos state for a single transaction
type PaxosState struct {
	mu          sync.Mutex
	CommittedTx Transaction
	Tx          Transaction
	LocksHeld   []*TryMutex
}

// WALRecord represents a write-ahead log record
type WALRecord struct {
	TxID      string
	X         int
	Y         int
	BalanceX  int
	BalanceY  int
	Timestamp time.Time
}

// NewServer initializes a new server
func NewServer(address string) *Server {
	// Determine ClusterID and ServerID based on the port number
	clusterID, serverID := getClusterAndServerIDFromAddress(address)
	log.Printf("Initializing Server: ClusterID=%d, ServerID=%d, Address=%s", clusterID, serverID, address)

	dbName := fmt.Sprintf("cluster%d_server%d.db", clusterID, serverID)
	db, err := bolt.Open(dbName, 0600, nil)
	if err != nil {
		log.Fatalf("Failed to open BoltDB (%s): %v", dbName, err)
	}

	// Initialize locks for data items
	locks := make(map[int]*TryMutex)
	for i := clusterID*DATA_ITEMS_PER_SHARD + 1; i <= (clusterID+1)*DATA_ITEMS_PER_SHARD; i++ {
		locks[i] = NewTryMutex()
	}

	// Initialize peers
	peers := getPeersForCluster(clusterID, address)
	log.Printf("Peers for Cluster %d: %v", clusterID, peers)

	server := &Server{
		ClusterID:   clusterID,
		ServerID:    serverID,
		Address:     address,
		db:          db,
		peers:       peers,
		locks:       locks,
		paxosStates: make(map[string]*PaxosState),
		active:      true, // Servers start as active by default
		ballotNum:   0,    // Initialize global ballot number

		// Initialize the transaction queue with a buffer size (e.g., 1000)
		transactionQueue: make(chan TransactionRequest, 1000),
	}

	// Initialize all accounts with a balance of 10
	err = server.initializeAccounts()
	if err != nil {
		log.Fatalf("Failed to initialize accounts: %v", err)
	}

	// Start the transaction processing goroutine
	go server.processTransactionQueue()

	return server
}

// processTransactionQueue processes transactions sequentially from the queue
func (s *Server) processTransactionQueue() {
	for req := range s.transactionQueue {
		log.Printf("Processing Transaction ID=%s", req.Tx.ID)
		err := s.runPaxos(req.Tx, req.IsCrossShard)
		if err != nil {
			log.Printf("Transaction Aborted: %+v, Error: %v", req.Tx, err)
			req.Reply <- ServerResponse{
				Status:        "aborted",
				TransactionID: req.Tx.ID,
			}
		} else {
			status := "committed"
			if req.IsCrossShard {
				status = "prepared" // For cross-shard transactions, we send back "prepared"
			}
			log.Printf("Transaction %s: %+v", strings.Title(status), req.Tx)
			req.Reply <- ServerResponse{
				Status:        status,
				TransactionID: req.Tx.ID,
			}
		}
	}
}

// initializeAccounts initializes all accounts in the server's shard with a balance of 10.
func (s *Server) initializeAccounts() error {
	return s.db.Update(func(btx *bolt.Tx) error {
		// Create or retrieve the "Accounts" bucket.
		accountsBucket, err := btx.CreateBucketIfNotExists([]byte("Accounts"))
		if err != nil {
			return fmt.Errorf("failed to create or retrieve Accounts bucket: %v", err)
		}

		// Determine the range of account IDs for this shard.
		startID := s.ClusterID*DATA_ITEMS_PER_SHARD + 1
		endID := (s.ClusterID + 1) * DATA_ITEMS_PER_SHARD

		for i := startID; i <= endID; i++ {
			key := intToBytes(i)
			balanceBytes := accountsBucket.Get(key)
			if balanceBytes == nil {
				// Initialize account with a balance of 10.
				err := accountsBucket.Put(key, intToBytes(10))
				if err != nil {
					return fmt.Errorf("failed to initialize account X=%d: %v", i, err)
				}
			}
		}
		return nil
	})
}

// getClusterAndServerIDFromAddress determines the ClusterID and ServerID based on the port number
func getClusterAndServerIDFromAddress(address string) (int, int) {
	// Extract the port number from the address
	parts := strings.Split(address, ":")
	portStr := parts[len(parts)-1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("Invalid port in address (%s): %v", address, err)
	}

	var clusterID int
	var serverID int

	switch {
	case port >= 5001 && port <= 5003:
		clusterID = 0
		serverID = port - 5000
	case port >= 5004 && port <= 5006:
		clusterID = 1
		serverID = port - 5003
	case port >= 5007 && port <= 5009:
		clusterID = 2
		serverID = port - 5006
	default:
		log.Fatalf("Invalid port number: %d", port)
	}

	log.Printf("Address %s mapped to ClusterID=%d, ServerID=%d", address, clusterID, serverID)
	return clusterID, serverID
}

// getPeersForCluster returns the list of peer addresses for the given cluster, excluding the current server
func getPeersForCluster(clusterID int, currentAddress string) []string {
	var basePort int
	switch clusterID {
	case 0:
		basePort = 5001
	case 1:
		basePort = 5004
	case 2:
		basePort = 5007
	}

	peers := []string{}
	for i := 0; i < NUM_SERVERS_PER_SHARD; i++ {
		port := basePort + i
		address := fmt.Sprintf("localhost:%d", port)
		if address != currentAddress {
			peers = append(peers, address)
		}
	}
	return peers
}

// HandleIntraShardTransaction handles an intra-shard transaction from a client
func (s *Server) HandleIntraShardTransaction(tx Transaction, reply *ServerResponse) error {
	log.Printf("Received Intra-Shard Transaction: %+v", tx)

	// Check if server is active
	if !s.isActive() {
		log.Printf("Server %s is inactive. Rejecting Intra-Shard Transaction.", s.Address)
		reply.Status = "aborted"
		reply.TransactionID = tx.ID
		return errors.New("server is inactive")
	}

	// Ensure the transaction ID is provided by the client
	if tx.ID == "" {
		log.Printf("Transaction ID is missing from the client request.")
		reply.Status = "aborted"
		reply.TransactionID = ""
		return errors.New("transaction ID is missing")
	}

	// Check if the transaction is within this cluster's data range
	if getShardForDataItem(tx.X) != s.ClusterID || getShardForDataItem(tx.Y) != s.ClusterID {
		log.Printf("Transaction is not intra-shard for this cluster")
		reply.Status = "aborted"
		reply.TransactionID = tx.ID
		return errors.New("transaction is not intra-shard for this cluster")
	}

	// Create a response channel
	responseChan := make(chan ServerResponse)

	// Create a TransactionRequest
	req := TransactionRequest{
		Tx:           tx,
		IsCrossShard: false,
		Reply:        responseChan,
	}

	// Enqueue the transaction
	s.transactionQueue <- req
	log.Printf("Transaction ID=%s enqueued", tx.ID)

	// Wait for the transaction to be processed
	response := <-responseChan

	// Populate the reply
	*reply = response
	return nil
}

// HandleCrossShardTransaction handles a cross-shard transaction from a client
func (s *Server) HandleCrossShardTransaction(tx Transaction, reply *ServerResponse) error {
	log.Printf("Received Cross-Shard Transaction: %+v", tx)

	// Check if server is active
	if !s.isActive() {
		log.Printf("Server %s is inactive. Rejecting Cross-Shard Transaction.", s.Address)
		reply.Status = "aborted"
		reply.TransactionID = tx.ID
		return errors.New("server is inactive")
	}

	// Ensure the transaction ID is provided by the client
	if tx.ID == "" {
		log.Printf("Transaction ID is missing from the client request.")
		reply.Status = "aborted"
		reply.TransactionID = ""
		return errors.New("transaction ID is missing")
	}

	// Check if the transaction involves this cluster's data item
	if getShardForDataItem(tx.X) != s.ClusterID && getShardForDataItem(tx.Y) != s.ClusterID {
		log.Printf("Transaction does not involve this cluster's data items")
		reply.Status = "aborted"
		reply.TransactionID = tx.ID
		return errors.New("transaction does not involve this cluster")
	}

	// Create a response channel
	responseChan := make(chan ServerResponse)

	// Create a TransactionRequest with IsCrossShard = true
	req := TransactionRequest{
		Tx:           tx,
		IsCrossShard: true,
		Reply:        responseChan,
	}

	// Enqueue the transaction
	s.transactionQueue <- req
	log.Printf("Cross-Shard Transaction ID=%s enqueued", tx.ID)

	// Wait for the transaction to be processed
	response := <-responseChan

	// Populate the reply
	*reply = response
	return nil
}

// runPaxos runs the Paxos consensus protocol for a transaction
func (s *Server) runPaxos(tx Transaction, isCrossShard bool) error {
	maxRetries := 1 // Number of times to retry after synchronization
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Assign the next global ballot number
		s.ballotMutex.Lock()
		s.ballotNum += 1
		currentBallot := s.ballotNum
		s.ballotMutex.Unlock()

		// Initialize Paxos state for this transaction if not already present
		s.mu.Lock()
		if _, exists := s.paxosStates[tx.ID]; !exists {
			s.paxosStates[tx.ID] = &PaxosState{
				CommittedTx: Transaction{},
			}
		}
		paxosState := s.paxosStates[tx.ID]
		paxosState.Tx = tx
		s.mu.Unlock()

		// Lock the PaxosState to prevent concurrent modifications
		paxosState.mu.Lock()

		log.Printf("Starting Paxos for Transaction %+v with BallotNum=%d (Attempt %d)", tx, currentBallot, attempt+1)

		// Prepare phase
		prepareArgs := PrepareArgs{
			TransactionID: tx.ID,
			BallotNum:     currentBallot,
			Tx:            tx,
			CallerAddress: s.Address, // Include the server's own address
		}
		var prepareReplies []PeerPrepareReply

		var wg sync.WaitGroup
		var mu sync.Mutex
		successCount := 1 // Include self
		maxPeerBallotNum := currentBallot

		log.Println("Entering Prepare Phase")

		// Send Prepare RPCs concurrently
		for _, peer := range s.peers {
			wg.Add(1)
			go func(peer string) {
				defer wg.Done()
				client, err := rpc.Dial("tcp", peer)
				if err != nil {
					log.Printf("Error connecting to peer %s: %v", peer, err)
					return
				}
				defer client.Close()

				var reply PrepareReply
				err = s.callRPCWithTimeout(peer, "Server.Prepare", prepareArgs, &reply, 2*time.Second)
				if err != nil {
					log.Printf("Prepare RPC to %s failed: %v", peer, err)
					return
				}

				mu.Lock()
				defer mu.Unlock()
				if reply.Promise {
					prepareReplies = append(prepareReplies, PeerPrepareReply{Peer: peer, Reply: reply})
					successCount += 1
					log.Printf("Received Promise from %s", peer)
				} else {
					log.Printf("Received Reject from %s with HighestBallotNum=%d", peer, reply.HighestBallotNum)
					if reply.HighestBallotNum > maxPeerBallotNum {
						maxPeerBallotNum = reply.HighestBallotNum
					}
				}
			}(peer)
		}
		wg.Wait()

		log.Printf("Prepare Phase completed with %d/%d successes", successCount, NUM_SERVERS_PER_SHARD)
		if successCount >= MAJORITY {
			// Attempt to obtain locks on data items x and y
			locksAcquired := []*TryMutex{}

			// Obtain locks for data items x and y if they exist in this shard
			s.mu.Lock()
			var senderLock, receiverLock *TryMutex
			if lock, exists := s.locks[tx.X]; exists {
				senderLock = lock
			}
			if lock, exists := s.locks[tx.Y]; exists {
				receiverLock = lock
			}
			s.mu.Unlock()

			// Try to obtain the locks
			if senderLock != nil {
				if !senderLock.TryLock() {
					log.Printf("Cannot obtain lock on data item x=%d", tx.X)
					// Release any locks acquired
					for _, lock := range locksAcquired {
						lock.Unlock()
					}
					paxosState.mu.Unlock()
					return errors.New("cannot obtain lock on data item x")
				}
				locksAcquired = append(locksAcquired, senderLock)
			}
			if receiverLock != nil {
				if !receiverLock.TryLock() {
					log.Printf("Cannot obtain lock on data item y=%d", tx.Y)
					// Release any locks acquired
					for _, lock := range locksAcquired {
						lock.Unlock()
					}
					paxosState.mu.Unlock()
					return errors.New("cannot obtain lock on data item y")
				}
				locksAcquired = append(locksAcquired, receiverLock)
			}

			// Check the balance of x if it exists in this shard
			balanceSufficient := true
			if senderLock != nil {
				balance, err := s.getBalance(tx.X)
				if err != nil {
					log.Printf("Error getting balance of x=%d: %v", tx.X, err)
					// Release locks
					for _, lock := range locksAcquired {
						lock.Unlock()
					}
					paxosState.mu.Unlock()
					return err
				}
				if balance < tx.Amt {
					log.Printf("Insufficient balance in account x=%d", tx.X)
					balanceSufficient = false
				}
			}
			if !balanceSufficient {
				// Release locks
				for _, lock := range locksAcquired {
					lock.Unlock()
				}
				paxosState.mu.Unlock()
				return errors.New("insufficient balance")
			}

			// Store the locks in paxosState
			paxosState.LocksHeld = locksAcquired

			// Proceed to Accept Phase
			paxosState.mu.Unlock() // Unlock before entering Accept Phase
			err := s.acceptPhase(tx, isCrossShard, paxosState, currentBallot)

			if err != nil {
				if strings.Contains(err.Error(), "retry due to being behind") && attempt < maxRetries {
					log.Printf("Retrying Paxos after synchronization (Attempt %d)", attempt+2)
					continue
				}
				log.Printf("Accept Phase failed for TransactionID=%s: %v", tx.ID, err)
				// Release locks
				for _, lock := range locksAcquired {
					lock.Unlock()
				}
				return err
			}

			return nil // Successfully committed
		}

		// If failed to get majority, check if we are behind
		if maxPeerBallotNum >= currentBallot {
			log.Printf("Leader is behind. Current BallotNum=%d, Max Peer BallotNum=%d. Synchronizing.", currentBallot, maxPeerBallotNum)
			paxosState.mu.Unlock()
			err := s.synchronizeWithBallotNum(maxPeerBallotNum)
			if err != nil {
				log.Printf("Failed to synchronize after Prepare failure: %v", err)
				return err
			}
			// Retry the transaction after synchronization
			continue
		}

		// Otherwise, abort the transaction
		paxosState.mu.Unlock()
		return errors.New("failed to get majority in prepare phase")
	}

	return errors.New("failed to commit transaction after retries")
}

// acceptPhase handles the Accept phase of Paxos
func (s *Server) acceptPhase(tx Transaction, isCrossShard bool, paxosState *PaxosState, currentBallot int) error {
	acceptArgs := AcceptArgs{
		TransactionID: tx.ID,
		BallotNum:     currentBallot,
		Tx:            tx,
	}

	var highestPeerBallotNum int
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 1 // Include self

	log.Println("Entering Accept Phase")

	// Send Accept RPCs concurrently
	for _, peer := range s.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				log.Printf("Error connecting to peer %s: %v", peer, err)
				return
			}
			defer client.Close()

			var reply AcceptReply
			err = client.Call("Server.Accept", acceptArgs, &reply)
			if err != nil {
				log.Printf("Error during Accept RPC to %s: %v", peer, err)
				return
			}

			mu.Lock()
			defer mu.Unlock()
			if reply.Accepted {
				successCount += 1
				log.Printf("Accepted by %s", peer)
			} else {
				log.Printf("Rejected by %s with HighestBallotNum=%d", peer, reply.HighestBallotNum)
				if reply.HighestBallotNum > highestPeerBallotNum {
					highestPeerBallotNum = reply.HighestBallotNum
				}
			}
		}(peer)
	}
	wg.Wait()

	log.Printf("Accept Phase completed with %d/%d successes", successCount, NUM_SERVERS_PER_SHARD)
	if successCount < MAJORITY {
		// Check if we are behind
		if highestPeerBallotNum > currentBallot {
			log.Printf("Leader is behind. Synchronizing with highest peer ballot number %d", highestPeerBallotNum)
			// Synchronize and retry
			err := s.synchronizeWithBallotNum(highestPeerBallotNum)
			if err != nil {
				log.Printf("Synchronization failed: %v", err)
				return err
			}
			return errors.New("retry due to being behind")
		}

		// Abort the transaction
		return errors.New("failed to get majority in accept phase")
	}

	// Commit phase
	log.Println("Entering Commit Phase")

	// Inform peers to commit concurrently for both intra-shard and cross-shard transactions
	commitArgs := CommitArgs{
		Tx:           tx,
		IsCrossShard: isCrossShard,
	}
	for _, peer := range s.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				log.Printf("Error connecting to peer %s: %v", peer, err)
				return
			}
			defer client.Close()

			var reply CommitReply
			err = client.Call("Server.CommitTransaction", commitArgs, &reply)
			if err != nil {
				log.Printf("Error during Commit RPC to %s: %v", peer, err)
				return
			}
			if reply.Success {
				log.Printf("Commit acknowledged by %s", peer)
			} else {
				log.Printf("Commit failed at %s", peer)
			}
		}(peer)
	}
	// Perform commitTransaction locally
	err := s.commitTransaction(tx, isCrossShard)
	if err != nil {
		log.Printf("Local commitTransaction failed for TransactionID=%s: %v", tx.ID, err)
		// Optionally, handle the error (e.g., abort the transaction)
		return err
	}
	wg.Wait()

	if !isCrossShard {
		// For intra-shard transactions, release locks
		if paxosState.LocksHeld != nil {
			for _, lock := range paxosState.LocksHeld {
				lock.Unlock()
			}
			paxosState.LocksHeld = nil
		}
		log.Printf("Transaction %+v successfully committed across cluster", tx)
	} else {
		// For cross-shard transactions, do not release locks here
		log.Printf("Transaction %+v prepared and awaiting client decision", tx)
	}

	return nil
}

// synchronizeWithBallotNum synchronizes the server's state with peers based on the given ballot number
func (s *Server) synchronizeWithBallotNum(ballotNum int) error {
	log.Printf("Synchronizing with peers based on BallotNum=%d", ballotNum)

	// Update the server's ballot number to at least the given ballotNum
	s.ballotMutex.Lock()
	if ballotNum > s.ballotNum {
		log.Printf("Updating global ballotNum from %d to %d during synchronization", s.ballotNum, ballotNum)
		s.ballotNum = ballotNum
	}
	s.ballotMutex.Unlock()

	// Synchronize committed transactions starting from ballotNum
	if len(s.peers) == 0 {
		return nil // No peers to synchronize with
	}
	err := s.synchronizeWithPeer(s.peers[0], ballotNum) // Assuming at least one peer exists
	if err != nil {
		log.Printf("Failed to synchronize with peer during ballotNum synchronization: %v", err)
		return err
	}

	return nil
}

// callRPCWithTimeout calls an RPC method with a timeout
func (s *Server) callRPCWithTimeout(peer string, rpcName string, args interface{}, reply interface{}, timeout time.Duration) error {
	client, err := rpc.Dial("tcp", peer)
	if err != nil {
		return err
	}
	defer client.Close()

	done := make(chan error, 1)
	go func() {
		done <- client.Call(rpcName, args, reply)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("RPC %s to %s timed out", rpcName, peer)
	}
}

// Prepare handles the Prepare phase of Paxos
func (s *Server) Prepare(args PrepareArgs, reply *PrepareReply) error {
	// Check if server is active
	if !s.isActive() {
		log.Printf("Server %s is inactive. Rejecting Prepare RPC for TransactionID=%s", s.Address, args.TransactionID)
		reply.Promise = false
		return errors.New("server is inactive")
	}

	// Synchronize state if necessary
	s.mu.Lock()
	paxosState, exists := s.paxosStates[args.TransactionID]
	if !exists {
		// Initialize Paxos state for this transaction if not present
		paxosState = &PaxosState{
			CommittedTx: Transaction{},
		}
		s.paxosStates[args.TransactionID] = paxosState
	}
	s.mu.Unlock()

	// Lock the PaxosState to prevent concurrent modifications
	paxosState.mu.Lock()
	defer paxosState.mu.Unlock()

	log.Printf("Received Prepare RPC for TransactionID=%s: BallotNum=%d, Transaction=%+v from %s", args.TransactionID, args.BallotNum, args.Tx, args.CallerAddress)

	// **Strict Comparison: Only promise if args.BallotNum > s.ballotNum**
	if args.BallotNum > s.ballotNum {
		log.Printf("Promising for TransactionID=%s: Incoming BallotNum=%d > Current BallotNum=%d", args.TransactionID, args.BallotNum, s.ballotNum)

		// Update the global BallotNum
		s.ballotMutex.Lock()
		s.ballotNum = args.BallotNum
		s.ballotMutex.Unlock()

		// Prepare the reply
		reply.Promise = true
		reply.AcceptNum = 0 // Not tracking per-transaction accept numbers
		reply.AcceptedValue = Transaction{}
		reply.HighestBallotNum = s.ballotNum
	} else {
		log.Printf("Rejecting Prepare for TransactionID=%s: Incoming BallotNum=%d <= Current BallotNum=%d", args.TransactionID, args.BallotNum, s.ballotNum)
		reply.Promise = false
		reply.HighestBallotNum = s.ballotNum
	}

	return nil
}

// Accept handles the Accept phase of Paxos
func (s *Server) Accept(args AcceptArgs, reply *AcceptReply) error {
	// Check if server is active
	if !s.isActive() {
		log.Printf("Server %s is inactive. Rejecting Accept RPC for TransactionID=%s", s.Address, args.TransactionID)
		reply.Accepted = false
		return errors.New("server is inactive")
	}

	// Synchronize global ballot number if incoming ballotNum is higher
	s.ballotMutex.Lock()
	if args.BallotNum > s.ballotNum {
		log.Printf("Updating global ballotNum from %d to %d based on Accept RPC from TransactionID=%s", s.ballotNum, args.BallotNum, args.TransactionID)
		s.ballotNum = args.BallotNum
	}
	s.ballotMutex.Unlock()

	s.mu.Lock()
	paxosState, exists := s.paxosStates[args.TransactionID]
	if !exists {
		// Initialize Paxos state for this transaction if not present
		paxosState = &PaxosState{
			CommittedTx: Transaction{},
		}
		s.paxosStates[args.TransactionID] = paxosState
	}
	s.mu.Unlock()

	// Lock the PaxosState to prevent concurrent modifications
	paxosState.mu.Lock()
	defer paxosState.mu.Unlock()

	log.Printf("Received Accept RPC for TransactionID=%s: BallotNum=%d, Transaction=%+v", args.TransactionID, args.BallotNum, args.Tx)

	// Try to obtain locks on data items x and y only if they exist in this shard
	var senderLock, receiverLock *TryMutex
	if lock, exists := s.locks[args.Tx.X]; exists {
		senderLock = lock
	}
	if lock, exists := s.locks[args.Tx.Y]; exists {
		receiverLock = lock
	}

	if senderLock != nil {
		if !senderLock.TryLock() {
			log.Printf("Cannot obtain lock on data item x=%d", args.Tx.X)
			reply.Accepted = false
			reply.HighestBallotNum = s.ballotNum
			return nil
		}
	}
	if receiverLock != nil {
		if !receiverLock.TryLock() {
			log.Printf("Cannot obtain lock on data item y=%d", args.Tx.Y)
			if senderLock != nil {
				senderLock.Unlock()
			}
			reply.Accepted = false
			return nil
		}
	}

	// Store the locks in paxosState
	paxosState.LocksHeld = []*TryMutex{}
	if senderLock != nil {
		paxosState.LocksHeld = append(paxosState.LocksHeld, senderLock)
	}
	if receiverLock != nil {
		paxosState.LocksHeld = append(paxosState.LocksHeld, receiverLock)
	}

	// Accept the proposal
	reply.Accepted = true
	reply.HighestBallotNum = s.ballotNum
	paxosState.Tx = args.Tx
	log.Printf("Accepted for TransactionID=%s: BallotNum=%d", args.TransactionID, args.BallotNum)

	return nil
}

// CommitTransaction commits the transaction
func (s *Server) CommitTransaction(args CommitArgs, reply *CommitReply) error {
	log.Printf("Received CommitTransaction RPC for TransactionID=%s, IsCrossShard=%v", args.Tx.ID, args.IsCrossShard)

	// Check if server is active
	if !s.isActive() {
		log.Printf("Server %s is inactive. Cannot handle CommitTransaction.", s.Address)
		reply.Success = false
		return errors.New("server is inactive")
	}

	s.mu.Lock()
	paxosState, exists := s.paxosStates[args.Tx.ID]
	if !exists {
		s.mu.Unlock()
		log.Printf("No Paxos state found for TransactionID=%s. Possibly already committed.", args.Tx.ID)
		reply.Success = true // Treat as already committed
		return nil
	}
	s.mu.Unlock()

	// Lock the PaxosState to prevent concurrent modifications
	paxosState.mu.Lock()
	defer paxosState.mu.Unlock()

	// Commit the transaction
	err := s.commitTransaction(args.Tx, args.IsCrossShard)
	if err != nil {
		log.Printf("Commit failed for Transaction %+v: %v", args.Tx, err)
		reply.Success = false
		return err
	}

	// For intra-shard transactions, release locks and cleanup
	if !args.IsCrossShard {
		// Release locks
		if paxosState.LocksHeld != nil {
			for _, lock := range paxosState.LocksHeld {
				lock.Unlock()
			}
			paxosState.LocksHeld = nil
		}
		// Remove the transaction from PaxosState
		s.mu.Lock()
		delete(s.paxosStates, args.Tx.ID)
		s.mu.Unlock()
	}

	reply.Success = true
	log.Printf("Transaction Committed: %+v", args.Tx)

	return nil
}

// GetCommittedTransactions handles the GetCommittedTransactions RPC call
func (s *Server) GetCommittedTransactions(args GetCommittedArgs, reply *GetCommittedReply) error {
	log.Printf("Received GetCommittedTransactions RPC: FromBallotNum=%d", args.FromBallotNum)
	var committedTxs []CommittedTransaction
	err := s.db.View(func(btx *bolt.Tx) error {
		committedBucket := btx.Bucket([]byte("CommittedTransactions"))
		if committedBucket == nil {
			// No committed transactions yet
			reply.CommittedTxs = []CommittedTransaction{}
			return nil
		}

		return committedBucket.ForEach(func(k, v []byte) error {
			ct, err := decodeCommittedTransaction(v)
			if err != nil {
				return err
			}
			if ct.BallotNum >= args.FromBallotNum {
				committedTxs = append(committedTxs, ct)
			}
			return nil
		})
	})
	if err != nil {
		log.Printf("Error fetching committed transactions: %v", err)
		return err
	}
	reply.CommittedTxs = committedTxs
	log.Printf("Returning %d committed transactions", len(committedTxs))
	return nil
}

// synchronizeWithPeer fetches and applies committed transactions from a peer starting from a specific BallotNum
func (s *Server) synchronizeWithPeer(peer string, fromBallotNum int) error {
	log.Printf("Synchronizing with peer %s starting from BallotNum=%d", peer, fromBallotNum)
	client, err := rpc.Dial("tcp", peer)
	if err != nil {
		log.Printf("Error connecting to peer %s for synchronization: %v", peer, err)
		return err
	}
	defer client.Close()

	var reply GetCommittedReply
	args := GetCommittedArgs{FromBallotNum: fromBallotNum}
	err = client.Call("Server.GetCommittedTransactions", args, &reply)
	if err != nil {
		log.Printf("Error during GetCommittedTransactions RPC to %s: %v", peer, err)
		return err
	}

	log.Printf("Fetched %d committed transactions from %s", len(reply.CommittedTxs), peer)

	// Sort the fetched transactions by BallotNum to ensure order
	sort.Slice(reply.CommittedTxs, func(i, j int) bool {
		return reply.CommittedTxs[i].BallotNum < reply.CommittedTxs[j].BallotNum
	})

	// Apply each committed transaction
	for _, ct := range reply.CommittedTxs {
		err := s.applyCommittedTransaction(ct)
		if err != nil {
			log.Printf("Failed to apply committed transaction %+v: %v", ct, err)
			return err
		}
	}

	// Update the ballotNum to the highest received if necessary
	s.ballotMutex.Lock()
	for _, ct := range reply.CommittedTxs {
		if ct.BallotNum > s.ballotNum {
			log.Printf("Updating global ballotNum from %d to %d based on committed transaction from %s", s.ballotNum, ct.BallotNum, peer)
			s.ballotNum = ct.BallotNum
		}
	}
	s.ballotMutex.Unlock()

	return nil
}

// applyCommittedTransaction applies a single committed transaction to the local state
func (s *Server) applyCommittedTransaction(ct CommittedTransaction) error {
	log.Printf("Applying committed transaction %+v with BallotNum=%d", ct.Tx, ct.BallotNum)

	// Check if the transaction is already committed
	alreadyCommitted, err := s.isTransactionCommitted(ct.Tx.ID)
	if err != nil {
		return err
	}
	if alreadyCommitted {
		log.Printf("TransactionID=%s is already committed. Skipping.", ct.Tx.ID)
		return nil
	}

	// Determine if the transaction is cross-shard
	isCrossShard := getShardForDataItem(ct.Tx.X) != getShardForDataItem(ct.Tx.Y)

	// Apply the transaction
	err = s.commitTransaction(ct.Tx, isCrossShard)
	if err != nil {
		return err
	}

	return nil
}

// isTransactionCommitted checks if a transaction is already committed
func (s *Server) isTransactionCommitted(txID string) (bool, error) {
	var committed bool
	err := s.db.View(func(btx *bolt.Tx) error {
		committedBucket := btx.Bucket([]byte("CommittedTransactions"))
		if committedBucket == nil {
			// If the bucket doesn't exist, no transactions have been committed yet.
			committed = false
			return nil
		}

		txBytes := committedBucket.Get([]byte(txID))
		if txBytes != nil {
			committed = true
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return committed, nil
}

// isActive checks if the server is currently active
func (s *Server) isActive() bool {
	s.activeMutex.RLock()
	defer s.activeMutex.RUnlock()
	return s.active
}

// setActive sets the server's active status
func (s *Server) setActive(status bool) {
	s.activeMutex.Lock()
	defer s.activeMutex.Unlock()
	s.active = status
	log.Printf("Server %s active status set to %v", s.Address, s.active)
}

// commitTransaction performs the actual commit of a transaction
func (s *Server) commitTransaction(tx Transaction, isCrossShard bool) error {
	log.Printf("Committing Transaction: %+v", tx)

	// Perform the transaction
	err := s.db.Update(func(btx *bolt.Tx) error {
		accountsBucket, err := btx.CreateBucketIfNotExists([]byte("Accounts"))
		if err != nil {
			return err
		}

		// For cross-shard transactions, record previous state in WAL (optional)
		if isCrossShard {
			walBucket, err := btx.CreateBucketIfNotExists([]byte("WriteAheadLog"))
			if err != nil {
				return err
			}

			walRecord := WALRecord{
				TxID:      tx.ID,
				Timestamp: time.Now(),
			}

			// Store balances of X and Y if they are in this shard
			if s.locks[tx.X] != nil {
				senderKey := intToBytes(tx.X)
				senderBalanceBytes := accountsBucket.Get(senderKey)
				walRecord.X = tx.X
				walRecord.BalanceX = bytesToInt(senderBalanceBytes)
			}
			if s.locks[tx.Y] != nil {
				receiverKey := intToBytes(tx.Y)
				receiverBalanceBytes := accountsBucket.Get(receiverKey)
				walRecord.Y = tx.Y
				walRecord.BalanceY = bytesToInt(receiverBalanceBytes)
			}

			// Store WAL record
			walBytes, err := encodeWALRecord(walRecord)
			if err != nil {
				return err
			}
			err = walBucket.Put([]byte(tx.ID), walBytes)
			if err != nil {
				return err
			}
			log.Printf("WAL Record created for TransactionID=%s", tx.ID)
		}

		// Apply the transaction to the accounts
		// Sender account
		if s.locks[tx.X] != nil {
			senderKey := intToBytes(tx.X)
			senderBalanceBytes := accountsBucket.Get(senderKey)
			if senderBalanceBytes == nil {
				// Initialize account with 10 units
				senderBalanceBytes = intToBytes(10)
				log.Printf("Initializing Sender Account X=%d with default balance=10", tx.X)
			}
			senderBalance := bytesToInt(senderBalanceBytes)
			senderBalance -= tx.Amt
			log.Printf("Updating Sender Account X=%d: New Balance=%d", tx.X, senderBalance)

			// Save updated balance
			err = accountsBucket.Put(senderKey, intToBytes(senderBalance))
			if err != nil {
				log.Printf("Failed to update Sender Account X=%d: %v", tx.X, err)
				return err
			}
		}

		// Receiver account
		if s.locks[tx.Y] != nil {
			receiverKey := intToBytes(tx.Y)
			receiverBalanceBytes := accountsBucket.Get(receiverKey)
			if receiverBalanceBytes == nil {
				// Initialize account with 10 units
				receiverBalanceBytes = intToBytes(10)
				log.Printf("Initializing Receiver Account Y=%d with default balance=10", tx.Y)
			}
			receiverBalance := bytesToInt(receiverBalanceBytes)
			receiverBalance += tx.Amt
			log.Printf("Updating Receiver Account Y=%d: New Balance=%d", tx.Y, receiverBalance)

			// Save updated balance
			err = accountsBucket.Put(receiverKey, intToBytes(receiverBalance))
			if err != nil {
				log.Printf("Failed to update Receiver Account Y=%d: %v", tx.Y, err)
				return err
			}
		}

		// Store the committed transaction with BallotNum
		committedBucket, err := btx.CreateBucketIfNotExists([]byte("CommittedTransactions"))
		if err != nil {
			return err
		}

		ct := CommittedTransaction{
			BallotNum: s.ballotNum,
			Tx:        tx,
		}
		txID := tx.ID // Use Transaction ID as the key
		txBytes, err := encodeCommittedTransaction(ct)
		if err != nil {
			return err
		}

		err = committedBucket.Put([]byte(txID), txBytes)
		if err != nil {
			return err
		}
		log.Printf("Stored Committed Transaction in DB: %+v", ct)

		// **New Code: Store the DatastoreEntry**
		datastoreBucket, err := btx.CreateBucketIfNotExists([]byte("DatastoreEntries"))
		if err != nil {
			return err
		}

		// Create the DatastoreEntry
		de := DatastoreEntry{
			BallotNum:   s.ballotNum,
			Type:        "",
			Transaction: tx,
		}
		if isCrossShard {
			de.Type = "P"
		}

		// Use NextSequence() to get a unique key
		seq, err := datastoreBucket.NextSequence()
		if err != nil {
			return err
		}
		key := itob(seq)

		// Encode and store the DatastoreEntry
		deBytes, err := encodeDatastoreEntry(de)
		if err != nil {
			return err
		}
		err = datastoreBucket.Put(key, deBytes)
		if err != nil {
			return err
		}
		log.Printf("Stored DatastoreEntry: %+v", de)

		return nil
	})
	if err != nil {
		log.Printf("Failed to commit Transaction %+v: %v", tx, err)
		return err
	}

	if !isCrossShard {
		log.Printf("Intra-shard Transaction committed: %+v", tx)
	} else {
		log.Printf("Cross-shard Transaction committed and written to WAL: %+v", tx)
		// Do not release locks here for cross-shard transactions
	}

	return nil
}

// Helper function to convert uint64 to []byte
func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// encodeWALRecord encodes a WALRecord into bytes
func encodeWALRecord(wal WALRecord) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(wal)
	if err != nil {
		log.Printf("Failed to encode WALRecord %+v: %v", wal, err)
	}
	return buf.Bytes(), err
}

// decodeWALRecord decodes bytes into a WALRecord
func decodeWALRecord(data []byte) (WALRecord, error) {
	var wal WALRecord
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&wal)
	return wal, err
}

// CommitCrossShardArgs represents the arguments for the CommitCrossShardTransaction RPC
type CommitCrossShardArgs struct {
	TransactionID string
	IsForwarded   bool
}

// CommitCrossShardReply represents the reply for the CommitCrossShardTransaction RPC
type CommitCrossShardReply struct {
	Success bool
}

// AbortCrossShardArgs represents the arguments for the AbortCrossShardTransaction RPC
type AbortCrossShardArgs struct {
	TransactionID string
	IsForwarded   bool
}

// AbortCrossShardReply represents the reply for the AbortCrossShardTransaction RPC
type AbortCrossShardReply struct {
	Success bool
}

// CommitCrossShardTransaction handles the commit of a cross-shard transaction
func (s *Server) CommitCrossShardTransaction(args CommitCrossShardArgs, reply *CommitCrossShardReply) error {
	log.Printf("Received CommitCrossShardTransaction RPC for TransactionID=%s, IsForwarded=%v", args.TransactionID, args.IsForwarded)

	s.mu.Lock()
	paxosState, exists := s.paxosStates[args.TransactionID]
	if !exists {
		s.mu.Unlock()
		log.Printf("No Paxos state found for TransactionID=%s. Cannot commit.", args.TransactionID)
		reply.Success = false
		return errors.New("no Paxos state found for transaction")
	}
	s.mu.Unlock()

	// Lock the PaxosState to prevent concurrent modifications
	paxosState.mu.Lock()
	defer paxosState.mu.Unlock()

	// Release locks held
	if paxosState.LocksHeld != nil {
		for _, lock := range paxosState.LocksHeld {
			lock.Unlock()
		}
		paxosState.LocksHeld = nil
	}

	// Append 'C' entry to DatastoreEntries
	err := s.db.Update(func(btx *bolt.Tx) error {
		datastoreBucket, err := btx.CreateBucketIfNotExists([]byte("DatastoreEntries"))
		if err != nil {
			return err
		}

		// Retrieve the transaction from CommittedTransactions
		var ct CommittedTransaction
		committedBucket := btx.Bucket([]byte("CommittedTransactions"))
		if committedBucket == nil {
			return errors.New("CommittedTransactions bucket not found")
		}
		txBytes := committedBucket.Get([]byte(args.TransactionID))
		if txBytes == nil {
			return errors.New("Transaction not found in CommittedTransactions")
		}
		ct, err = decodeCommittedTransaction(txBytes)
		if err != nil {
			return err
		}

		// Create the DatastoreEntry with Type 'C'
		de := DatastoreEntry{
			BallotNum:   ct.BallotNum,
			Type:        "C",
			Transaction: ct.Tx,
		}

		// Use NextSequence() to get a unique key
		seq, err := datastoreBucket.NextSequence()
		if err != nil {
			return err
		}
		key := itob(seq)

		// Encode and store the DatastoreEntry
		deBytes, err := encodeDatastoreEntry(de)
		if err != nil {
			return err
		}
		err = datastoreBucket.Put(key, deBytes)
		if err != nil {
			return err
		}
		log.Printf("Appended 'C' DatastoreEntry: %+v", de)
		return nil
	})

	if err != nil {
		log.Printf("Failed to append 'C' entry to DatastoreEntries: %v", err)
		reply.Success = false
		return err
	}

	// Cleanup PaxosState after commit
	s.mu.Lock()
	delete(s.paxosStates, args.TransactionID)
	s.mu.Unlock()

	reply.Success = true
	log.Printf("Transaction Committed: %s", args.TransactionID)

	// Propagate commit to peers if not already forwarded
	if !args.IsForwarded {
		var wg sync.WaitGroup
		for _, peer := range s.peers {
			wg.Add(1)
			go func(peer string) {
				defer wg.Done()
				client, err := rpc.Dial("tcp", peer)
				if err != nil {
					log.Printf("Error connecting to peer %s for commit propagation: %v", peer, err)
					return
				}
				defer client.Close()

				forwardArgs := CommitCrossShardArgs{
					TransactionID: args.TransactionID,
					IsForwarded:   true, // Set the flag to true to prevent further propagation
				}

				var peerReply CommitCrossShardReply
				err = client.Call("Server.CommitCrossShardTransaction", forwardArgs, &peerReply)
				if err != nil {
					log.Printf("Error during CommitCrossShardTransaction RPC to %s: %v", peer, err)
					return
				}
				if peerReply.Success {
					log.Printf("Commit propagated to %s", peer)
				}
			}(peer)
		}
		wg.Wait()
	}

	return nil
}

// AbortCrossShardTransaction handles the abort of a cross-shard transaction
func (s *Server) AbortCrossShardTransaction(args AbortCrossShardArgs, reply *AbortCrossShardReply) error {
	log.Printf("Received AbortCrossShardTransaction RPC for TransactionID=%s, IsForwarded=%v", args.TransactionID, args.IsForwarded)

	// Check if the abort request has already been forwarded
	if !args.IsForwarded {
		// Forward the abort to peers with IsForwarded=true to prevent further propagation
		var wg sync.WaitGroup
		for _, peer := range s.peers {
			wg.Add(1)
			go func(peer string) {
				defer wg.Done()
				client, err := rpc.Dial("tcp", peer)
				if err != nil {
					log.Printf("Error connecting to peer %s for abort propagation: %v", peer, err)
					return
				}
				defer client.Close()

				forwardArgs := AbortCrossShardArgs{
					TransactionID: args.TransactionID,
					IsForwarded:   true, // Set to true to indicate forwarded abort
				}

				var peerReply AbortCrossShardReply
				err = client.Call("Server.AbortCrossShardTransaction", forwardArgs, &peerReply)
				if err != nil {
					log.Printf("Error during AbortCrossShardTransaction RPC to %s: %v", peer, err)
					return
				}
				if peerReply.Success {
					log.Printf("Abort propagated to %s", peer)
				}
			}(peer)
		}
		wg.Wait()
	}

	// Proceed with aborting the transaction locally
	s.mu.Lock()
	paxosState, exists := s.paxosStates[args.TransactionID]
	if !exists {
		s.mu.Unlock()
		log.Printf("No Paxos state found for TransactionID=%s. Cannot abort.", args.TransactionID)
		reply.Success = false
		return errors.New("no Paxos state found for transaction")
	}
	s.mu.Unlock()

	// Lock the PaxosState to prevent concurrent modifications
	paxosState.mu.Lock()
	defer paxosState.mu.Unlock()

	tx := paxosState.Tx // Retrieve the transaction

	// Use WAL to undo transaction and delete WAL entry
	err := s.db.Update(func(btx *bolt.Tx) error {
		walBucket := btx.Bucket([]byte("WriteAheadLog"))
		if walBucket == nil {
			return errors.New("WAL bucket not found")
		}
		walBytes := walBucket.Get([]byte(args.TransactionID))
		if walBytes == nil {
			return errors.New("WAL entry not found")
		}
		walRecord, err := decodeWALRecord(walBytes)
		if err != nil {
			return err
		}

		accountsBucket := btx.Bucket([]byte("Accounts"))
		if accountsBucket == nil {
			return errors.New("accounts bucket not found")
		}

		// Restore balances from WAL
		if walRecord.X != 0 {
			err = accountsBucket.Put(intToBytes(walRecord.X), intToBytes(walRecord.BalanceX))
			if err != nil {
				return err
			}
		}
		if walRecord.Y != 0 {
			err = accountsBucket.Put(intToBytes(walRecord.Y), intToBytes(walRecord.BalanceY))
			if err != nil {
				return err
			}
		}

		// Delete WAL entry
		err = walBucket.Delete([]byte(args.TransactionID))
		if err != nil {
			return err
		}
		log.Printf("TransactionID=%s rolled back using WAL", args.TransactionID)

		// **New Code: Append 'A' entry to DatastoreEntries**
		datastoreBucket, err := btx.CreateBucketIfNotExists([]byte("DatastoreEntries"))
		if err != nil {
			return err
		}

		// Create the DatastoreEntry with Type 'A'
		de := DatastoreEntry{
			BallotNum:   s.ballotNum, // Use appropriate ballot number if available
			Type:        "A",
			Transaction: tx,
		}

		// Use NextSequence() to get a unique key
		seq, err := datastoreBucket.NextSequence()
		if err != nil {
			return err
		}
		key := itob(seq)

		// Encode and store the DatastoreEntry
		deBytes, err := encodeDatastoreEntry(de)
		if err != nil {
			return err
		}
		err = datastoreBucket.Put(key, deBytes)
		if err != nil {
			return err
		}
		log.Printf("Appended 'A' DatastoreEntry: %+v", de)

		return nil
	})
	if err != nil {
		log.Printf("Failed to abort TransactionID=%s: %v", args.TransactionID, err)
		reply.Success = false
		return err
	}

	// Release locks held
	if paxosState.LocksHeld != nil {
		for _, lock := range paxosState.LocksHeld {
			lock.Unlock()
		}
		paxosState.LocksHeld = nil
	}

	// Cleanup PaxosState after abort
	s.mu.Lock()
	delete(s.paxosStates, args.TransactionID)
	s.mu.Unlock()

	reply.Success = true
	log.Printf("Cross-shard Transaction Aborted: %s", args.TransactionID)

	return nil
}

// encodeDatastoreEntry encodes a DatastoreEntry into bytes
func encodeDatastoreEntry(de DatastoreEntry) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(de)
	if err != nil {
		log.Printf("Failed to encode DatastoreEntry %+v: %v", de, err)
	}
	return buf.Bytes(), err
}

// decodeDatastoreEntry decodes bytes into a DatastoreEntry
func decodeDatastoreEntry(data []byte) (DatastoreEntry, error) {
	var de DatastoreEntry
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&de)
	return de, err
}

// getBalance returns the balance of a specified data item.
// Assumes that all accounts have been initialized.
func (s *Server) getBalance(itemID int) (int, error) {
	var balance int
	err := s.db.View(func(btx *bolt.Tx) error {
		bucket := btx.Bucket([]byte("Accounts"))
		if bucket == nil {
			return errors.New("accounts bucket not found")
		}

		balanceBytes := bucket.Get(intToBytes(itemID))
		if balanceBytes == nil {
			return fmt.Errorf("account X=%d not found", itemID)
		}

		balance = bytesToInt(balanceBytes)
		log.Printf("Account X=%d has balance=%d", itemID, balance)

		return nil
	})
	if err != nil {
		return 0, err
	}
	return balance, nil
}

// Handle SetActive RPC call
func (s *Server) SetActive(args SetActiveArgs, reply *SetActiveReply) error {
	log.Printf("Received SetActive RPC. Setting active status to %v.", args.Active)
	s.setActive(args.Active)

	if args.Active {
		log.Printf("Server %s is now active.", s.Address)
	} else {
		log.Printf("Server %s is now inactive.", s.Address)
	}

	return nil
}

// getShardForDataItem determines the shard for a given data item
func getShardForDataItem(itemID int) int {
	return (itemID - 1) / DATA_ITEMS_PER_SHARD
}

// encodeCommittedTransaction encodes a CommittedTransaction into bytes
func encodeCommittedTransaction(ct CommittedTransaction) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(ct)
	if err != nil {
		log.Printf("Failed to encode CommittedTransaction %+v: %v", ct, err)
	}
	return buf.Bytes(), err
}

// decodeCommittedTransaction decodes bytes into a CommittedTransaction
func decodeCommittedTransaction(data []byte) (CommittedTransaction, error) {
	var ct CommittedTransaction
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&ct)
	return ct, err
}

// Utility functions to convert between int and []byte
func intToBytes(n int) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(n)
	return buf.Bytes()
}

func bytesToInt(b []byte) int {
	var n int
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	dec.Decode(&n)
	return n
}

// main function
func main() {
	// Server address (e.g., "localhost:5001")
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run server.go <address>")
		return
	}
	address := os.Args[1]

	// Initialize server
	server := NewServer(address)

	// Register RPC methods
	err := rpc.RegisterName("Server", server)
	if err != nil {
		log.Fatalf("RPC Registration failed: %v", err)
	}
	log.Println("RPC methods registered successfully")

	// Start listening
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Listener error on %s: %v", address, err)
	}
	defer listener.Close()
	log.Printf("Server is listening on %s", address)

	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Connection accept error: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

// GetBalance retrieves the balance for a specified data item (client ID)
func (s *Server) GetBalance(clientID int, balance *int) error {
	log.Printf("Received GetBalance RPC for ClientID=%d", clientID)

	err := s.db.View(func(btx *bolt.Tx) error {
		bucket := btx.Bucket([]byte("Accounts"))
		if bucket == nil {
			return errors.New("accounts bucket not found")
		}

		balanceBytes := bucket.Get(intToBytes(clientID))
		if balanceBytes == nil {
			*balance = 0 // Assuming 0 balance for non-existing accounts
			return nil
		}

		*balance = bytesToInt(balanceBytes)
		return nil
	})
	if err != nil {
		log.Printf("Error retrieving balance for ClientID=%d: %v", clientID, err)
		return err
	}

	log.Printf("Retrieved balance for ClientID=%d: %d", clientID, *balance)
	return nil
}

// GetDatastoreArgs represents the arguments for the GetDatastore RPC call
type GetDatastoreArgs struct{}

// GetDatastoreReply represents the reply for the GetDatastore RPC call
type GetDatastoreReply struct {
	Entries []DatastoreEntry
}

// GetDatastore retrieves all entries in the datastore
func (s *Server) GetDatastore(args GetDatastoreArgs, reply *GetDatastoreReply) error {
	log.Printf("Received GetDatastore RPC")

	var entries []DatastoreEntry
	err := s.db.View(func(btx *bolt.Tx) error {
		datastoreBucket := btx.Bucket([]byte("DatastoreEntries"))
		if datastoreBucket == nil {
			reply.Entries = []DatastoreEntry{}
			return nil
		}

		c := datastoreBucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			de, err := decodeDatastoreEntry(v)
			if err != nil {
				return err
			}
			entries = append(entries, de)
		}
		return nil
	})
	if err != nil {
		log.Printf("Error fetching datastore entries: %v", err)
		return err
	}
	reply.Entries = entries
	log.Printf("Returning %d datastore entries", len(entries))
	return nil
}
