// server.go
package main

import (
	"dslab/models" // Importing the models package
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Server represents a bank server handling transactions.
type Server struct {
	ClientID          string
	Port              string
	Peers             []string
	Balance           float64
	LocalLog          []models.Transaction
	Datastore         []models.MajorBlock
	PendingQueue      []models.Transaction
	Mutex             sync.Mutex
	cond              *sync.Cond
	AcceptNum         int
	AcceptVal         *models.MajorBlock
	MaxBallot         int
	LastCommitted     int
	IsAlive           bool
	TransactionID     int
	RetryCount        int // Tracks the number of consensus retries
	MaxRetries        int // Maximum number of consensus retries
	isProcessing      bool
	LatencyData       []time.Duration
	TotalTransactions int
	StartTime         time.Time
}

// Initialize sets up the RPC server and starts listening for incoming connections.
func (s *Server) Initialize() {
	err := rpc.Register(s)
	if err != nil {
		log.Fatalf("Failed to register RPC methods for server %s: %v", s.ClientID, err)
	}

	listener, err := net.Listen("tcp", ":"+s.Port)
	if err != nil {
		log.Fatalf("Server %s failed to start: %v", s.ClientID, err)
	}
	defer listener.Close()

	// log.Printf("[Server %s] Listening on port %s", s.ClientID, s.Port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			// log.Printf("[Server %s] Error accepting connection: %v", s.ClientID, err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

// Accept handles the Accept message from the leader. It will not respond if the server is not alive.
func (s *Server) Accept(majorBlock *models.MajorBlock, response *bool) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if !s.IsAlive {
		// Do not respond if the server is marked as dead
		// log.Printf("[Server %s] Promise request ignored as server is down.", s.ClientID)
		return nil
	}

	if majorBlock.BallotNumber >= s.MaxBallot {
		s.AcceptNum = majorBlock.BallotNumber
		s.AcceptVal = majorBlock
		s.MaxBallot = majorBlock.BallotNumber
		*response = true
		// log.Printf("[Server %s] Accepted Major Block with BallotNumber %d.", s.ClientID, majorBlock.BallotNumber)
	} else {
		*response = false
		// log.Printf("[Server %s] Rejected Major Block with BallotNumber %d (Current MaxBallot: %d).", s.ClientID, majorBlock.BallotNumber, s.MaxBallot)
	}

	return nil
}

// ReceiveTransaction processes incoming transactions from clients.
func (s *Server) ReceiveTransaction(transaction models.Transaction, reply *string) error {
	startTime := time.Now()
	// log.Printf("[Server %s] Received a new transaction from client: %+v", s.ClientID, transaction)

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	var transactionProcessed bool = false

	if !s.IsAlive {
		// Server is marked as dead: allow adding to the queue but don't initiate consensus.
		// Simply add it to the PendingQueue.
		transaction.ID = s.TransactionID
		transaction.Timestamp = time.Now()
		s.TransactionID++
		s.PendingQueue = append(s.PendingQueue, transaction)
		*reply = fmt.Sprintf("Server %s is down for this set. Transaction added to pending queue but not processed.", s.ClientID)
		// log.Printf("[Server %s] Transaction added to PendingQueue without processing as server is down: %+v", s.ClientID, transaction)
		return nil
	}
	if transaction.Sender == s.ClientID {
		currentBalance := s.calculateBalance()
		// log.Printf("[Server %s] Current Balance: %.2f | Required Amount: %.2f", s.ClientID, currentBalance, transaction.Amount)

		if currentBalance >= transaction.Amount {
			// Sufficient balance: add transaction to the local log.
			transaction.ID = s.TransactionID
			transaction.Timestamp = time.Now()
			s.TransactionID++
			s.LocalLog = append(s.LocalLog, transaction)
			transactionProcessed = true
			*reply = fmt.Sprintf("Transaction added to local log by server %s", s.ClientID)
			// log.Printf("[Server %s] Transaction added to LocalLog: %+v", s.ClientID, transaction)
			// log.Printf("[Server %s] Updated Balance: %.2f", s.ClientID, s.calculateBalance())
		} else {
			// Insufficient balance: add to PendingQueue but do not initiate consensus if not alive.
			transaction.ID = s.TransactionID
			transaction.Timestamp = time.Now()
			s.TransactionID++
			s.PendingQueue = append(s.PendingQueue, transaction)
			*reply = fmt.Sprintf("Insufficient balance on server %s. Transaction added to pending queue.", s.ClientID)
			// log.Printf("[Server %s] Insufficient balance. Transaction added to PendingQueue: %+v", s.ClientID, transaction)

			if s.IsAlive {
				// log.Printf("[Server %s] Initiating Paxos consensus for pending transactions.", s.ClientID)
				go s.initiateConsensus()
			}
		}
	} else {
		// Incoming transaction from another server.
		transactionProcessed = true
		transaction.ID = s.TransactionID
		transaction.Timestamp = time.Now()
		s.TransactionID++
		s.LocalLog = append(s.LocalLog, transaction)
		*reply = fmt.Sprintf("Transaction received by server %s", s.ClientID)
		// log.Printf("[Server %s] Incoming transaction added to LocalLog: %+v", s.ClientID, transaction)
		// log.Printf("[Server %s] Updated Balance: %.2f", s.ClientID, s.calculateBalance())
	}
	if transactionProcessed {
		endTime := time.Now()
		latency := endTime.Sub(startTime)
		s.LatencyData = append(s.LatencyData, latency)
		s.TotalTransactions++
	}

	return nil
}

// calculateBalance calculates the current balance of the server's client.
func (s *Server) calculateBalance() float64 {
	balance := s.Balance

	// Iterate through committed blocks in the datastore.
	for _, block := range s.Datastore {
		for _, tx := range block.Transactions {
			if tx.Sender == s.ClientID {
				balance -= tx.Amount
			}
			if tx.Receiver == s.ClientID {
				balance += tx.Amount
			}
		}
	}

	// Iterate through the local log for uncommitted transactions.
	for _, tx := range s.LocalLog {
		if tx.Sender == s.ClientID {
			balance -= tx.Amount
		}
		if tx.Receiver == s.ClientID {
			balance += tx.Amount
		}
	}

	return balance
}

var RetryConsensus = []models.PromiseResponse(nil)

// preparePhaseWithTimeout sends Prepare messages to all peers and waits for promises with a timeout.
func (s *Server) preparePhaseWithTimeout(ballotNumber int, timeoutDuration time.Duration) ([]models.PromiseResponse, bool) {
	// log.Printf("[Server %s] [Leader] Sending Prepare messages to peers.", s.ClientID)

	var promiseResponses []models.PromiseResponse
	var mu sync.Mutex
	var wg sync.WaitGroup
	promiseChannel := make(chan models.PromiseResponse, len(s.Peers))
	timeout := time.After(timeoutDuration)
	retry := false

	// Send Prepare messages to all peers
	for _, peer := range s.Peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				// log.Printf("[Server %s] PreparePhase: Error connecting to peer %s: %v", s.ClientID, peer, err)
				return
			}
			defer client.Close()

			var response models.PromiseResponse
			// Include LastCommitted sequence number in the request
			request := models.PrepareRequest{
				BallotNumber:    ballotNumber,
				LeaderCommitted: s.LastCommitted,
				LeaderAddress:   "localhost:" + s.Port,
			}
			err = client.Call("Server.Promise", request, &response)
			if err != nil {
				// log.Printf("[Server %s] PreparePhase: Error calling Promise on peer %s: %v", s.ClientID, peer, err)
				return
			}

			// Send the response to the channel
			promiseChannel <- response
		}(peer)
	}

	// Wait for responses or timeout
	go func() {
		wg.Wait()
		close(promiseChannel) // Close the channel when all responses are collected
	}()

	// Collect responses until timeout or until the channel is closed
	for {
		select {
		case response, ok := <-promiseChannel:
			if !ok {
				return promiseResponses, retry // Channel is closed, return collected responses
			}

			if !response.Success && len(response.MissingBlocks) > 0 {
				// log.Printf("Server is missing some blocks! Applying missing blocks")
				// Leader is behind, apply missing blocks
				s.applyMissingBlocks(response.MissingBlocks)
				// log.Printf("[Server %s] Applied missing blocks from follower. Restarting consensus.", s.ClientID)
				// Stop current consensus and restart
				return nil, retry // Returning nil to indicate restart
			}

			// If the promise was rejected due to a higher MaxBallot, update leader's ballot number
			if !response.Success && response.CurrentMaxBallot > s.MaxBallot {
				s.MaxBallot = response.CurrentMaxBallot
				// Retry consensus with the updated MaxBallot
				// return nil // Returning nil to indicate retry
				retry = true
				return nil, retry
			}

			mu.Lock()
			promiseResponses = append(promiseResponses, response)
			mu.Unlock()
		case <-timeout:
			// log.Printf("[Server %s] PreparePhase: Timeout occurred while waiting for Promises.", s.ClientID)
			return promiseResponses, retry
		}
	}
}

func (s *Server) initiateConsensus() {
	if !s.IsAlive {
		// log.Printf("[Server %s] Cannot initiate consensus as the server is not alive.", s.ClientID)
		return
	}

	s.Mutex.Lock()

	if len(s.PendingQueue) == 0 {
		s.Mutex.Unlock()
		// log.Printf("[Server %s] No pending transactions to process.", s.ClientID)
		return
	}

	// Set the processing flag
	s.isProcessing = true

	// Process one transaction at a time from the PendingQueue
	// triggeringTransaction := s.PendingQueue[0]
	s.Mutex.Unlock()

	// log.Printf("[Server %s] Starting Paxos consensus for triggering transaction: %+v", s.ClientID, triggeringTransaction)

	s.Mutex.Lock()

	// Increment the ballot number
	s.MaxBallot++
	ballotNumber := s.MaxBallot
	s.Mutex.Unlock()

	// Prepare Phase
	// log.Printf("[Server %s] [Leader] Entering Prepare phase with BallotNumber %d", s.ClientID, ballotNumber)
	promiseResponses, upDatedBallotNumber := s.preparePhaseWithTimeout(ballotNumber, 1*time.Second) // Wait for 1 second for promises

	if upDatedBallotNumber {
		s.finalizeConsensus(false)
		go s.initiateConsensus()
		return
	}

	// Check if promiseResponses is nil, indicating leader was behind and needs to sync
	if promiseResponses == nil {
		// log.Printf("[Server %s] Leader was behind and has synced missing blocks. Restarting consensus.", s.ClientID)
		s.finalizeConsensus(false)
		go s.initiateConsensus()
		return
	}

	// Check if majority promises were received
	if len(promiseResponses) < len(s.Peers)/2 {
		// log.Printf("[Server %s] [Leader] Failed to receive majority promises. Retrying consensus.", s.ClientID)
		s.finalizeConsensus(false)
		time.Sleep(1 * time.Second)
		go s.initiateConsensus() // Re-initiate consensus
		return
	}

	// Construct Major Block
	majorBlock := s.constructMajorBlock(promiseResponses, ballotNumber)

	// log.Printf("[Server %s] [Leader] Constructed Major Block: %+v", s.ClientID, majorBlock)

	// Accept Phase
	// log.Printf("[Server %s] [Leader] Entering Accept phase.", s.ClientID)
	accepted := s.acceptPhase(majorBlock)

	// Check if majority acceptance was received
	if len(accepted) < len(s.Peers)/2 {
		// log.Printf("[Server %s] [Leader] Failed to receive majority acceptance.", s.ClientID)
		s.finalizeConsensus(false)
		time.Sleep(1 * time.Second)
		go s.initiateConsensus() // Re-initiate consensus
		return
	}

	// Commit Phase
	// log.Printf("[Server %s] [Leader] Entering Commit phase.", s.ClientID)
	s.commitPhase(majorBlock)

	// Commit the major block locally
	var commitResponse bool
	err := s.Commit(majorBlock, &commitResponse)
	if err != nil || !commitResponse {
		// log.Printf("[Server %s] [Leader] Failed to commit the major block locally.", s.ClientID)
		s.finalizeConsensus(false)
		go s.initiateConsensus() // Re-initiate consensus
		return
	}

	// log.Printf("[Server %s] [Leader] Successfully committed Major Block locally.", s.ClientID)
	// log.Printf("[Server %s] Current Balance after Commit: %.2f", s.ClientID, s.calculateBalance())

	// After committing, attempt to process pending transactions
	s.processPendingTransactions()

	// Finalize consensus successfully
	s.finalizeConsensus(true)
}

// finalizeConsensus resets processing flags and signals waiting goroutines.
func (s *Server) finalizeConsensus(success bool) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	s.isProcessing = false
	s.cond.Broadcast()

	if !success {
		// log.Printf("[Server %s] Consensus process completed unsuccessfully.", s.ClientID)
	} else {
		// log.Printf("[Server %s] Consensus process completed successfully.", s.ClientID)
	}
}

// Commit handles the Commit message from the leader. It will not respond if the server is not alive.
func (s *Server) Commit(majorBlock *models.MajorBlock, response *bool) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if !s.IsAlive {
		// Do not respond if the server is marked as dead
		// log.Printf("[Server %s] Accept request ignored as server is down.", s.ClientID)
		return nil
	}

	// Append block to datastore if not already committed.
	if majorBlock.SequenceNumber > s.LastCommitted {
		s.Datastore = append(s.Datastore, *majorBlock)
		s.LastCommitted = majorBlock.SequenceNumber

		// Remove committed transactions from local log by comparing all transaction attributes
		for _, committedTx := range majorBlock.Transactions {
			s.removeTransactionFromLocalLog(committedTx)
		}

		// Reset AcceptNum and AcceptVal.
		s.AcceptNum = 0
		s.AcceptVal = nil

		// log.Printf("[Server %s] Committed Major Block: %+v", s.ClientID, majorBlock)
		// log.Printf("[Server %s] Current Balance after Commit: %.2f", s.ClientID, s.calculateBalance())
	}

	*response = true
	return nil
}

// removeTransactionFromLocalLog removes a transaction from the LocalLog if it matches the committed transaction.
func (s *Server) removeTransactionFromLocalLog(committedTx models.Transaction) {
	var newLocalLog []models.Transaction
	for _, tx := range s.LocalLog {
		if !transactionsMatch(tx, committedTx) {
			newLocalLog = append(newLocalLog, tx)
		} else {
			// log.Printf("[Server %s] Removed committed transaction from LocalLog: %+v", s.ClientID, tx)
		}
	}
	s.LocalLog = newLocalLog
}

// transactionsMatch checks if two transactions are identical based on all attributes.
func transactionsMatch(tx1, tx2 models.Transaction) bool {
	return tx1.ID == tx2.ID &&
		tx1.Sender == tx2.Sender &&
		tx1.Receiver == tx2.Receiver &&
		tx1.Amount == tx2.Amount &&
		tx1.Timestamp.Equal(tx2.Timestamp)
}

func (s *Server) getMissingBlocks(fromSequence int) []models.MajorBlock {
	var missingBlocks []models.MajorBlock
	for _, block := range s.Datastore {
		if block.SequenceNumber >= fromSequence {
			missingBlocks = append(missingBlocks, block)
		}
	}
	return missingBlocks
}

// Promise handles the Prepare message from a leader. It will not respond if the server is not alive.
func (s *Server) Promise(request models.PrepareRequest, response *models.PromiseResponse) error {
	if !s.IsAlive {
		// log.Printf("[Server %s] Promise request ignored as server is down.", s.ClientID)
		return nil
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	ballotNumber := request.BallotNumber
	leaderLastCommitted := request.LeaderCommitted

	// Detect if the follower is behind
	if leaderLastCommitted > s.LastCommitted {
		// log.Printf("[Server %s] Detected out-of-sync with leader. Initiating sync.", s.ClientID)
		// Follower requests missing blocks from the leader
		s.Mutex.Unlock() // Unlock before network call
		err := s.requestSyncFromLeader(request.LeaderAddress, s.LastCommitted+1)
		s.Mutex.Lock() // Re-lock after network call
		if err != nil {
			// log.Printf("[Server %s] Sync failed with leader: %v", s.ClientID, err)
			response.Success = false
			return nil
		}
		// After syncing, proceed to send Promise
	}

	// Detect if the leader is behind
	if leaderLastCommitted < s.LastCommitted {
		// log.Printf("[Server %s] Leader is behind. Sending missing blocks.", s.ClientID)
		// Reject the prepare and send missing blocks
		response.Success = false
		missingBlocks := s.getMissingBlocks(leaderLastCommitted + 1)
		response.MissingBlocks = missingBlocks
		return nil
	}

	// If the ballot number in the Prepare message is greater than or equal to the server's MaxBallot, update MaxBallot.
	if ballotNumber >= s.MaxBallot {
		s.MaxBallot = ballotNumber // Update MaxBallot to the highest seen ballot number
		// response.AcceptNum = s.AcceptNum
		// response.AcceptVal = s.AcceptVal
		response.AcceptNum = 0
		response.AcceptVal = nil
		response.LocalLog = s.LocalLog
		response.Success = true
		response.BallotNumber = ballotNumber
		response.LastCommittedSN = s.LastCommitted

		// log.Printf("[Server %s] Sent Promise response to leader with BallotNumber %d.", s.ClientID, ballotNumber)
	} else {
		// Reject the Prepare message if the ballot number is less than the current MaxBallot
		response.Success = false
		response.CurrentMaxBallot = s.MaxBallot // Send the server's current MaxBallot
		// log.Printf("[Server %s] Promise rejected. Received BallotNumber %d is less than the current MaxBallot %d.", s.ClientID, ballotNumber, s.MaxBallot)
	}

	return nil
}

func (s *Server) requestSyncFromLeader(leaderAddress string, missingFrom int) error {
	client, err := rpc.Dial("tcp", leaderAddress)
	if err != nil {
		// log.Printf("[Server %s] Error connecting to leader for sync: %v", s.ClientID, err)
		return err
	}
	defer client.Close()

	var syncResponse models.SyncResponse
	syncRequest := models.SyncRequest{
		FromSequence: missingFrom,
	}
	err = client.Call("Server.Sync", syncRequest, &syncResponse)
	if err != nil {
		// log.Printf("[Server %s] Error during sync with leader: %v", s.ClientID, err)
		return err
	}

	s.applyMissingBlocks(syncResponse.MajorBlocks)
	return nil
}

func (s *Server) Sync(request models.SyncRequest, response *models.SyncResponse) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// Send the missing blocks after the requested sequence number
	for _, block := range s.Datastore {
		if block.SequenceNumber >= request.FromSequence {
			response.MajorBlocks = append(response.MajorBlocks, block)
		}
	}

	return nil
}

func (s *Server) applyMissingBlocks(blocks []models.MajorBlock) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	for _, block := range blocks {
		if block.SequenceNumber > s.LastCommitted {
			s.Datastore = append(s.Datastore, block)
			s.LastCommitted = block.SequenceNumber

			// Remove committed transactions from LocalLog
			for _, tx := range block.Transactions {
				s.removeTransactionFromLocalLog(tx)
			}
			// log.Printf("[Server %s] Synced and committed Major Block: %+v", s.ClientID, block)
		}
	}
	// Recalculate balance
	// s.calculateBalance()
	// log.Printf("[Server %s] Balance after syncing: %.2f", s.ClientID, currentBalance)
}

// constructMajorBlock constructs the Major Block for acceptance.
func (s *Server) constructMajorBlock(promiseResponses []models.PromiseResponse, ballotNumber int) *models.MajorBlock {
	// log.Printf("[Server %s] [Leader] Constructing Major Block.", s.ClientID)

	var majorBlock *models.MajorBlock
	highestAcceptNum := -1

	// Check for any previously accepted value.
	for _, resp := range promiseResponses {
		if resp.AcceptVal != nil && resp.AcceptNum > highestAcceptNum {
			highestAcceptNum = resp.AcceptNum
			majorBlock = resp.AcceptVal
		}
	}

	// If previously accepted value found, update its BallotNumber
	if majorBlock != nil {
		// log.Printf("[Server %s] [Leader] Previously accepted value found with AcceptNum %d. Reusing value.", s.ClientID, highestAcceptNum)
		// Update the BallotNumber in the major block
		// majorBlock.BallotNumber = ballotNumber
	} else {
		// No previously accepted value, create new Major Block.
		// log.Printf("[Server %s] [Leader] No previously accepted values found. Creating new Major Block.", s.ClientID)

		// Append transactions from the promise responses (peers' logs)
		allTransactions := []models.Transaction{}
		for _, resp := range promiseResponses {
			allTransactions = append(allTransactions, resp.LocalLog...)
			// log.Printf("[Server %s] [Leader] Appending transactions from peer's LocalLog: %+v", s.ClientID, resp.LocalLog)
		}

		// Append the server's own local log
		allTransactions = append(allTransactions, s.LocalLog...)
		// log.Printf("[Server %s] [Leader] Appending own local transactions: %+v", s.ClientID, s.LocalLog)

		// Construct the major block with all transactions
		majorBlock = &models.MajorBlock{
			SequenceNumber: s.LastCommitted + 1,
			BallotNumber:   ballotNumber,
			Transactions:   allTransactions,
		}
	}

	// log.Printf("[Server %s] [Leader] Created Major Block: %+v", s.ClientID, majorBlock)
	return majorBlock
}

// SetAliveStatus sets the alive status of the server for a particular set.
func (s *Server) SetAliveStatus(aliveStatus bool, reply *bool) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	s.IsAlive = aliveStatus
	// log.Printf("[Server %s] Set alive status to %v", s.ClientID, aliveStatus)

	*reply = true
	return nil
}

// acceptPhase sends Accept messages to all peers.
func (s *Server) acceptPhase(majorBlock *models.MajorBlock) []bool {
	// log.Printf("[Server %s] [Leader] Sending Accept messages to peers.", s.ClientID)

	var accepted []bool
	var mu sync.Mutex
	var wg sync.WaitGroup
	timeout := time.After(1 * time.Second)

	for _, peer := range s.Peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				// log.Printf("[Server %s] AcceptPhase: Error connecting to peer %s: %v", s.ClientID, peer, err)
				return
			}
			defer client.Close()

			var response bool
			err = client.Call("Server.Accept", majorBlock, &response)
			if err != nil {
				// log.Printf("[Server %s] AcceptPhase: Error calling Accept on peer %s: %v", s.ClientID, peer, err)
				return
			}

			if response {
				mu.Lock()
				accepted = append(accepted, true)
				mu.Unlock()
				// log.Printf("[Server %s] Received acceptance from peer %s.", s.ClientID, peer)
			} else {
				// log.Printf("[Server %s] Peer %s rejected the Accept request.", s.ClientID, peer)
			}
		}(peer)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-timeout:
		// log.Printf("[Server %s] AcceptPhase: Timeout occurred while waiting for Acceptances.", s.ClientID)
	}

	return accepted
}

// commitPhase sends Commit messages to all peers.
func (s *Server) commitPhase(majorBlock *models.MajorBlock) {
	// log.Printf("[Server %s] [Leader] Sending Commit messages to peers.", s.ClientID)

	var wg sync.WaitGroup

	for _, peer := range s.Peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				// log.Printf("[Server %s] CommitPhase: Error connecting to peer %s: %v", s.ClientID, peer, err)
				return
			}
			defer client.Close()

			var response bool
			err = client.Call("Server.Commit", majorBlock, &response)
			if err != nil {
				// log.Printf("[Server %s] CommitPhase: Error calling Commit on peer %s: %v", s.ClientID, peer, err)
				return
			}

			if response {
				// log.Printf("[Server %s] Successfully sent Commit to peer %s.", s.ClientID, peer)
			} else {
				// log.Printf("[Server %s] Peer %s failed to commit Major Block.", s.ClientID, peer)
			}
		}(peer)
	}

	wg.Wait()
	// log.Printf("[Server %s] [Leader] Completed Commit phase.", s.ClientID)
}

// processPendingTransactions attempts to process transactions from the PendingQueue.
func (s *Server) processPendingTransactions() {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	if len(s.PendingQueue) == 0 {
		// log.Printf("[Server %s] No pending transactions to process.", s.ClientID)
		s.cond.Broadcast()
		return
	}

	// Iterate through the PendingQueue and process transactions
	for i := 0; i < len(s.PendingQueue); {
		tx := s.PendingQueue[i]
		currentBalance := s.calculateBalance()
		// log.Printf("[Server %s] Attempting to process pending transaction: %+v | Current Balance: %.2f", s.ClientID, tx, currentBalance)

		if currentBalance >= tx.Amount {
			// Sufficient balance: move transaction to LocalLog
			s.LocalLog = append(s.LocalLog, tx)
			// log.Printf("[Server %s] Pending transaction moved to LocalLog: %+v", s.ClientID, tx)
			// log.Printf("[Server %s] Updated Balance: %.2f", s.ClientID, s.calculateBalance())

			// Remove transaction from PendingQueue
			s.PendingQueue = append(s.PendingQueue[:i], s.PendingQueue[i+1:]...)
			// Reset RetryCount after successful processing
			s.RetryCount = 0
		} else {
			// Insufficient balance: decide whether to retry
			if s.RetryCount < s.MaxRetries {
				s.RetryCount++
				// log.Printf("[Server %s] Insufficient balance for pending transaction. Retry attempt %d/%d.", s.ClientID, s.RetryCount, s.MaxRetries)
				// Initiate consensus again if the server is alive
				if s.IsAlive {
					go s.initiateConsensus()
				}
				// Since a consensus is initiated, set isProcessing and wait
				s.isProcessing = true
				return
			} else {
				// log.Printf("[Server %s] Maximum retry attempts reached for pending transaction. Transaction failed: %+v", s.ClientID, tx)
				// Remove transaction from PendingQueue
				s.PendingQueue = append(s.PendingQueue[:i], s.PendingQueue[i+1:]...)
			}
			i++
		}
	}

	// Signal that processing is complete
	s.cond.Broadcast()
}

// Start the server.
func startServer(clientID string, port string, peers []string) *Server {

	server := &Server{
		ClientID:      clientID,
		Port:          port,
		Peers:         peers,
		Balance:       100.0, // Initial balance
		LocalLog:      []models.Transaction{},
		Datastore:     []models.MajorBlock{},
		PendingQueue:  []models.Transaction{},
		MaxBallot:     0,
		IsAlive:       true,
		TransactionID: 1,
		RetryCount:    0,      // Start with zero retries
		MaxRetries:    100000, // Set maximum number of retries
		isProcessing:  false,
	}
	server.StartTime = time.Now()

	// Initialize the condition variable with the server's mutex
	server.cond = sync.NewCond(&server.Mutex)

	// Register custom types for gob.
	gob.Register(models.Transaction{})
	gob.Register(models.MajorBlock{})
	gob.Register(models.PromiseResponse{})
	gob.Register(models.PrepareRequest{})
	gob.Register(models.SyncRequest{})
	gob.Register(models.SyncResponse{})
	gob.Register(models.PerformanceMetrics{})

	go server.Initialize()

	return server
}

// GetBalance returns the current balance of the server's client.
func (s *Server) GetBalance(_ *struct{}, reply *float64) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// Calculate the current balance
	currentBalance := s.calculateBalance()
	*reply = currentBalance
	// log.Printf("[Server %s] Current balance: %.2f", s.ClientID, currentBalance)

	return nil
}

// GetLog returns the local log of transactions on the server.
func (s *Server) GetLog(_ *struct{}, reply *[]models.Transaction) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// Return the current local log
	*reply = append([]models.Transaction{}, s.LocalLog...) // Copy local log to avoid race conditions
	// log.Printf("[Server %s] Returned LocalLog: %+v", s.ClientID, s.LocalLog)

	return nil
}

// GetDatastore returns the current datastore of the server.
func (s *Server) GetDatastore(_ *struct{}, reply *[]models.MajorBlock) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// Create a copy of the datastore to avoid race conditions
	*reply = append([]models.MajorBlock{}, s.Datastore...)
	return nil
}

func (s *Server) GetPerformanceMetrics(_ *struct{}, reply *models.PerformanceMetrics) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	var totalLatency time.Duration
	for _, l := range s.LatencyData {
		totalLatency += l
	}

	var avgLatency time.Duration
	if len(s.LatencyData) > 0 {
		avgLatency = totalLatency / time.Duration(len(s.LatencyData))
	}

	uptime := time.Since(s.StartTime).Seconds()
	throughput := float64(s.TotalTransactions) / uptime

	*reply = models.PerformanceMetrics{
		AverageLatency: avgLatency,
		Throughput:     throughput,
	}
	return nil
}

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: go run server.go <ClientID> <Port>")
	}

	clientID := os.Args[1] // Get ClientID from command-line argument
	port := os.Args[2]     // Get Port from command-line argument

	peersMap := map[string]string{
		"S1": "1231",
		"S2": "1232",
		"S3": "1233",
		"S4": "1234",
		"S5": "1235",
	}

	otherPeers := []string{}
	for otherID, otherPort := range peersMap {
		if otherID != clientID {
			otherPeers = append(otherPeers, "localhost:"+otherPort)
		}
	}

	startServer(clientID, port, otherPeers)

	// log.Printf("[Server %s] Server is up and running.", clientID)
	for {
		time.Sleep(1 * time.Hour)
	}
}
