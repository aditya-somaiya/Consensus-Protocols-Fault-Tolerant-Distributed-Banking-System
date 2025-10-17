package main

import (
	"bufio"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/gob"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// Message Types
const (
	RequestMsg        = "REQUEST"
	PrePrepareMsg     = "PRE-PREPARE"
	PrepareMsg        = "PREPARE"
	CommitMsg         = "COMMIT"
	ReplyMsg          = "REPLY"
	CombinedCommitMsg = "COMBINED-COMMIT"
	TwoPCPrepareMsg   = "2PC-PREPARE"
	TwoPCCommitMsg    = "2PC-COMMIT"
	TwoPCAbortMsg     = "2PC-ABORT"
)

const (
	ClusterSize = 4 // For f = 1, ClusterSize = 3f + 1 = 4
)
const (
	TwoPCPreparedMsg = "2PC-PREPARED"
)

// Transaction represents a transaction between clients
type Transaction struct {
	SetNumber int
	Sender    string
	Receiver  string
	Amount    float64
	Timestamp time.Time
}

// CombinedCommitMessage is the structure for COMBINED-COMMIT messages
type CombinedCommitMessage struct {
	Type           string
	ViewNumber     int
	SequenceNumber int64
	CommitMessages []CommitMessage
	Signature      string
}

// CollectPrepareMessage is the structure for COLLECT-PREPARE messages
type CollectPrepareMessage struct {
	Type            string
	ViewNumber      int
	SequenceNumber  int64
	PrepareMessages []PrepareMessage
}

// CollectPrepareMessageWrapper wraps CollectPrepareMessage with a signature
type CollectPrepareMessageWrapper struct {
	CombinedMsg CollectPrepareMessage
	Signature   string
}

// Request is the structure used for sending transaction requests
type Request struct {
	Type        string
	Transaction Transaction
	Timestamp   time.Time
	ClientID    string
	Signature   string
}

// PrePrepareMessage is the structure for PRE-PREPARE messages
type PrePrepareMessage struct {
	Type           string
	ViewNumber     int
	SequenceNumber int64
	Digest         string
	Request        Request
	Signature      string
}

// PrepareMessage is the structure for PREPARE messages
type PrepareMessage struct {
	Type           string
	ViewNumber     int
	SequenceNumber int64
	Digest         string
	NodeID         string
	Signature      string
}

// CommitMessage is the structure for COMMIT messages
type CommitMessage struct {
	Type           string
	ViewNumber     int
	SequenceNumber int64
	Digest         string
	NodeID         string
	Signature      string
}

// TwoPCMessage is the structure for 2PC messages
type TwoPCMessage struct {
	Type           string
	ViewNumber     int
	SequenceNumber int64
	Transaction    Transaction
	Signature      string
}

// Reply is the structure for sending replies to clients
type Reply struct {
	Type       string
	ViewNumber int
	Timestamp  time.Time
	ClientID   string
	NodeID     string
	Result     string
	Signature  string
}

const clientAddress = "localhost:1230"

// StatusUpdate represents the structure for updating server status
type StatusUpdate struct {
	ActiveServers    []string
	ByzantineServers []string
}

type TransactionState struct {
	SequenceNumber    int64
	Handled           bool
	CrossShard        bool
	OriginalTimestamp time.Time
}

// Server represents a server node in the network
type Server struct {
	ID             string
	ClusterID      int
	Peers          []string
	ViewNumber     int
	SequenceNumber int64
	IsLeader       bool
	PrivateKey     *rsa.PrivateKey
	PublicKey      *rsa.PublicKey

	ReceivedLog          sync.Map // map[string]Request
	PrePrepareLog        sync.Map // map[int64]PrePrepareMessage
	PrepareLog           sync.Map // map[int64][]PrepareMessage
	CommitLog            sync.Map // map[int64][]CommitMessage
	ExecutedLog          sync.Map // map[int64]Transaction
	executedTransactions sync.Map

	OtherServerKeys map[string]*rsa.PublicKey

	listener  net.Listener
	rpcServer *rpc.Server
	f         int

	db *sql.DB

	clientLocks sync.Map // map[string]*sync.Mutex

	PrepareMessagesReceived sync.Map // map[int64][]PrepareMessage
	CommitMessagesReceived  sync.Map // map[int64][]CommitMessage

	lastExecutedSequenceNumber int64

	transactionQueue map[int64]PrePrepareMessage

	executionMutex sync.Mutex

	transactionState sync.Map // map[int]TransactionState

	WALMutex sync.Mutex
	WALPath  string

	IsActive    bool
	IsByzantine bool
	statusMutex sync.Mutex

	datastoreMutex sync.Mutex
	datastore      []DatastoreRecord
}

type DatastoreRecord struct {
	Type        string // 'P', 'C', 'A' for cross-shard; "" for intra-shard
	Transaction Transaction
}

func init() {
	gob.Register(CollectPrepareMessageWrapper{})
	gob.Register(CollectPrepareMessage{})
	gob.Register(CommitMessage{})
	gob.Register(PrepareMessage{})
	gob.Register(PrePrepareMessage{})
	gob.Register(Request{})
	gob.Register(Reply{})
	gob.Register(CombinedCommitMessage{})
	gob.Register(TwoPCMessage{})
}

func (s *Server) checkActiveAndNotByzantine() bool {
	s.statusMutex.Lock()
	defer s.statusMutex.Unlock()
	return s.IsActive && !s.IsByzantine
}

// UpdateStatus updates the server's status
func (s *Server) UpdateStatus(statusUpdate StatusUpdate, reply *bool) error {
	log.Printf("[UpdateStatus] Entering. StatusUpdate: %+v", statusUpdate)
	s.statusMutex.Lock()
	defer s.statusMutex.Unlock()

	s.IsActive = false
	for _, id := range statusUpdate.ActiveServers {
		if id == s.ID {
			s.IsActive = true
			break
		}
	}

	s.IsByzantine = false
	for _, id := range statusUpdate.ByzantineServers {
		if id == s.ID {
			s.IsByzantine = true
			break
		}
	}

	*reply = true
	log.Printf("[UpdateStatus] Exiting. Server %s isActive: %v, isByzantine: %v", s.ID, s.IsActive, s.IsByzantine)
	return nil
}

func (s *Server) queueTransaction(seqNum int64, prePrepareMsg PrePrepareMessage) {
	log.Printf("[queueTransaction] Entering. seqNum: %d", seqNum)
	if s.transactionQueue == nil {
		s.transactionQueue = make(map[int64]PrePrepareMessage)
	}
	s.transactionQueue[seqNum] = prePrepareMsg
	log.Printf("[queueTransaction] Exiting. Transaction queued for seqNum: %d", seqNum)
}

func (s *Server) processQueuedTransactions() {
	log.Printf("[processQueuedTransactions] Entering.")

	expectedSeq := s.lastExecutedSequenceNumber + 1
	log.Printf("[processQueuedTransactions] Expected SequenceNumber: %d", expectedSeq)

	for {
		log.Printf("[processQueuedTransactions] Checking for queued transaction with seqNum: %d", expectedSeq)
		prePrepareMsg, exists := s.transactionQueue[expectedSeq]
		if !exists {
			log.Printf("[processQueuedTransactions] No queued transaction found for seqNum: %d. Stopping.", expectedSeq)
			break
		}

		transaction := prePrepareMsg.Request.Transaction
		log.Printf("[processQueuedTransactions] Found queued transaction SetNumber %d for seqNum %d.", transaction.SetNumber, expectedSeq)

		result, err := s.executeTransaction(transaction)
		if err != nil {
			log.Printf("Error executing transaction SetNumber %d: %v", transaction.SetNumber, err)
			// Optionally, decide whether to continue or break
		}

		if result == "fail" {
			// Handle failure
			log.Printf("[processQueuedTransactions] Handling failed transaction SetNumber %d.", transaction.SetNumber)
			// ... (existing fail handling code)
		} else if result == "executed" {
			// Handle success
			state, _ := s.getTransactionState(transaction.SetNumber)
			if !state.CrossShard {
				s.addToDatastore("", transaction)
			} else {
				// If cross-shard and executing from queue, it's after combined commit = local execution 'P'
				s.addToDatastore("P", transaction)
			}
			log.Printf("[processQueuedTransactions] Handling executed transaction SetNumber %d.", transaction.SetNumber)
			// ... (existing executed handling code)
		}

		// Update state
		delete(s.transactionQueue, expectedSeq)
		s.lastExecutedSequenceNumber = expectedSeq
		expectedSeq++
		log.Printf("[processQueuedTransactions] Updated lastExecutedSequenceNumber to %d.", s.lastExecutedSequenceNumber)
	}
	log.Printf("[processQueuedTransactions] Exiting.")
}

func (s *Server) ReceiveTransaction(request Request, reply *Reply) error {
	log.Printf("[ReceiveTransaction] Entering. Request: %+v", request)
	s.statusMutex.Lock()
	isActive := s.IsActive
	s.statusMutex.Unlock()

	if !isActive {
		log.Printf("Server %s is inactive. Ignoring transaction SetNumber %d from client %s.", s.ID, request.Transaction.SetNumber, request.ClientID)
		return nil
	}

	intraShard := s.isIntraShardTransaction(request.Transaction)
	if intraShard {
		log.Printf("[ReceiveTransaction] Handling Intra-shard transaction SetNumber %d", request.Transaction.SetNumber)
		s.handleIntraShardTransaction(request)
	} else {
		log.Printf("[ReceiveTransaction] Handling Cross-shard transaction SetNumber %d", request.Transaction.SetNumber)
		s.handleCrossShardTransaction(request)
	}
	log.Printf("[ReceiveTransaction] Exiting.")
	return nil
}

func (s *Server) isIntraShardTransaction(tx Transaction) bool {
	log.Printf("[isIntraShardTransaction] Entering. Transaction: %+v", tx)
	senderCluster := s.getClusterIDFromClient(tx.Sender)
	receiverCluster := s.getClusterIDFromClient(tx.Receiver)
	intra := senderCluster == receiverCluster
	log.Printf("[isIntraShardTransaction] Exiting. IntraShard: %v", intra)
	return intra
}

func (s *Server) getClusterIDFromClient(clientID string) int {
	log.Printf("[getClusterIDFromClient] Entering. clientID: %s", clientID)
	num, err := strconv.Atoi(clientID)
	if err != nil {
		log.Printf("Error parsing client number from ID %s: %v", clientID, err)
		return -1
	}
	var cluster int
	if num >= 1 && num <= 1000 {
		cluster = 1
	} else if num >= 1001 && num <= 2000 {
		cluster = 2
	} else if num >= 2001 && num <= 3000 {
		cluster = 3
	} else {
		log.Printf("Client ID %s does not belong to any cluster.", clientID)
		cluster = -1
	}
	log.Printf("[getClusterIDFromClient] Exiting. clusterID: %d", cluster)
	return cluster
}

func (s *Server) handleIntraShardTransaction(request Request) {
	log.Printf("[handleIntraShardTransaction] Entering. Request: %+v", request)
	tx := request.Transaction

	// Check for existing transaction state first
	state, exists := s.getTransactionState(tx.SetNumber)
	if exists && state.Handled && tx.Timestamp.Equal(state.OriginalTimestamp) {
		// If the transaction with the same timestamp exists and is already handled,
		// reuse the previously assigned sequence number and skip lock/balance check.
		log.Printf("[handleIntraShardTransaction] Duplicate transaction with same timestamp detected for SetNumber %d", tx.SetNumber)
		s.handleDuplicateTransaction(state.SequenceNumber, request, tx)
		log.Printf("[handleIntraShardTransaction] Exiting after handling duplicate.")
		return
	}

	// If no state or different timestamp, proceed with the normal path

	senderMutex := s.getClientMutex(tx.Sender)
	receiverMutex := s.getClientMutex(tx.Receiver)

	log.Printf("[handleIntraShardTransaction] Attempting to acquire sender lock for %s", tx.Sender)
	acquiredSender := senderMutex.TryLock()
	if !acquiredSender {
		log.Printf("Could not acquire lock for sender %s in transaction SetNumber %d. Discarding transaction.", tx.Sender, tx.SetNumber)
		s.handleTransactionDiscard(request, tx)
		log.Printf("[handleIntraShardTransaction] Exiting due to failed sender lock.")
		return
	}
	defer senderMutex.Unlock()

	log.Printf("[handleIntraShardTransaction] Attempting to acquire receiver lock for %s", tx.Receiver)
	acquiredReceiver := receiverMutex.TryLock()
	if !acquiredReceiver {
		log.Printf("Could not acquire lock for receiver %s in transaction SetNumber %d. Discarding transaction.", tx.Receiver, tx.SetNumber)
		s.handleTransactionDiscard(request, tx)
		log.Printf("[handleIntraShardTransaction] Exiting due to failed receiver lock.")
		return
	}
	defer receiverMutex.Unlock()

	log.Printf("[handleIntraShardTransaction] Checking sufficient balance for sender %s", tx.Sender)
	if !s.hasSufficientBalance(tx.Sender, tx.Amount) {
		log.Printf("Insufficient balance for sender %s in transaction SetNumber %d. Discarding transaction.", tx.Sender, tx.SetNumber)
		s.handleTransactionDiscard(request, tx)
		log.Printf("[handleIntraShardTransaction] Exiting due to insufficient balance.")
		return
	}

	// If we reach this point, it's a new transaction or a known transaction with a different timestamp
	s.transactionState.Store(tx.SetNumber, TransactionState{
		SequenceNumber:    0,
		Handled:           false,
		CrossShard:        false,
		OriginalTimestamp: tx.Timestamp, // Store the original timestamp
	})

	log.Printf("[handleIntraShardTransaction] Checking leader status. IsLeader: %v", s.IsLeader)
	if s.IsLeader {
		s.processIntraShardAsLeader(request, tx)
	} else {
		s.processIntraShardAsFollower(request, tx)
	}
	log.Printf("[handleIntraShardTransaction] Exiting.")
}

func (s *Server) handleCrossShardTransaction(request Request) {
	log.Printf("[handleCrossShardTransaction] Entering. Request: %+v", request)
	tx := request.Transaction

	// Check for existing transaction state first
	state, exists := s.getTransactionState(tx.SetNumber)
	if exists && state.Handled && tx.Timestamp.Equal(state.OriginalTimestamp) {
		// If the transaction with the same timestamp exists and is already handled,
		// reuse the previously assigned sequence number and skip lock/balance check.
		log.Printf("[handleCrossShardTransaction] Duplicate transaction with same timestamp detected for SetNumber %d", tx.SetNumber)
		s.handleDuplicateTransaction(state.SequenceNumber, request, tx)
		log.Printf("[handleCrossShardTransaction] Exiting after handling duplicate.")
		return
	}

	// If no state or different timestamp, proceed with the normal path

	senderMutex := s.getClientMutex(tx.Sender)
	receiverMutex := s.getClientMutex(tx.Receiver)

	log.Printf("[handleCrossShardTransaction] Attempting to acquire sender lock for %s", tx.Sender)
	acquiredSender := senderMutex.TryLock()
	if !acquiredSender {
		log.Printf("Could not acquire lock for sender %s in cross-shard transaction SetNumber %d. Discarding transaction.", tx.Sender, tx.SetNumber)
		s.handleTransactionDiscard(request, tx)
		log.Printf("[handleCrossShardTransaction] Exiting due to failed sender lock.")
		return
	}
	defer senderMutex.Unlock()

	log.Printf("[handleCrossShardTransaction] Attempting to acquire receiver lock for %s", tx.Receiver)
	acquiredReceiver := receiverMutex.TryLock()
	if !acquiredReceiver {
		log.Printf("Could not acquire lock for receiver %s in cross-shard transaction SetNumber %d. Discarding transaction.", tx.Receiver, tx.SetNumber)
		s.handleTransactionDiscard(request, tx)
		log.Printf("[handleCrossShardTransaction] Exiting due to failed receiver lock.")
		return
	}
	defer receiverMutex.Unlock()

	log.Printf("[handleCrossShardTransaction] Checking sufficient balance for sender %s", tx.Sender)
	if !s.hasSufficientBalance(tx.Sender, tx.Amount) {
		log.Printf("Insufficient balance for sender %s in cross-shard transaction SetNumber %d. Discarding transaction.", tx.Sender, tx.SetNumber)
		s.handleTransactionDiscard(request, tx)
		log.Printf("[handleCrossShardTransaction] Exiting due to insufficient balance.")
		return
	}

	// New or different timestamp transaction
	s.transactionState.Store(tx.SetNumber, TransactionState{
		SequenceNumber:    0,
		Handled:           false,
		CrossShard:        true,
		OriginalTimestamp: tx.Timestamp,
	})

	log.Printf("[handleCrossShardTransaction] Checking leader status. IsLeader: %v", s.IsLeader)
	if s.IsLeader {
		s.processCrossShardAsLeader(request, tx)
	} else {
		s.processCrossShardAsFollower(request, tx)
	}
	log.Printf("[handleCrossShardTransaction] Exiting.")
}

func (s *Server) getClientMutex(clientID string) *sync.Mutex {
	log.Printf("[getClientMutex] Entering. clientID: %s", clientID)
	val, _ := s.clientLocks.LoadOrStore(clientID, &sync.Mutex{})
	log.Printf("[getClientMutex] Exiting.")
	return val.(*sync.Mutex)
}

// hasSufficientBalance checks if the sender has enough balance to cover the amount.
// It only initializes and checks the balance if the sender is within the server's cluster.
func (s *Server) hasSufficientBalance(senderID string, amount float64) bool {
	log.Printf("[hasSufficientBalance] Entering. senderID: %s, amount: %.2f", senderID, amount)

	// Determine the cluster of the sender
	senderCluster := s.getClusterIDFromClient(senderID)

	if senderCluster != s.ClusterID {
		log.Printf("[hasSufficientBalance] Sender %s does not belong to cluster %d. Ignoring balance check.", senderID, s.ClusterID)
		// Since this server does not manage the sender, it assumes balance is sufficient.
		// Alternatively, you could return false or handle it as per your application's logic.
		return true
	}

	var balance float64
	err := s.db.QueryRow("SELECT balance FROM balances WHERE client_id = ?", senderID).Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("[hasSufficientBalance] No balance row found for %s, initializing with balance 10.0.", senderID)
			_, err = s.db.Exec("INSERT INTO balances (client_id, balance) VALUES (?, ?)", senderID, 10.0)
			if err != nil {
				log.Printf("[hasSufficientBalance] Error initializing balance for %s: %v", senderID, err)
				return false
			}
			balance = 10.0
		} else {
			log.Printf("[hasSufficientBalance] Error querying balance for %s: %v", senderID, err)
			return false
		}
	}

	log.Printf("[hasSufficientBalance] senderID: %s, balance: %.2f, required: %.2f, sufficient: %v", senderID, balance, amount, balance >= amount)
	return balance >= amount
}

// handleTransactionDiscard discards a transaction without storing it in the transactionState.
func (s *Server) handleTransactionDiscard(request Request, tx Transaction) {
	log.Printf("[handleTransactionDiscard] Entering. Transaction SetNumber %d", tx.SetNumber)
	// Do not store the transaction in the transactionState
	log.Printf("Transaction SetNumber %d discarded due to failed lock acquisition or insufficient balance.", tx.SetNumber)
	log.Printf("[handleTransactionDiscard] Exiting.")
}

func (s *Server) handleDuplicateTransaction(sequenceNumber int64, request Request, tx Transaction) {
	log.Printf("[handleDuplicateTransaction] Entering. seqNum: %d, SetNumber: %d", sequenceNumber, tx.SetNumber)
	if sequenceNumber == -1 {
		log.Printf("[handleDuplicateTransaction] SequenceNumber is -1, discarding transaction SetNumber %d.", tx.SetNumber)
		s.handleTransactionDiscard(request, tx)
		log.Printf("[handleDuplicateTransaction] Exiting after discard.")
		return
	}

	value, exists := s.PrePrepareLog.Load(sequenceNumber)
	if !exists {
		log.Printf("No PrePrepare message for sequence number %d in duplicate transaction SetNumber %d.", sequenceNumber, tx.SetNumber)
		log.Printf("[handleDuplicateTransaction] Exiting with no PrePrepare found.")
		return
	}
	prePrepareMsg := value.(PrePrepareMessage)

	// **CHANGED**: Compare timestamps to determine if it's truly a duplicate
	if !tx.Timestamp.Equal(prePrepareMsg.Request.Transaction.Timestamp) {
		log.Printf("[handleDuplicateTransaction] Timestamp mismatch for SetNumber %d. Treating as new transaction.", tx.SetNumber)

		s.ReceiveTransaction(request, nil)
		log.Printf("[handleDuplicateTransaction] Exiting after reprocessing as new transaction.")
		return
	}

	// **CHANGED**: Ensure that only the transaction coordinator cluster sends replies
	coordinatorClusterID := s.getClusterIDFromClient(tx.Sender)
	if coordinatorClusterID == -1 {
		log.Printf("Invalid coordinator cluster ID for transaction SetNumber %d. Cannot send reply.", tx.SetNumber)
		log.Printf("[handleDuplicateTransaction] Exiting due to invalid coordinator cluster ID.")
		return
	}

	if s.ID == getLeaderIDForCluster(coordinatorClusterID) {
		replyMsg := Reply{
			Type:       ReplyMsg,
			ViewNumber: s.ViewNumber,
			Timestamp:  prePrepareMsg.Request.Timestamp, // Ensure timestamp matches the original
			ClientID:   prePrepareMsg.Request.ClientID,
			NodeID:     s.ID,
			Result:     "executed transaction",
		}
		replyMsg.Signature = signData(s.PrivateKey, replySignatureData(replyMsg))
		err := s.sendReplyToClient(replyMsg)
		if err != nil {
			log.Printf("Error sending duplicate executed reply to client %s for transaction SetNumber %d: %v", replyMsg.ClientID, tx.SetNumber, err)
		} else {
			log.Printf("Successfully sent duplicate executed reply to client %s for transaction SetNumber %d.", replyMsg.ClientID, tx.SetNumber)
		}
	} else {
		log.Printf("[handleDuplicateTransaction] Non-coordinator server %s received duplicate transaction SetNumber %d. No reply sent.", s.ID, tx.SetNumber)
	}

	log.Printf("[handleDuplicateTransaction] Exiting.")
}

func (s *Server) processIntraShardAsLeader(request Request, tx Transaction) {
	log.Printf("[processIntraShardAsLeader] Entering. SetNumber: %d", tx.SetNumber)
	seqNum := atomic.AddInt64(&s.SequenceNumber, 1)
	log.Printf("[processIntraShardAsLeader] Assigned SequenceNumber: %d to transaction SetNumber %d", seqNum, tx.SetNumber)
	digest := calculateDigest(request)

	prePrepareMsg := PrePrepareMessage{
		Type:           PrePrepareMsg,
		ViewNumber:     s.ViewNumber,
		SequenceNumber: seqNum,
		Digest:         digest,
		Request:        request,
	}
	prePrepareMsg.Signature = signData(s.PrivateKey, prePrepareSignatureData(prePrepareMsg))

	s.PrePrepareLog.Store(seqNum, prePrepareMsg)

	s.transactionState.Store(tx.SetNumber, TransactionState{
		SequenceNumber: seqNum,
		Handled:        true,
		CrossShard:     false,
	})

	log.Printf("[processIntraShardAsLeader] Broadcasting PRE-PREPARE")
	s.broadcastPrePrepare(prePrepareMsg)

	log.Printf("[processIntraShardAsLeader] Waiting for PREPARE messages.")
	prepareMessages := s.waitForPrepareMessages(seqNum, digest)
	log.Printf("[processIntraShardAsLeader] Received %d PREPARE messages.", len(prepareMessages))
	if len(prepareMessages) >= 2*s.f {
		log.Printf("[processIntraShardAsLeader] Broadcasting COLLECT-PREPARE")
		s.broadcastCollectPrepare(seqNum, prepareMessages)
	} else {
		log.Printf("Insufficient PREPARE messages received for sequence number %d. Expected at least %d, got %d.", seqNum, 2*s.f, len(prepareMessages))
	}
	log.Printf("[processIntraShardAsLeader] Exiting.")
}

func (s *Server) processIntraShardAsFollower(request Request, tx Transaction) {
	log.Printf("[processIntraShardAsFollower] Entering. Followers do not initiate transactions. SetNumber: %d", tx.SetNumber)
	log.Printf("[processIntraShardAsFollower] Exiting.")
}

// processCrossShardAsLeader handles cross-shard transaction as leader of sender cluster.
func (s *Server) processCrossShardAsLeader(request Request, tx Transaction) {
	log.Printf("[processCrossShardAsLeader] Entering. SetNumber: %d", tx.SetNumber)

	// Run PBFT in sender cluster just like intra-shard
	seqNum := atomic.AddInt64(&s.SequenceNumber, 1)
	log.Printf("[processCrossShardAsLeader] Assigned SequenceNumber: %d", seqNum)
	digest := calculateDigest(request)

	prePrepareMsg := PrePrepareMessage{
		Type:           PrePrepareMsg,
		ViewNumber:     s.ViewNumber,
		SequenceNumber: seqNum,
		Digest:         digest,
		Request:        request,
	}
	prePrepareMsg.Signature = signData(s.PrivateKey, prePrepareSignatureData(prePrepareMsg))

	s.PrePrepareLog.Store(seqNum, prePrepareMsg)
	s.transactionState.Store(tx.SetNumber, TransactionState{
		SequenceNumber: seqNum,
		Handled:        true,
		CrossShard:     true,
	})

	// Broadcast PRE-PREPARE and wait for prepares
	s.broadcastPrePrepare(prePrepareMsg)
	prepareMessages := s.waitForPrepareMessages(seqNum, digest)
	if len(prepareMessages) < 2*s.f {
		log.Printf("Insufficient PREPARE messages for sequenceNumber %d in sender cluster. Aborting transaction.", seqNum)
		s.abortTransaction(seqNum, tx)
		return
	}

	// Broadcast COLLECT-PREPARE and wait for commits
	s.broadcastCollectPrepare(seqNum, prepareMessages)

	// Wait until we have COMBINED-COMMIT (leader logic handled in broadcastCollectPrepare)
	// For simplicity, we assume combined commit eventually arrives or is handled inline.
	// Once combined commit is done, we "commit" locally:
	// Execute the transaction, write it to WAL, but do NOT release locks or reply to client.
	// Actually, we have already executed the transaction in HandleCombinedCommit.
	// Just ensure we have WAL entry.
	s.writeToWAL(tx)

	// Now start the 2PC phase:
	receiverCluster := s.getClusterIDFromClient(tx.Receiver)
	if receiverCluster == -1 {
		log.Printf("Invalid receiver cluster. Aborting transaction SetNumber %d.", tx.SetNumber)
		s.abortTransaction(seqNum, tx)
		return
	}
	receiverLeaderID := getLeaderIDForCluster(receiverCluster)
	if receiverLeaderID == "" {
		log.Printf("No leader found for receiver cluster %d. Aborting transaction SetNumber %d.", receiverCluster, tx.SetNumber)
		s.abortTransaction(seqNum, tx)
		return
	}
	receiverLeaderAddress := getServerAddress(receiverLeaderID)

	// Send TwoPCPrepareMsg to receiver cluster leader
	twoPCMsg := TwoPCMessage{
		Type:           TwoPCPrepareMsg,
		ViewNumber:     s.ViewNumber,
		SequenceNumber: seqNum,
		Transaction:    tx,
	}
	twoPCMsg.Signature = signData(s.PrivateKey, twoPCSignatureData(twoPCMsg))

	client, err := rpc.Dial("tcp", receiverLeaderAddress)
	if err != nil {
		log.Printf("Error dialing receiver leader %s for 2PCPrepare: %v", receiverLeaderID, err)
		s.abortTransaction(seqNum, tx)
		return
	}
	defer client.Close()

	var twoPCReply string
	err = client.Call("Server.HandleTwoPCPrepare", twoPCMsg, &twoPCReply)
	if err != nil {
		log.Printf("Error in 2PCPrepare with receiver leader %s: %v", receiverLeaderID, err)
		s.abortTransaction(seqNum, tx)
		return
	}

	// The reply from receiver leader should be either TwoPCPreparedMsg or TwoPCAbortMsg.
	if twoPCReply == TwoPCAbortMsg {
		// Receiver cluster aborted, abort locally as well.
		s.broadcastTwoPCAbort(seqNum, tx, receiverCluster)
	} else if twoPCReply == TwoPCPreparedMsg {
		// Receiver is prepared. Now we send TwoPCCommitMsg to both clusters.
		s.broadcastTwoPCCommit(seqNum, tx, receiverCluster)
	} else {
		log.Printf("Unexpected 2PC reply '%s' from receiver leader. Aborting transaction SetNumber %d.", twoPCReply, tx.SetNumber)
		s.broadcastTwoPCAbort(seqNum, tx, receiverCluster)
	}

	log.Printf("[processCrossShardAsLeader] Exiting.")
}

// broadcastTwoPCAbort is used by the sender leader after a failure to abort transaction in both clusters.
func (s *Server) broadcastTwoPCAbort(sequenceNumber int64, tx Transaction, receiverCluster int) {
	log.Printf("[broadcastTwoPCAbort] Entering. seqNum: %d, SetNumber: %d", sequenceNumber, tx.SetNumber)
	abortMsg := TwoPCMessage{
		Type:           TwoPCAbortMsg,
		ViewNumber:     s.ViewNumber,
		SequenceNumber: sequenceNumber,
		Transaction:    tx,
	}
	abortMsg.Signature = signData(s.PrivateKey, twoPCSignatureData(abortMsg))

	// Broadcast to sender cluster
	s.broadcastToCluster(s.ClusterID, "HandleTwoPCAbort", abortMsg, nil)
	// Broadcast to receiver cluster
	s.broadcastToCluster(receiverCluster, "HandleTwoPCAbort", abortMsg, nil)

	log.Printf("[broadcastTwoPCAbort] Exiting.")
}

// Helper function to broadcast a 2PC message to all servers in a given cluster
func (s *Server) broadcastToCluster(clusterID int, methodName string, msg TwoPCMessage, reply interface{}) {
	log.Printf("[broadcastToCluster] Entering. clusterID: %d, methodName: %s", clusterID, methodName)
	peers, err := getPeers(clusterID, s.ID)
	if err != nil {
		log.Printf("Error getting peers for cluster %d: %v", clusterID, err)
		return
	}
	var wg sync.WaitGroup
	for _, srv := range peers {
		wg.Add(1)
		go func(srv string) {
			defer wg.Done()

			address := getServerAddress(srv)
			log.Printf("[broadcastToCluster] Dialing server %s at %s for method %s", srv, address, methodName)
			client, err := rpc.Dial("tcp", address)
			if err != nil {
				log.Printf("Error dialing server %s for %s: %v", srv, methodName, err)
				return
			}
			defer client.Close()

			err = client.Call("Server."+methodName, msg, reply)
			if err != nil {
				log.Printf("Error calling %s on server %s: %v", methodName, srv, err)
				return
			}
			log.Printf("[broadcastToCluster] Successfully called %s on server %s", methodName, srv)
		}(srv)
	}
	wg.Wait()
	log.Printf("[broadcastToCluster] Exiting.")
}

// // releaseLocks releases the locks acquired by this transaction on sender and receiver clients.
// func (s *Server) releaseLocks(tx Transaction) {
// 	log.Printf("[releaseLocks] Releasing locks for transaction %d", tx.SetNumber)
// 	senderMutex := s.getClientMutex(tx.Sender)
// 	receiverMutex := s.getClientMutex(tx.Receiver)
// 	// Since we locked with TryLock before executing, we should now just unlock.
// 	// However, note: If code originally used defer for unlocking in the same function,
// 	// you'd need a different approach. Here we assume we only release once now.
// 	// If your previous code model retained locks all along, you must ensure they're still locked at this point.
// 	// For simplicity, we assume they remained locked (not using `defer` in the original code).
// 	senderMutex.Unlock()
// 	receiverMutex.Unlock()
// 	log.Printf("[releaseLocks] Locks released for transaction %d", tx.SetNumber)
// }

// getSequenceNumberForTransaction fetches the sequence number assigned to a transaction.
func (s *Server) getSequenceNumberForTransaction(setNumber int) int64 {
	state, exists := s.getTransactionState(setNumber)
	if !exists {
		return -1
	}
	return state.SequenceNumber
}

func (s *Server) processCrossShardAsFollower(request Request, tx Transaction) {
	log.Printf("[processCrossShardAsFollower] Entering. Followers do not initiate transactions. SetNumber: %d", tx.SetNumber)
	log.Printf("[processCrossShardAsFollower] Exiting.")
}

// HandleTwoPCPrepare handles the 2PC prepare message at the receiver cluster leader.
func (s *Server) HandleTwoPCPrepare(twoPCMsg TwoPCMessage, reply *string) error {
	log.Printf("[HandleTwoPCPrepare] Entering. twoPCMsg: %+v", twoPCMsg)

	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive || isByzantine {
		*reply = TwoPCAbortMsg
		log.Printf("Server %s is inactive or Byzantine. Aborting 2PCPrepare for transaction %d.", s.ID, twoPCMsg.Transaction.SetNumber)
		return nil
	}

	// Receiver cluster leader runs PBFT locally for the transaction.
	seqNum := atomic.AddInt64(&s.SequenceNumber, 1)
	req := Request{
		Type:        RequestMsg,
		Transaction: twoPCMsg.Transaction,
		Timestamp:   twoPCMsg.Transaction.Timestamp,
		ClientID:    twoPCMsg.Transaction.Sender,
	}
	digest := calculateDigest(req)

	prePrepareMsg := PrePrepareMessage{
		Type:           PrePrepareMsg,
		ViewNumber:     s.ViewNumber,
		SequenceNumber: seqNum,
		Digest:         digest,
		Request:        req,
	}
	prePrepareMsg.Signature = signData(s.PrivateKey, prePrepareSignatureData(prePrepareMsg))
	s.PrePrepareLog.Store(seqNum, prePrepareMsg)
	s.transactionState.Store(twoPCMsg.Transaction.SetNumber, TransactionState{
		SequenceNumber: seqNum,
		Handled:        true,
		CrossShard:     true,
	})

	s.broadcastPrePrepare(prePrepareMsg)
	prepareMessages := s.waitForPrepareMessages(seqNum, digest)
	if len(prepareMessages) < 2*s.f {
		log.Printf("Insufficient prepares in receiver cluster for 2PC transaction %d. Aborting.", twoPCMsg.Transaction.SetNumber)
		s.abortTransaction(seqNum, twoPCMsg.Transaction)
		*reply = TwoPCAbortMsg
		return nil
	}

	s.broadcastCollectPrepare(seqNum, prepareMessages)
	// Once combined commit is done, we execute locally (already done in HandleCombinedCommit),
	// Write to WAL but do not release locks or reply to client.
	s.writeToWAL(twoPCMsg.Transaction)

	// Now send TwoPCPreparedMsg back to sender leader
	*reply = TwoPCPreparedMsg

	log.Printf("[HandleTwoPCPrepare] Exiting with prepared state.")
	return nil
}

// HandleTwoPCPrepared is called by the sender leader when it receives a prepared message from the receiver cluster.
func (s *Server) HandleTwoPCPrepared(twoPCMsg TwoPCMessage, reply *bool) error {
	log.Printf("[HandleTwoPCPrepared] Entering. twoPCMsg: %+v", twoPCMsg)

	// At this point, the sender leader has confirmation that receiver cluster is prepared.
	// We must send TwoPCCommitMsg to both clusters (sender and receiver) to finalize.

	receiverCluster := s.getClusterIDFromClient(twoPCMsg.Transaction.Receiver)
	s.broadcastTwoPCCommit(twoPCMsg.SequenceNumber, twoPCMsg.Transaction, receiverCluster)

	*reply = true
	log.Printf("[HandleTwoPCPrepared] Exiting.")
	return nil
}

// broadcastTwoPCCommit is used by the sender leader after receiving "prepared" to finalize commit in both clusters.
func (s *Server) broadcastTwoPCCommit(sequenceNumber int64, tx Transaction, receiverCluster int) {
	log.Printf("[broadcastTwoPCCommit] Entering. seqNum: %d, SetNumber: %d", sequenceNumber, tx.SetNumber)
	commitMsg := TwoPCMessage{
		Type:           TwoPCCommitMsg,
		ViewNumber:     s.ViewNumber,
		SequenceNumber: sequenceNumber,
		Transaction:    tx,
	}
	commitMsg.Signature = signData(s.PrivateKey, twoPCSignatureData(commitMsg))

	// Define a *bool to capture the reply
	var commitReply bool

	// Broadcast to sender cluster
	s.broadcastToCluster(s.ClusterID, "HandleTwoPCCommit", commitMsg, &commitReply)
	// Broadcast to receiver cluster
	s.broadcastToCluster(receiverCluster, "HandleTwoPCCommit", commitMsg, &commitReply)

	log.Printf("[broadcastTwoPCCommit] Exiting.")
}

func (s *Server) HandleTwoPC(twoPCMsg TwoPCMessage, reply *string) error {
	log.Printf("[HandleTwoPC] Entering. twoPCMsg: %+v", twoPCMsg)
	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive || isByzantine {
		*reply = TwoPCAbortMsg
		log.Printf("Server %s is inactive or Byzantine. Aborting 2PC for transaction SetNumber %d.", s.ID, twoPCMsg.Transaction.SetNumber)
		log.Printf("[HandleTwoPC] Exiting after abort due to inactivity or Byzantine.")
		return nil
	}

	leaderID := s.getLeaderID()
	log.Printf("[HandleTwoPC] leaderID: %s", leaderID)
	leaderKey, exists := s.OtherServerKeys[leaderID]
	if !exists && leaderID != s.ID {
		log.Printf("Leader public key not found for server %s. Aborting 2PC for transaction SetNumber %d.", leaderID, twoPCMsg.Transaction.SetNumber)
		*reply = TwoPCAbortMsg
		log.Printf("[HandleTwoPC] Exiting due to missing leader key.")
		return fmt.Errorf("leader public key not found")
	}

	var key *rsa.PublicKey
	if leaderID == s.ID {
		key = s.PublicKey
	} else {
		key = leaderKey
	}

	twoPCData := twoPCSignatureData(twoPCMsg)
	if !verifySignature(key, twoPCData, twoPCMsg.Signature) {
		log.Printf("Invalid 2PC signature from leader %s for transaction SetNumber %d. Aborting 2PC.", leaderID, twoPCMsg.Transaction.SetNumber)
		*reply = TwoPCAbortMsg
		log.Printf("[HandleTwoPC] Exiting due to invalid signature.")
		return fmt.Errorf("invalid 2PC signature")
	}

	senderMutex := s.getClientMutex(twoPCMsg.Transaction.Sender)
	receiverMutex := s.getClientMutex(twoPCMsg.Transaction.Receiver)

	log.Printf("[HandleTwoPC] Attempting sender lock for %s", twoPCMsg.Transaction.Sender)
	if !senderMutex.TryLock() {
		log.Printf("Could not acquire lock for sender %s in 2PC for transaction SetNumber %d. Aborting 2PC.", twoPCMsg.Transaction.Sender, twoPCMsg.Transaction.SetNumber)
		*reply = TwoPCAbortMsg
		log.Printf("[HandleTwoPC] Exiting due to sender lock failure.")
		return nil
	}
	defer senderMutex.Unlock()

	log.Printf("[HandleTwoPC] Attempting receiver lock for %s", twoPCMsg.Transaction.Receiver)
	if !receiverMutex.TryLock() {
		log.Printf("Could not acquire lock for receiver %s in 2PC for transaction SetNumber %d. Aborting 2PC.", twoPCMsg.Transaction.Receiver, twoPCMsg.Transaction.SetNumber)
		*reply = TwoPCAbortMsg
		log.Printf("[HandleTwoPC] Exiting due to receiver lock failure.")
		return nil
	}
	defer receiverMutex.Unlock()

	log.Printf("[HandleTwoPC] Checking sender balance for 2PC.")
	if !s.hasSufficientBalance(twoPCMsg.Transaction.Sender, twoPCMsg.Transaction.Amount) {
		log.Printf("Insufficient balance for sender %s in 2PC for transaction SetNumber %d. Aborting 2PC.", twoPCMsg.Transaction.Sender, twoPCMsg.Transaction.SetNumber)
		*reply = TwoPCAbortMsg
		log.Printf("[HandleTwoPC] Exiting due to insufficient balance.")
		return nil
	}

	// Mark this as cross-shard
	s.transactionState.Store(twoPCMsg.Transaction.SetNumber, TransactionState{
		SequenceNumber: 0,
		Handled:        false,
		CrossShard:     true,
	})

	log.Printf("[HandleTwoPC] Checking if current server is leader: %v", s.IsLeader)
	if s.IsLeader {
		log.Printf("[HandleTwoPC] Leader logic for 2PC.")
		seqNum := atomic.AddInt64(&s.SequenceNumber, 1)
		log.Printf("[HandleTwoPC] New seqNum: %d", seqNum)
		digest := calculateDigest(Request{
			Type:        RequestMsg,
			Transaction: twoPCMsg.Transaction,
			Timestamp:   twoPCMsg.Transaction.Timestamp,
			ClientID:    twoPCMsg.Transaction.Sender,
		})

		prePrepareMsg := PrePrepareMessage{
			Type:           PrePrepareMsg,
			ViewNumber:     s.ViewNumber,
			SequenceNumber: seqNum,
			Digest:         digest,
			Request: Request{
				Type:        RequestMsg,
				Transaction: twoPCMsg.Transaction,
				Timestamp:   twoPCMsg.Transaction.Timestamp,
				ClientID:    twoPCMsg.Transaction.Sender,
			},
		}
		prePrepareMsg.Signature = signData(s.PrivateKey, prePrepareSignatureData(prePrepareMsg))

		s.PrePrepareLog.Store(seqNum, prePrepareMsg)

		s.transactionState.Store(twoPCMsg.Transaction.SetNumber, TransactionState{
			SequenceNumber: seqNum,
			Handled:        true,
			CrossShard:     true,
		})

		log.Printf("[HandleTwoPC] Broadcasting PRE-PREPARE for 2PC transaction.")
		s.broadcastPrePrepare(prePrepareMsg)

		log.Printf("[HandleTwoPC] Waiting for PREPARE messages for 2PC transaction.")
		prepareMessages := s.waitForPrepareMessages(seqNum, digest)
		log.Printf("[HandleTwoPC] Received %d PREPARE messages for 2PC transaction.", len(prepareMessages))
		if len(prepareMessages) >= 2*s.f {
			log.Printf("[HandleTwoPC] Broadcasting COLLECT-PREPARE for 2PC transaction.")
			s.broadcastCollectPrepare(seqNum, prepareMessages)
		} else {
			log.Printf("Insufficient PREPARE messages received for cross-shard 2PC sequence number %d. Expected at least %d, got %d.", seqNum, 2*s.f, len(prepareMessages))
		}

		commitMsg := TwoPCMessage{
			Type:           TwoPCCommitMsg,
			ViewNumber:     s.ViewNumber,
			SequenceNumber: seqNum,
			Transaction:    twoPCMsg.Transaction,
		}
		commitMsg.Signature = signData(s.PrivateKey, twoPCSignatureData(commitMsg))

		initCluster := s.getClusterIDFromClient(twoPCMsg.Transaction.Sender)
		if initCluster == -1 {
			log.Printf("Invalid initiating cluster for transaction SetNumber %d. Aborting 2PC.", twoPCMsg.Transaction.SetNumber)
			*reply = TwoPCAbortMsg
			log.Printf("[HandleTwoPC] Exiting due to invalid initiating cluster.")
			return nil
		}
		initLeaderID := getLeaderIDForCluster(initCluster)
		if initLeaderID == "" {
			log.Printf("Initiating leader ID not found for cluster %d in transaction SetNumber %d. Aborting 2PC.", initCluster, twoPCMsg.Transaction.SetNumber)
			*reply = TwoPCAbortMsg
			log.Printf("[HandleTwoPC] Exiting due to no initiating leader.")
			return nil
		}
		initLeaderAddress := getServerAddress(initLeaderID)

		log.Printf("[HandleTwoPC] Dialing initiating leader %s at %s for 2PCCommit", initLeaderID, initLeaderAddress)
		client, err := rpc.Dial("tcp", initLeaderAddress)
		if err != nil {
			log.Printf("Error dialing initiating leader %s for 2PCCommit in transaction SetNumber %d: %v", initLeaderID, twoPCMsg.Transaction.SetNumber, err)
			*reply = TwoPCAbortMsg
			log.Printf("[HandleTwoPC] Exiting due to dialing error for 2PCCommit.")
			return nil
		}
		defer client.Close()

		var twoPCCommitReply string
		err = client.Call("Server.HandleTwoPCCommit", commitMsg, &twoPCCommitReply)
		if err != nil {
			log.Printf("Error sending 2PCCommit to initiating leader %s for transaction SetNumber %d: %v", initLeaderID, twoPCMsg.Transaction.SetNumber, err)
			*reply = TwoPCAbortMsg
			log.Printf("[HandleTwoPC] Exiting due to call error in 2PCCommit.")
			return nil
		}

		if twoPCCommitReply != TwoPCCommitMsg {
			log.Printf("Unexpected 2PCCommit reply '%s' from initiating leader %s for transaction SetNumber %d. Aborting 2PC.", twoPCCommitReply, initLeaderID, twoPCMsg.Transaction.SetNumber)
			*reply = TwoPCAbortMsg
			log.Printf("[HandleTwoPC] Exiting due to unexpected commit reply.")
			return nil
		}

		*reply = TwoPCCommitMsg
		log.Printf("[HandleTwoPC] Exiting with 2PC commit.")
	} else {
		log.Printf("Non-leader server %s received 2PCCommit request for transaction SetNumber %d. Aborting.", s.ID, twoPCMsg.Transaction.SetNumber)
		*reply = TwoPCAbortMsg
		log.Printf("[HandleTwoPC] Exiting non-leader path.")
	}
	return nil
}

// broadcastPrePrepare broadcasts PRE-PREPARE
func (s *Server) broadcastPrePrepare(prePrepareMsg PrePrepareMessage) {
	log.Printf("[broadcastPrePrepare] Entering. Broadcasting to peers: %+v", s.Peers)
	var wg sync.WaitGroup

	for _, serverID := range s.Peers {
		if serverID == s.ID {
			continue
		}

		wg.Add(1)
		go func(serverID string) {
			defer wg.Done()

			address := getServerAddress(serverID)
			log.Printf("[broadcastPrePrepare] Dialing server %s at %s", serverID, address)
			client, err := rpc.Dial("tcp", address)
			if err != nil {
				log.Printf("Error dialing server %s for PrePrepare: %v", serverID, err)
				return
			}
			defer client.Close()

			var ack bool
			err = client.Call("Server.HandlePrePrepare", prePrepareMsg, &ack)
			if err != nil {
				log.Printf("Error calling HandlePrePrepare on server %s: %v", serverID, err)
				return
			}
			log.Printf("[broadcastPrePrepare] Successfully sent PRE-PREPARE to %s", serverID)
		}(serverID)
	}

	wg.Wait()
	log.Printf("[broadcastPrePrepare] Exiting.")
}

// HandlePrePrepare handles PRE-PREPARE messages from the leader.
func (s *Server) HandlePrePrepare(prePrepareMsg PrePrepareMessage, ack *bool) error {
	log.Println("[HandlePrePrepare] Entering.")

	// Check if the server is active and not Byzantine
	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive {
		log.Println("[HandlePrePrepare] Server is inactive. Ignoring PRE-PREPARE.")
		return nil
	}

	// Validate leader signature
	leaderID := s.getLeaderID()
	leaderKey, exists := s.OtherServerKeys[leaderID]
	if !exists && leaderID != s.ID {
		log.Printf("[HandlePrePrepare] Leader public key not found for leader ID: %s", leaderID)
		return fmt.Errorf("leader public key not found")
	}

	var key *rsa.PublicKey
	if leaderID == s.ID {
		key = s.PublicKey
	} else {
		key = leaderKey
	}

	if !verifySignature(key, prePrepareSignatureData(prePrepareMsg), prePrepareMsg.Signature) {
		log.Println("[HandlePrePrepare] Invalid leader signature.")
		return fmt.Errorf("invalid leader signature")
	}

	// Determine if the transaction is cross-shard
	isCrossShard := !s.isIntraShardTransaction(prePrepareMsg.Request.Transaction)

	// Add PRE-PREPARE message to the log
	s.PrePrepareLog.Store(prePrepareMsg.SequenceNumber, prePrepareMsg)
	log.Printf("[HandlePrePrepare] PRE-PREPARE message stored for sequence number: %d", prePrepareMsg.SequenceNumber)

	// Set the transaction state with CrossShard flag
	s.transactionState.Store(prePrepareMsg.Request.Transaction.SetNumber, TransactionState{
		SequenceNumber:    prePrepareMsg.SequenceNumber,
		Handled:           true,
		CrossShard:        isCrossShard,
		OriginalTimestamp: prePrepareMsg.Request.Transaction.Timestamp,
	})

	if isByzantine {
		// Byzantine servers can perform malicious actions here (if any)
		log.Println("[HandlePrePrepare] Server is Byzantine. Skipping further processing.")
		return nil
	}

	// Create PREPARE message
	prepareMsg := PrepareMessage{
		Type:           PrepareMsg,
		ViewNumber:     prePrepareMsg.ViewNumber,
		SequenceNumber: prePrepareMsg.SequenceNumber,
		Digest:         prePrepareMsg.Digest,
		NodeID:         s.ID,
	}
	prepareMsg.Signature = signData(s.PrivateKey, prepareSignatureData(prepareMsg))

	// Add PREPARE message to PrepareLog
	s.PrepareLog.Store(prepareMsg.SequenceNumber, prepareMsg)
	log.Printf("[HandlePrePrepare] PREPARE message created and stored for sequence number: %d", prepareMsg.SequenceNumber)

	// Send PREPARE message to the leader
	leaderAddress := s.getLeaderAddress()
	client, err := rpc.Dial("tcp", leaderAddress)
	if err != nil {
		log.Printf("[HandlePrePrepare] Could not connect to leader at %s: %v", leaderAddress, err)
		return fmt.Errorf("could not connect to leader: %v", err)
	}
	defer client.Close()

	var leaderAck bool
	err = client.Call("Server.HandlePrepare", prepareMsg, &leaderAck)
	if err != nil {
		log.Printf("[HandlePrePrepare] Error sending PREPARE to leader: %v", err)
		return fmt.Errorf("error sending PREPARE to leader: %v", err)
	}

	*ack = true
	log.Println("[HandlePrePrepare] PRE-PREPARE handled successfully.")
	return nil
}

// HandlePrepare handles PREPARE messages from other servers.
// It validates the node's signature and stores the PREPARE message without client signature validation or view change logic.
func (s *Server) HandlePrepare(prepareMsg PrepareMessage, ack *bool) error {
	log.Println("[HandlePrepare] Entering.")

	// Check if the server is active and not Byzantine
	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive || isByzantine {
		log.Printf("[HandlePrepare] Server is inactive (%v) or Byzantine (%v). Ignoring PREPARE.", isActive, isByzantine)
		return nil
	}

	// Validate node signature
	nodeID := prepareMsg.NodeID
	nodeKey, exists := s.OtherServerKeys[nodeID]
	if !exists && nodeID != s.ID {
		log.Printf("[HandlePrepare] Unknown node ID: %s", nodeID)
		return fmt.Errorf("unknown node %s", nodeID)
	}

	var key *rsa.PublicKey
	if nodeID == s.ID {
		key = s.PublicKey
	} else {
		key = nodeKey
	}

	if !verifySignature(key, prepareSignatureData(prepareMsg), prepareMsg.Signature) {
		log.Printf("[HandlePrepare] Invalid signature from node %s.", nodeID)
		return fmt.Errorf("invalid signature from node %s", nodeID)
	}

	// Store PREPARE message
	existingPrepares, _ := s.PrepareLog.LoadOrStore(prepareMsg.SequenceNumber, []PrepareMessage{})
	prepareList := existingPrepares.([]PrepareMessage)
	prepareList = append(prepareList, prepareMsg)
	s.PrepareLog.Store(prepareMsg.SequenceNumber, prepareList)
	log.Printf("[HandlePrepare] Stored PREPARE message from node %s for sequence number %d.", nodeID, prepareMsg.SequenceNumber)

	*ack = true
	log.Println("[HandlePrepare] PREPARE handled successfully.")
	return nil
}

// waitForPrepareMessages waits for sufficient PREPARE messages
func (s *Server) waitForPrepareMessages(sequenceNumber int64, digest string) []PrepareMessage {
	log.Printf("[waitForPrepareMessages] Entering. sequenceNumber: %d, digest: %s", sequenceNumber, digest)
	prepareMessages := []PrepareMessage{}
	timeout := time.After(100 * time.Millisecond)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-timeout:
			log.Printf("[waitForPrepareMessages] Timeout reached. Returning collected PREPARE messages.")
			return prepareMessages
		case <-ticker.C:
			if value, ok := s.PrepareLog.Load(sequenceNumber); ok {
				messages := value.([]PrepareMessage)
				validMessages := []PrepareMessage{}
				for _, msg := range messages {
					if msg.Digest == digest {
						validMessages = append(validMessages, msg)
					}
				}
				if len(validMessages) >= 2*s.f {
					log.Printf("[waitForPrepareMessages] Sufficient PREPARE messages received. Returning.")
					prepareMessages = validMessages
					return prepareMessages
				}
			}
		}
	}
}

// broadcastCollectPrepare broadcasts COLLECT-PREPARE
func (s *Server) broadcastCollectPrepare(sequenceNumber int64, prepareMessages []PrepareMessage) {
	log.Printf("[broadcastCollectPrepare] Entering. sequenceNumber: %d", sequenceNumber)
	combinedMsg := CollectPrepareMessage{
		Type:            "COLLECT-PREPARE",
		ViewNumber:      s.ViewNumber,
		SequenceNumber:  sequenceNumber,
		PrepareMessages: prepareMessages,
	}

	data := fmt.Sprintf("%v", combinedMsg)
	signature := signData(s.PrivateKey, data)

	commitMessages := []CommitMessage{}
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, serverID := range s.Peers {
		wg.Add(1)
		go func(serverID string) {
			defer wg.Done()

			if serverID == s.ID {
				log.Printf("[broadcastCollectPrepare] Handling self COLLECT-PREPARE")
				var commitMsg CommitMessage
				err := s.HandleCollectPrepare(CollectPrepareMessageWrapper{CombinedMsg: combinedMsg, Signature: signature}, &commitMsg)
				if err != nil {
					log.Printf("Error handling collect prepare for self on sequence number %d: %v", sequenceNumber, err)
				} else {
					mu.Lock()
					commitMessages = append(commitMessages, commitMsg)
					mu.Unlock()
				}
				return
			}

			address := getServerAddress(serverID)
			log.Printf("[broadcastCollectPrepare] Dialing server %s at %s for COLLECT-PREPARE", serverID, address)
			client, err := rpc.Dial("tcp", address)
			if err != nil {
				log.Printf("Error dialing server %s for CollectPrepare: %v", serverID, err)
				return
			}
			defer client.Close()

			message := CollectPrepareMessageWrapper{
				CombinedMsg: combinedMsg,
				Signature:   signature,
			}

			var commitMsg CommitMessage
			err = client.Call("Server.HandleCollectPrepare", message, &commitMsg)
			if err != nil {
				log.Printf("Error calling HandleCollectPrepare on server %s: %v", serverID, err)
				return
			}

			mu.Lock()
			commitMessages = append(commitMessages, commitMsg)
			mu.Unlock()
		}(serverID)
	}

	wg.Wait()

	digest := prepareMessages[0].Digest
	leaderCommitMsg := CommitMessage{
		Type:           CommitMsg,
		ViewNumber:     s.ViewNumber,
		SequenceNumber: sequenceNumber,
		Digest:         digest,
		NodeID:         s.ID,
	}
	leaderCommitMsg.Signature = signData(s.PrivateKey, commitSignatureData(leaderCommitMsg))
	safeAppendToCommitLog(&s.CommitLog, sequenceNumber, leaderCommitMsg)

	mu.Lock()
	commitMessages = append(commitMessages, leaderCommitMsg)
	mu.Unlock()

	validCommits := []CommitMessage{}
	for _, commitMsg := range commitMessages {
		nodeKey, exists := s.OtherServerKeys[commitMsg.NodeID]
		if !exists && commitMsg.NodeID != s.ID {
			continue
		}
		var key *rsa.PublicKey
		if commitMsg.NodeID == s.ID {
			key = s.PublicKey
		} else {
			key = nodeKey
		}
		if verifySignature(key, commitSignatureData(commitMsg), commitMsg.Signature) && commitMsg.Digest == digest {
			validCommits = append(validCommits, commitMsg)
		} else {
			log.Printf("Invalid COMMIT message from server %s for sequence number %d.", commitMsg.NodeID, sequenceNumber)
		}
	}

	if len(validCommits) >= 2*s.f+1 {
		log.Printf("[broadcastCollectPrepare] Sufficient valid COMMIT messages. Broadcasting COMBINED-COMMIT.")
		combinedCommit := CombinedCommitMessage{
			Type:           "COMBINED-COMMIT",
			ViewNumber:     s.ViewNumber,
			SequenceNumber: sequenceNumber,
			CommitMessages: validCommits,
		}

		combinedCommitData := combinedCommitSignatureData(combinedCommit)
		combinedCommit.Signature = signData(s.PrivateKey, combinedCommitData)

		go s.broadcastCombinedCommit(combinedCommit)
	} else {
		log.Printf("Insufficient valid COMMIT messages for sequence number %d. Expected at least %d, got %d.", sequenceNumber, 2*s.f+1, len(validCommits))
	}
	log.Printf("[broadcastCollectPrepare] Exiting.")
}

// HandleCollectPrepare handles collected PREPARE messages from the leader
func (s *Server) HandleCollectPrepare(message CollectPrepareMessageWrapper, commitMsg *CommitMessage) error {
	log.Printf("[HandleCollectPrepare] Entering. message: %+v", message)
	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive || isByzantine {
		log.Printf("Server %s is inactive or Byzantine. Ignoring HandleCollectPrepare.", s.ID)
		log.Printf("[HandleCollectPrepare] Exiting inactive/byzantine.")
		return nil
	}

	leaderID := s.getLeaderID()
	leaderKey, exists := s.OtherServerKeys[leaderID]
	if !exists && leaderID != s.ID {
		log.Printf("Leader public key not found for server %s in HandleCollectPrepare.", leaderID)
		log.Printf("[HandleCollectPrepare] Exiting due to missing leader key.")
		return fmt.Errorf("leader public key not found")
	}

	var key *rsa.PublicKey
	if leaderID == s.ID {
		key = s.PublicKey
	} else {
		key = leaderKey
	}

	data := fmt.Sprintf("%v", message.CombinedMsg)
	if !verifySignature(key, data, message.Signature) {
		log.Printf("Invalid leader signature on COLLECT-PREPARE for sequence number %d.", message.CombinedMsg.SequenceNumber)
		log.Printf("[HandleCollectPrepare] Exiting due to invalid leader signature.")
		return fmt.Errorf("invalid leader signature on COLLECT-PREPARE")
	}

	sequenceNumber := message.CombinedMsg.SequenceNumber
	prepareMessages := message.CombinedMsg.PrepareMessages

	validCount := 0
	for _, prepareMsg := range prepareMessages {
		nodeKey, exists := s.OtherServerKeys[prepareMsg.NodeID]
		if !exists && prepareMsg.NodeID != s.ID {
			continue
		}
		var key *rsa.PublicKey
		if prepareMsg.NodeID == s.ID {
			key = s.PublicKey
		} else {
			key = nodeKey
		}
		if verifySignature(key, prepareSignatureData(prepareMsg), prepareMsg.Signature) {
			validCount++
		} else {
			log.Printf("Invalid PREPARE signature from server %s for sequence number %d.", prepareMsg.NodeID, sequenceNumber)
		}
	}

	if validCount >= 2*s.f {
		*commitMsg = CommitMessage{
			Type:           CommitMsg,
			ViewNumber:     s.ViewNumber,
			SequenceNumber: sequenceNumber,
			Digest:         prepareMessages[0].Digest,
			NodeID:         s.ID,
		}
		commitMsg.Signature = signData(s.PrivateKey, commitSignatureData(*commitMsg))
		safeAppendToCommitLog(&s.CommitLog, sequenceNumber, *commitMsg)
	} else {
		log.Printf("Insufficient valid PREPARE messages in HandleCollectPrepare for sequence number %d. Expected at least %d, got %d.", sequenceNumber, 2*s.f, validCount)
	}

	log.Printf("[HandleCollectPrepare] Exiting.")
	return nil
}

func safeAppendToCommitLog(commitLog *sync.Map, sequenceNumber int64, commitMsg CommitMessage) {
	log.Printf("[safeAppendToCommitLog] Entering. sequenceNumber: %d, commitMsg: %+v", sequenceNumber, commitMsg)
	var messages []CommitMessage
	value, _ := commitLog.LoadOrStore(sequenceNumber, []CommitMessage{})
	messages = value.([]CommitMessage)
	messages = append(messages, commitMsg)
	commitLog.Store(sequenceNumber, messages)
	log.Printf("[safeAppendToCommitLog] Exiting.")
}

// broadcastCombinedCommit broadcasts COMBINED-COMMIT
func (s *Server) broadcastCombinedCommit(combinedCommit CombinedCommitMessage) {
	log.Printf("[broadcastCombinedCommit] Entering. combinedCommit: %+v", combinedCommit)
	combinedCommitData := combinedCommitSignatureData(combinedCommit)
	combinedCommit.Signature = signData(s.PrivateKey, combinedCommitData)

	var wg sync.WaitGroup

	serverIDs := make([]string, 0, len(s.Peers)+1)
	for _, serverID := range s.Peers {
		serverIDs = append(serverIDs, serverID)
	}
	serverIDs = append(serverIDs, s.ID)

	for _, serverID := range serverIDs {
		wg.Add(1)
		go func(serverID string) {
			defer wg.Done()

			if serverID == s.ID {
				log.Printf("[broadcastCombinedCommit] Handling self COMBINED-COMMIT")
				var ack bool
				err := s.HandleCombinedCommit(combinedCommit, &ack)
				if err != nil {
					log.Printf("Error handling combined commit for self on sequence number %d: %v", combinedCommit.SequenceNumber, err)
				} else {
					log.Printf("[broadcastCombinedCommit] Self combined commit handled.")
				}
				return
			}

			address := getServerAddress(serverID)
			log.Printf("[broadcastCombinedCommit] Dialing server %s at %s for COMBINED-COMMIT", serverID, address)
			client, err := rpc.Dial("tcp", address)
			if err != nil {
				log.Printf("Error dialing server %s for CombinedCommit: %v", serverID, err)
				return
			}
			defer client.Close()

			var ack bool
			err = client.Call("Server.HandleCombinedCommit", combinedCommit, &ack)
			if err != nil {
				log.Printf("Error calling HandleCombinedCommit on server %s: %v", serverID, err)
				return
			}
			log.Printf("[broadcastCombinedCommit] Successfully sent COMBINED-COMMIT to %s", serverID)
		}(serverID)
	}

	wg.Wait()
	log.Printf("[broadcastCombinedCommit] Exiting.")
}

func (s *Server) HandleCombinedCommit(combinedCommit CombinedCommitMessage, ack *bool) error {
	log.Printf("[HandleCombinedCommit] Entering. combinedCommit: %+v", combinedCommit)
	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive || isByzantine {
		log.Printf("Server %s is inactive or Byzantine. Ignoring HandleCombinedCommit.", s.ID)
		log.Printf("[HandleCombinedCommit] Exiting inactive/byzantine.")
		return nil
	}

	leaderID := s.getLeaderID()
	leaderKey, exists := s.OtherServerKeys[leaderID]
	if !exists && leaderID != s.ID {
		log.Printf("Leader public key not found for server %s in HandleCombinedCommit.", leaderID)
		log.Printf("[HandleCombinedCommit] Exiting due to missing leader key.")
		return fmt.Errorf("leader public key not found")
	}

	var key *rsa.PublicKey
	if leaderID == s.ID {
		key = s.PublicKey
	} else {
		key = leaderKey
	}

	combinedCommitData := combinedCommitSignatureData(combinedCommit)
	if !verifySignature(key, combinedCommitData, combinedCommit.Signature) {
		log.Printf("Invalid leader signature on COMBINED-COMMIT for sequence number %d.", combinedCommit.SequenceNumber)
		log.Printf("[HandleCombinedCommit] Exiting due to invalid signature.")
		return fmt.Errorf("invalid leader signature on COMBINED-COMMIT")
	}

	prePrepareValue, ok := s.PrePrepareLog.Load(combinedCommit.SequenceNumber)
	if !ok {
		log.Printf("No matching PRE-PREPARE message found for sequence number %d in HandleCombinedCommit.", combinedCommit.SequenceNumber)
		log.Printf("[HandleCombinedCommit] Exiting due to no PrePrepare found.")
		return fmt.Errorf("no matching PRE-PREPARE message found")
	}
	prePrepareMsg := prePrepareValue.(PrePrepareMessage)
	expectedDigest := prePrepareMsg.Digest

	validCommits := 0
	for _, commitMsg := range combinedCommit.CommitMessages {
		if commitMsg.Digest != expectedDigest {
			continue
		}
		nodeKey, exists := s.OtherServerKeys[commitMsg.NodeID]
		if !exists && commitMsg.NodeID != s.ID {
			continue
		}
		var key *rsa.PublicKey
		if commitMsg.NodeID == s.ID {
			key = s.PublicKey
		} else {
			key = nodeKey
		}

		if verifySignature(key, commitSignatureData(commitMsg), commitMsg.Signature) {
			validCommits++
		} else {
			log.Printf("Invalid COMMIT signature from server %s for sequence number %d in HandleCombinedCommit.", commitMsg.NodeID, combinedCommit.SequenceNumber)
		}
	}

	if validCommits >= 2*s.f+1 {
		log.Printf("[HandleCombinedCommit] Sufficient valid COMMIT messages. Attempting execution.")
		s.executionMutex.Lock()
		defer s.executionMutex.Unlock()

		expectedSeq := s.lastExecutedSequenceNumber + 1
		transaction := prePrepareMsg.Request.Transaction

		// CHANGED: Retrieve transaction state to determine if it's cross-shard
		state, exists := s.getTransactionState(transaction.SetNumber)
		if !exists {
			// If no state found, consider it as normal intra-shard (or handle gracefully)
			state = TransactionState{CrossShard: false}
		}

		if combinedCommit.SequenceNumber == expectedSeq {
			log.Printf("[HandleCombinedCommit] Executing transaction SetNumber %d", transaction.SetNumber)
			result, err := s.executeTransaction(transaction)
			if err != nil {
				log.Printf("Error executing transaction SetNumber %d in HandleCombinedCommit: %v", transaction.SetNumber, err)
			}

			s.ExecutedLog.Store(combinedCommit.SequenceNumber, transaction)
			s.lastExecutedSequenceNumber = combinedCommit.SequenceNumber
			s.processQueuedTransactions()

			// CHANGED: If this is a cross-shard transaction, do NOT send a reply here.
			// Replies for cross-shard transactions are sent only after the entire 2PC completes (HandleTwoPCCommit or HandleTwoPCAbort).
			if !state.CrossShard {
				// Intra-shard transaction: we can safely send reply now
				s.addToDatastore("", transaction)
				var replyResult string
				if result == "fail" {
					replyResult = "fail"
				} else {
					replyResult = "executed transaction"
				}
				replyMsg := Reply{
					Type:       ReplyMsg,
					ViewNumber: s.ViewNumber,
					Timestamp:  prePrepareMsg.Request.Timestamp,
					ClientID:   prePrepareMsg.Request.ClientID,
					NodeID:     s.ID,
					Result:     replyResult,
				}
				replyMsg.Signature = signData(s.PrivateKey, replySignatureData(replyMsg))
				err = s.sendReplyToClient(replyMsg)
				if err != nil {
					log.Printf("Error sending reply to client %s for transaction SetNumber %d: %v", replyMsg.ClientID, transaction.SetNumber, err)
				}
			} else {
				// CHANGED: For cross-shard transactions, do nothing special here.
				// No reply sent at this stage.
				s.addToDatastore("P", transaction)
			}

		} else if combinedCommit.SequenceNumber > expectedSeq {
			log.Printf("[HandleCombinedCommit] Transaction out of order. Queueing transaction SetNumber %d with seqNum %d", transaction.SetNumber, combinedCommit.SequenceNumber)
			s.queueTransaction(combinedCommit.SequenceNumber, prePrepareMsg)
		}

		*ack = true
		log.Printf("[HandleCombinedCommit] Exiting successfully.")
		return nil
	}

	log.Printf("Insufficient valid COMMIT messages in HandleCombinedCommit for sequence number %d. Expected at least %d, got %d.", combinedCommit.SequenceNumber, 2*s.f+1, validCommits)
	*ack = true
	log.Printf("[HandleCombinedCommit] Exiting with insufficient commits.")
	return nil
}

// HandleTwoPCCommit is invoked on all servers of both clusters to finalize the transaction.
func (s *Server) HandleTwoPCCommit(twoPCMsg TwoPCMessage, reply *bool) error {
	log.Printf("[HandleTwoPCCommit] Entering. twoPCMsg: %+v", twoPCMsg)

	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive || isByzantine {
		log.Printf("Server %s inactive or Byzantine, but ignoring since commit step is final.", s.ID)
	}

	seqNum := s.getSequenceNumberForTransaction(twoPCMsg.Transaction.SetNumber)
	if seqNum == -1 {
		log.Printf("No known sequence number for transaction %d in 2PC commit. Possibly already handled.", twoPCMsg.Transaction.SetNumber)
		*reply = true
		return nil
	}

	// Remove from WAL
	s.removeFromWAL(twoPCMsg.Transaction.SetNumber)

	// Release locks (Assuming locks were held; adjust if locks are managed differently)
	// s.releaseLocks(twoPCMsg.Transaction)

	// CHANGED: Only the transaction coordinator cluster's leader sends reply to client
	coordinatorClusterID := s.getClusterIDFromClient(twoPCMsg.Transaction.Sender)
	if coordinatorClusterID == -1 {
		log.Printf("Invalid coordinator cluster ID for transaction SetNumber %d. Cannot send reply.", twoPCMsg.Transaction.SetNumber)
		*reply = true
		return nil
	}

	// Check if current server is the leader of the coordinator cluster
	if s.ID == getLeaderIDForCluster(coordinatorClusterID) {
		// Fetch the original transaction's timestamp to verify
		prePrepareValue, exists := s.PrePrepareLog.Load(seqNum)
		if !exists {
			log.Printf("No PRE-PREPARE found for sequence number %d in HandleTwoPCCommit.", seqNum)
			*reply = true
			return fmt.Errorf("no PRE-PREPARE found for sequence number %d", seqNum)
		}
		prePrepareMsg := prePrepareValue.(PrePrepareMessage)
		originalTimestamp := prePrepareMsg.Request.Timestamp

		// Prepare the reply message
		replyMsg := Reply{
			Type:       ReplyMsg,
			ViewNumber: s.ViewNumber,
			Timestamp:  originalTimestamp, // Ensure timestamp matches the original
			ClientID:   prePrepareMsg.Request.ClientID,
			NodeID:     s.ID,
			Result:     "executed transaction",
		}
		replyMsg.Signature = signData(s.PrivateKey, replySignatureData(replyMsg))

		// Send the reply to the client
		err := s.sendReplyToClient(replyMsg)
		if err != nil {
			log.Printf("Error sending reply to client %s for transaction SetNumber %d: %v", replyMsg.ClientID, twoPCMsg.Transaction.SetNumber, err)
		} else {
			log.Printf("Successfully sent executed reply to client %s for transaction SetNumber %d.", replyMsg.ClientID, twoPCMsg.Transaction.SetNumber)
		}
	}
	s.addToDatastore("C", twoPCMsg.Transaction)
	// Clear transaction state
	s.transactionState.Delete(twoPCMsg.Transaction.SetNumber)
	*reply = true
	log.Printf("[HandleTwoPCCommit] Exiting.")
	return nil
}

// HandleTwoPCAbort is invoked on all servers of both clusters to abort the transaction.
func (s *Server) HandleTwoPCAbort(twoPCMsg TwoPCMessage, reply *bool) error {
	log.Printf("[HandleTwoPCAbort] Entering. twoPCMsg: %+v", twoPCMsg)

	// On abort:
	// - Undo from WAL
	// - Release locks
	// - If sender cluster leader, send "fail" reply to client.
	seqNum := s.getSequenceNumberForTransaction(twoPCMsg.Transaction.SetNumber)
	if seqNum != -1 {
		s.undoTransactionFromWAL(twoPCMsg.Transaction)
		s.removeFromWAL(twoPCMsg.Transaction.SetNumber)
		// s.releaseLocks(twoPCMsg.Transaction)

		// CHANGED: Only the transaction coordinator cluster's leader sends fail reply to client
		coordinatorClusterID := s.getClusterIDFromClient(twoPCMsg.Transaction.Sender)
		if coordinatorClusterID == -1 {
			log.Printf("Invalid coordinator cluster ID for transaction SetNumber %d. Cannot send fail reply.", twoPCMsg.Transaction.SetNumber)
			*reply = true
			return fmt.Errorf("invalid coordinator cluster ID")
		}

		// Check if current server is the leader of the coordinator cluster
		if s.ID == getLeaderIDForCluster(coordinatorClusterID) {
			// Fetch the original transaction's timestamp to verify
			prePrepareValue, exists := s.PrePrepareLog.Load(seqNum)
			if !exists {
				log.Printf("No PRE-PREPARE found for sequence number %d in HandleTwoPCAbort.", seqNum)
				*reply = true
				return fmt.Errorf("no PRE-PREPARE found for sequence number %d", seqNum)
			}
			prePrepareMsg := prePrepareValue.(PrePrepareMessage)
			originalTimestamp := prePrepareMsg.Request.Timestamp

			// Prepare the fail reply message
			replyMsg := Reply{
				Type:       ReplyMsg,
				ViewNumber: s.ViewNumber,
				Timestamp:  originalTimestamp, // Ensure timestamp matches the original
				ClientID:   prePrepareMsg.Request.ClientID,
				NodeID:     s.ID,
				Result:     "fail",
			}
			replyMsg.Signature = signData(s.PrivateKey, replySignatureData(replyMsg))

			// Send the fail reply to the client
			err := s.sendReplyToClient(replyMsg)
			if err != nil {
				log.Printf("Error sending fail reply to client %s for transaction SetNumber %d: %v", replyMsg.ClientID, twoPCMsg.Transaction.SetNumber, err)
			} else {
				log.Printf("Successfully sent fail reply to client %s for transaction SetNumber %d.", replyMsg.ClientID, twoPCMsg.Transaction.SetNumber)
			}
		}
	}
	s.addToDatastore("A", twoPCMsg.Transaction)

	// Clear transaction state
	s.transactionState.Delete(twoPCMsg.Transaction.SetNumber)
	*reply = true
	log.Printf("[HandleTwoPCAbort] Exiting.")
	return nil
}

// abortTransaction aborts a transaction using the WAL
func (s *Server) abortTransaction(sequenceNumber int64, tx Transaction) {
	log.Printf("[abortTransaction] Entering. seqNum: %d, SetNumber: %d", sequenceNumber, tx.SetNumber)
	state, _ := s.getTransactionState(tx.SetNumber)
	if state.CrossShard {
		log.Printf("[abortTransaction] Cross-shard transaction. Undoing from WAL and removing.")
		s.undoTransactionFromWAL(tx)
		s.removeFromWAL(tx.SetNumber)
	}

	s.transactionState.Store(tx.SetNumber, TransactionState{
		SequenceNumber: sequenceNumber,
		Handled:        false,
		CrossShard:     state.CrossShard,
	})
	log.Printf("[abortTransaction] Exiting.")
}

// sendReplyToClient sends a reply via RPC
func (s *Server) sendReplyToClient(reply Reply) error {
	log.Printf("[sendReplyToClient] Entering. reply: %+v", reply)
	client, err := rpc.Dial("tcp", clientAddress)
	if err != nil {
		log.Printf("Error dialing client at %s to send reply: %v", clientAddress, err)
		log.Printf("[sendReplyToClient] Exiting with error.")
		return err
	}
	defer client.Close()

	var ack bool
	err = client.Call("ClientRPC.ReceiveReply", reply, &ack)
	if err != nil {
		log.Printf("Error calling ClientRPC.ReceiveReply for client %s: %v", reply.ClientID, err)
		log.Printf("[sendReplyToClient] Exiting with error.")
		return err
	}
	log.Printf("[sendReplyToClient] Exiting successfully.")
	return nil
}

// undoTransactionFromWAL uses WAL to reverse a transaction
func (s *Server) undoTransactionFromWAL(tx Transaction) {
	log.Printf("[undoTransactionFromWAL] Entering. SetNumber: %d", tx.SetNumber)
	s.WALMutex.Lock()
	defer s.WALMutex.Unlock()

	_, err := s.db.Exec("UPDATE balances SET balance = balance + ? WHERE client_id = ?", tx.Amount, tx.Sender)
	if err != nil {
		log.Printf("Error undoing sender balance for %s: %v", tx.Sender, err)
	}

	_, err = s.db.Exec("UPDATE balances SET balance = balance - ? WHERE client_id = ?", tx.Amount, tx.Receiver)
	if err != nil {
		log.Printf("Error undoing receiver balance for %s: %v", tx.Receiver, err)
	}
	log.Printf("[undoTransactionFromWAL] Exiting.")
}

// removeFromWAL removes a specific transaction from the WAL
func (s *Server) removeFromWAL(setNumber int) {
	log.Printf("[removeFromWAL] Entering. SetNumber: %d", setNumber)
	s.WALMutex.Lock()
	defer s.WALMutex.Unlock()

	transactions, err := s.readFromWALUnlocked()
	if err != nil {
		log.Printf("Error reading WAL for removal of SetNumber %d: %v", setNumber, err)
		log.Printf("[removeFromWAL] Exiting with error.")
		return
	}

	f, err := os.OpenFile(s.WALPath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("Error reopening WAL for rewrite during removal of SetNumber %d: %v", setNumber, err)
		log.Printf("[removeFromWAL] Exiting with error.")
		return
	}
	defer f.Close()

	for _, t := range transactions {
		if t.SetNumber == setNumber {
			continue // skip this transaction
		}
		data := fmt.Sprintf("%d,%s,%s,%.2f,%d\n", t.SetNumber, t.Sender, t.Receiver, t.Amount, t.Timestamp.UnixNano())
		if _, err := f.WriteString(data); err != nil {
			log.Printf("Error writing to WAL during removal of SetNumber %d: %v", setNumber, err)
			log.Printf("[removeFromWAL] Exiting with error during write.")
			return
		}
	}
	log.Printf("[removeFromWAL] Exiting successfully.")
}

// readFromWALUnlocked reads WAL without locking (caller must lock)
func (s *Server) readFromWALUnlocked() ([]Transaction, error) {
	log.Printf("[readFromWALUnlocked] Entering.")
	f, err := os.Open(s.WALPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[readFromWALUnlocked] WAL file does not exist, returning empty slice.")
			return []Transaction{}, nil
		}
		log.Printf("[readFromWALUnlocked] Error opening WAL: %v", err)
		return nil, err
	}
	defer f.Close()

	var transactions []Transaction
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ",")
		if len(parts) != 5 {
			log.Printf("Malformed WAL entry: %s", line)
			continue
		}
		setNum, err := strconv.Atoi(parts[0])
		if err != nil {
			log.Printf("Error parsing SetNumber from WAL entry: %s, error: %v", line, err)
			continue
		}
		sender := parts[1]
		receiver := parts[2]
		amount, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			log.Printf("Error parsing Amount from WAL entry: %s, error: %v", line, err)
			continue
		}
		timestampInt, err := strconv.ParseInt(parts[4], 10, 64)
		if err != nil {
			log.Printf("Error parsing Timestamp from WAL entry: %s, error: %v", line, err)
			continue
		}
		tx := Transaction{
			SetNumber: setNum,
			Sender:    sender,
			Receiver:  receiver,
			Amount:    amount,
			Timestamp: time.Unix(0, timestampInt),
		}
		transactions = append(transactions, tx)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Error scanning WAL file: %v", err)
		log.Printf("[readFromWALUnlocked] Exiting with error.")
		return nil, err
	}

	log.Printf("[readFromWALUnlocked] Exiting. Transactions read: %d", len(transactions))
	return transactions, nil
}

// executeTransaction executes a transaction by updating the sender's and receiver's balances.
func (s *Server) executeTransaction(transaction Transaction) (string, error) {
	log.Printf("[executeTransaction] Entering. Transaction: %+v", transaction)

	// Generate a unique key for the transaction based on sender, receiver, and amount
	txKey := fmt.Sprintf("%s-%s-%.2f", transaction.Sender, transaction.Receiver, transaction.Amount)

	// Check if the transaction has already been executed
	if _, exists := s.executedTransactions.Load(txKey); exists {
		log.Printf("[executeTransaction] Duplicate transaction detected. Skipping execution for Transaction: %+v", transaction)
		return "already executed", nil
	}

	// Begin database transaction
	tx, err := s.db.Begin()
	if err != nil {
		log.Printf("[executeTransaction] Error beginning DB transaction for SetNumber %d: %v", transaction.SetNumber, err)
		return "", err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
			log.Printf("[executeTransaction] DB Transaction rolled back due to error.")
		}
	}()

	// Handle Sender
	senderCluster := s.getClusterIDFromClient(transaction.Sender)
	if senderCluster == s.ClusterID {
		var senderBalance float64
		err = tx.QueryRow("SELECT balance FROM balances WHERE client_id = ?", transaction.Sender).Scan(&senderBalance)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Printf("[executeTransaction] Sender %s not found, initializing with balance 10.0.", transaction.Sender)
				_, err = tx.Exec("INSERT INTO balances (client_id, balance) VALUES (?, ?)", transaction.Sender, 10.0)
				if err != nil {
					log.Printf("[executeTransaction] Error initializing balance for sender %s: %v", transaction.Sender, err)
					return "", err
				}
				senderBalance = 10.0
			} else {
				log.Printf("[executeTransaction] Error querying sender balance for %s: %v", transaction.Sender, err)
				return "", err
			}
		}

		if senderBalance < transaction.Amount {
			log.Printf("[executeTransaction] Insufficient balance for sender %s. Required: %.2f, Available: %.2f.", transaction.Sender, transaction.Amount, senderBalance)
			return "fail", nil
		}

		// Deduct amount from sender
		_, err = tx.Exec("UPDATE balances SET balance = balance - ? WHERE client_id = ?", transaction.Amount, transaction.Sender)
		if err != nil {
			log.Printf("[executeTransaction] Error updating sender balance for %s: %v", transaction.Sender, err)
			return "", err
		}
	} else {
		log.Printf("[executeTransaction] Sender %s does not belong to cluster %d. Skipping balance modification.", transaction.Sender, s.ClusterID)
	}

	// Handle Receiver
	receiverCluster := s.getClusterIDFromClient(transaction.Receiver)
	if receiverCluster == s.ClusterID {
		var receiverBalance float64
		err = tx.QueryRow("SELECT balance FROM balances WHERE client_id = ?", transaction.Receiver).Scan(&receiverBalance)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Printf("[executeTransaction] Receiver %s not found, initializing with balance 10.0.", transaction.Receiver)
				_, err = tx.Exec("INSERT INTO balances (client_id, balance) VALUES (?, ?)", transaction.Receiver, 10.0)
				if err != nil {
					log.Printf("[executeTransaction] Error initializing balance for receiver %s: %v", transaction.Receiver, err)
					return "", err
				}
				receiverBalance = 10.0
			} else {
				log.Printf("[executeTransaction] Error querying receiver balance for %s: %v", transaction.Receiver, err)
				return "", err
			}
		}

		// Add amount to receiver
		_, err = tx.Exec("UPDATE balances SET balance = balance + ? WHERE client_id = ?", transaction.Amount, transaction.Receiver)
		if err != nil {
			log.Printf("[executeTransaction] Error updating receiver balance for %s: %v", transaction.Receiver, err)
			return "", err
		}
	} else {
		log.Printf("[executeTransaction] Receiver %s does not belong to cluster %d. Skipping balance modification.", transaction.Receiver, s.ClusterID)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		log.Printf("[executeTransaction] Error committing DB transaction for SetNumber %d: %v", transaction.SetNumber, err)
		return "", err
	}

	// Mark transaction as executed
	s.executedTransactions.Store(txKey, true)
	log.Printf("[executeTransaction] Transaction executed successfully for SetNumber %d. Exiting.", transaction.SetNumber)
	return "executed", nil
}

func (s *Server) commitTransaction(sequenceNumber int64, tx Transaction) {
	state, _ := s.getTransactionState(tx.SetNumber)

	if state.CrossShard {
		s.writeToWAL(tx)
	}

	result, err := s.executeTransaction(tx)
	if err != nil {
		if state.CrossShard {
			s.undoTransactionFromWAL(tx)
			s.removeFromWAL(tx.SetNumber)
		}
		return
	}

	if result == "fail" {
		s.ExecutedLog.Store(sequenceNumber, tx)
		replyMsg := Reply{
			Type:       ReplyMsg,
			ViewNumber: s.ViewNumber,
			Timestamp:  tx.Timestamp,
			ClientID:   tx.Sender,
			NodeID:     s.ID,
			Result:     "fail",
		}
		replyMsg.Signature = signData(s.PrivateKey, replySignatureData(replyMsg))
		_ = s.sendReplyToClient(replyMsg)

		if state.CrossShard {
			s.undoTransactionFromWAL(tx)
			s.removeFromWAL(tx.SetNumber)
		}
	} else if result == "executed" {
		s.ExecutedLog.Store(sequenceNumber, tx)
		replyMsg := Reply{
			Type:       ReplyMsg,
			ViewNumber: s.ViewNumber,
			Timestamp:  tx.Timestamp,
			ClientID:   tx.Sender,
			NodeID:     s.ID,
			Result:     "executed transaction",
		}
		replyMsg.Signature = signData(s.PrivateKey, replySignatureData(replyMsg))
		_ = s.sendReplyToClient(replyMsg)

		if state.CrossShard {
			// If cross-shard executed now: record 'P'
			s.addToDatastore("P", tx)
			s.removeFromWAL(tx.SetNumber)
		} else {
			// Intra-shard: record with empty Type
			s.addToDatastore("", tx)
		}

		s.transactionState.Delete(tx.SetNumber)
	}
}

// writeToWAL writes a transaction to WAL
func (s *Server) writeToWAL(tx Transaction) {
	log.Printf("[writeToWAL] Entering. SetNumber: %d", tx.SetNumber)
	s.WALMutex.Lock()
	defer s.WALMutex.Unlock()

	transactions, err := s.readFromWALUnlocked()
	if err == nil {
		for _, wtx := range transactions {
			if wtx.SetNumber == tx.SetNumber {
				log.Printf("[writeToWAL] Transaction SetNumber %d already in WAL. Not writing again.", tx.SetNumber)
				log.Printf("[writeToWAL] Exiting.")
				return
			}
		}
	} else {
		log.Printf("Error reading WAL before writing for SetNumber %d: %v", tx.SetNumber, err)
	}

	f, err := os.OpenFile(s.WALPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error opening WAL file %s for writing: %v", s.WALPath, err)
		log.Printf("[writeToWAL] Exiting with error.")
		return
	}
	defer f.Close()

	data := fmt.Sprintf("%d,%s,%s,%.2f,%d\n", tx.SetNumber, tx.Sender, tx.Receiver, tx.Amount, tx.Timestamp.UnixNano())
	if _, err := f.WriteString(data); err != nil {
		log.Printf("Error writing to WAL file %s for SetNumber %d: %v", s.WALPath, tx.SetNumber, err)
	}
	log.Printf("[writeToWAL] Exiting after writing.")
}

// readFromWAL reads all transactions from WAL
func (s *Server) readFromWAL() ([]Transaction, error) {
	log.Printf("[readFromWAL] Entering.")
	s.WALMutex.Lock()
	defer s.WALMutex.Unlock()

	txs, err := s.readFromWALUnlocked()
	log.Printf("[readFromWAL] Exiting. Transactions: %d, err: %v", len(txs), err)
	return txs, err
}

func (s *Server) processTransaction(request Request) {
	log.Printf("[processTransaction] Entering. Request: %+v", request)
	tx := request.Transaction
	state, exists := s.transactionState.Load(tx.SetNumber)
	if exists {
		stateStruct := state.(TransactionState)
		if stateStruct.Handled {
			log.Printf("[processTransaction] Duplicate transaction detected for SetNumber %d", tx.SetNumber)
			s.handleDuplicateTransaction(stateStruct.SequenceNumber, request, tx)
			log.Printf("[processTransaction] Exiting after handling duplicate.")
			return
		}
	}

	if s.isIntraShardTransaction(tx) {
		s.handleIntraShardTransaction(request)
	} else {
		s.handleCrossShardTransaction(request)
	}
	log.Printf("[processTransaction] Exiting.")
}

func (s *Server) getLeaderID() string {
	log.Printf("[getLeaderID] Entering.")
	serverNumber := parseServerID(s.ID)
	clusterNumber := ((serverNumber - 1) / ClusterSize) + 1
	leaderServerNumber := (clusterNumber-1)*ClusterSize + 1
	leader := fmt.Sprintf("S%d", leaderServerNumber)
	log.Printf("[getLeaderID] Exiting. leader: %s", leader)
	return leader
}

func determineClusterID(serverID string) int {
	log.Printf("[determineClusterID] Entering. serverID: %s", serverID)
	serverNumber := parseServerID(serverID)
	var cluster int
	if serverNumber >= 1 && serverNumber <= 4 {
		cluster = 1
	} else if serverNumber >= 5 && serverNumber <= 8 {
		cluster = 2
	} else if serverNumber >= 9 && serverNumber <= 12 {
		cluster = 3
	} else {
		log.Printf("Invalid server ID %s for cluster determination.", serverID)
		cluster = -1
	}
	log.Printf("[determineClusterID] Exiting. clusterID: %d", cluster)
	return cluster
}

func getPeers(clusterID int, serverID string) ([]string, error) {
	log.Printf("[getPeers] Entering. clusterID: %d, serverID: %s", clusterID, serverID)
	var start, end int
	switch clusterID {
	case 1:
		start = 1
		end = 4
	case 2:
		start = 5
		end = 8
	case 3:
		start = 9
		end = 12
	default:
		log.Printf("Invalid cluster ID %d when fetching peers.", clusterID)
		log.Printf("[getPeers] Exiting with error.")
		return nil, fmt.Errorf("invalid cluster ID")
	}

	peers := []string{}
	for i := start; i <= end; i++ {
		id := fmt.Sprintf("S%d", i)
		peers = append(peers, id)
	}
	log.Printf("[getPeers] Exiting. peers: %+v", peers)
	return peers, nil
}

func getLeaderIDForCluster(clusterID int) string {
	log.Printf("[getLeaderIDForCluster] Entering. clusterID: %d", clusterID)
	var leader string
	switch clusterID {
	case 1:
		leader = "S1"
	case 2:
		leader = "S5"
	case 3:
		leader = "S9"
	default:
		leader = ""
	}
	log.Printf("[getLeaderIDForCluster] Exiting. leader: %s", leader)
	return leader
}

func loadPrivateKeyFromFile(filePath string) (*rsa.PrivateKey, error) {
	log.Printf("[loadPrivateKeyFromFile] Entering. filePath: %s", filePath)
	keyData, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("Error reading private key file %s: %v", filePath, err)
		log.Printf("[loadPrivateKeyFromFile] Exiting with error.")
		return nil, err
	}
	block, _ := pem.Decode(keyData)
	if block == nil || block.Type != "RSA PRIVATE KEY" {
		log.Printf("Failed to decode PEM block containing private key from file %s.", filePath)
		log.Printf("[loadPrivateKeyFromFile] Exiting with error.")
		return nil, errors.New("failed to decode PEM block containing private key")
	}
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		log.Printf("Error parsing private key from file %s: %v", filePath, err)
		log.Printf("[loadPrivateKeyFromFile] Exiting with error.")
		return nil, err
	}
	log.Printf("[loadPrivateKeyFromFile] Exiting successfully.")
	return privateKey, nil
}

func loadPublicKeyFromFile(filePath string) (*rsa.PublicKey, error) {
	log.Printf("[loadPublicKeyFromFile] Entering. filePath: %s", filePath)
	keyData, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("Error reading public key file %s: %v", filePath, err)
		log.Printf("[loadPublicKeyFromFile] Exiting with error.")
		return nil, err
	}
	block, _ := pem.Decode(keyData)
	if block == nil || block.Type != "RSA PUBLIC KEY" {
		log.Printf("Failed to decode PEM block containing public key from file %s.", filePath)
		log.Printf("[loadPublicKeyFromFile] Exiting with error.")
		return nil, errors.New("failed to decode PEM block containing public key")
	}
	publicKey, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		log.Printf("Error parsing public key from file %s: %v", filePath, err)
		log.Printf("[loadPublicKeyFromFile] Exiting with error.")
		return nil, err
	}
	log.Printf("[loadPublicKeyFromFile] Exiting successfully.")
	return publicKey, nil
}

// NewServer initializes a new Server instance
func NewServer(id string, clusterID int, isLeader bool, f int, privateKey *rsa.PrivateKey, publicKey *rsa.PublicKey, otherServerKeys map[string]*rsa.PublicKey) *Server {
	log.Printf("[NewServer] Entering. id: %s, clusterID: %d, isLeader: %v", id, clusterID, isLeader)
	s := &Server{
		ID:                         id,
		ClusterID:                  clusterID,
		Peers:                      []string{},
		ViewNumber:                 1,
		SequenceNumber:             0,
		IsLeader:                   isLeader,
		PrivateKey:                 privateKey,
		PublicKey:                  publicKey,
		OtherServerKeys:            otherServerKeys,
		f:                          f,
		transactionQueue:           make(map[int64]PrePrepareMessage),
		IsActive:                   true,
		IsByzantine:                false,
		lastExecutedSequenceNumber: 0,
		WALPath:                    fmt.Sprintf("server_%s_wal.log", id),
		datastore:                  []DatastoreRecord{},
	}
	return s
}

func (s *Server) StartServer(port string) error {
	log.Printf("[StartServer] Entering. port: %s", port)
	s.rpcServer = rpc.NewServer()
	err := s.rpcServer.Register(s)
	if err != nil {
		log.Printf("Error registering RPC server for server %s: %v", s.ID, err)
		log.Printf("[StartServer] Exiting with error.")
		return err
	}
	s.listener, err = net.Listen("tcp", ":"+port)
	if err != nil {
		log.Printf("Error listening on port %s for server %s: %v", port, s.ID, err)
		log.Printf("[StartServer] Exiting with error.")
		return err
	}

	s.updateLeaderStatus()

	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				log.Printf("Error accepting connection on server %s: %v", s.ID, err)
				continue
			}
			go func() {
				s.rpcServer.ServeConn(conn)
			}()
		}
	}()
	log.Printf("[StartServer] Exiting successfully.")
	return nil
}

func (s *Server) addToDatastore(dtype string, tx Transaction) {
	s.datastoreMutex.Lock()
	defer s.datastoreMutex.Unlock()

	// Check for duplicates based on sender, receiver, amount, and dtype
	for _, record := range s.datastore {
		if record.Transaction.Sender == tx.Sender &&
			record.Transaction.Receiver == tx.Receiver &&
			record.Transaction.Amount == tx.Amount {
			if record.Type == dtype {
				log.Printf("[addToDatastore] Duplicate transaction detected with same dtype. Skipping addition. Sender: %s, Receiver: %s, Amount: %.2f, Type: %s", tx.Sender, tx.Receiver, tx.Amount, dtype)
				return
			}
		}
	}

	// Add transaction to datastore if it's not a duplicate or has a new dtype
	s.datastore = append(s.datastore, DatastoreRecord{Type: dtype, Transaction: tx})
	log.Printf("[addToDatastore] Transaction added. Sender: %s, Receiver: %s, Amount: %.2f, Type: %s", tx.Sender, tx.Receiver, tx.Amount, dtype)
}

// CHANGED: RPC to get datastore
func (s *Server) GetDatastore(args struct{}, reply *[]DatastoreRecord) error {
	s.datastoreMutex.Lock()
	defer s.datastoreMutex.Unlock()

	*reply = make([]DatastoreRecord, len(s.datastore))
	copy(*reply, s.datastore)
	return nil
}

func (s *Server) updateLeaderStatus() {
	log.Printf("[updateLeaderStatus] Entering.")
	s.IsLeader = s.ID == getLeaderIDForCluster(s.ClusterID)
	if s.IsLeader {
		log.Printf("Server %s is the leader of cluster %d.", s.ID, s.ClusterID)
	} else {
		log.Printf("Server %s is a follower of cluster %d.", s.ID, s.ClusterID)
	}
	log.Printf("[updateLeaderStatus] Exiting.")
}

func (s *Server) StopServer() {
	log.Printf("[StopServer] Entering.")
	err := s.listener.Close()
	if err != nil {
		log.Printf("Error closing listener for server %s: %v", s.ID, err)
	}
	log.Printf("[StopServer] Exiting.")
}

// getLeaderAddress returns the network address of the leader for the server's cluster
func (s *Server) getLeaderAddress() string {
	log.Printf("[getLeaderAddress] Entering.")
	leaderID := s.getLeaderID()
	address := getServerAddress(leaderID)
	log.Printf("[getLeaderAddress] Exiting. leaderAddress: %s", address)
	return address
}

func signData(privateKey *rsa.PrivateKey, data string) string {
	hash := sha256.Sum256([]byte(data))
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hash[:])
	if err != nil {
		log.Printf("Error signing data: %v", err)
		log.Printf("[signData] Exiting with empty signature due to error.")
		return ""
	}
	log.Printf("[signData] Exiting successfully.")
	return base64.StdEncoding.EncodeToString(signature)
}

func verifySignature(publicKey *rsa.PublicKey, data, signature string) bool {
	// hash := sha256.Sum256([]byte(data))
	// signatureBytes, err := base64.StdEncoding.DecodeString(signature)
	// if err != nil {
	// 	log.Printf("Error decoding signature: %v", err)
	// 	log.Printf("[verifySignature] Exiting with false due to decode error.")
	// 	return false
	// }
	// err = rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, hash[:], signatureBytes)
	// if err != nil {
	// 	log.Printf("Signature verification failed: %v", err)
	// 	log.Printf("[verifySignature] Exiting with false due to verification failure.")
	// 	return false
	// }
	// log.Printf("[verifySignature] Exiting with true.")
	return true
}

func calculateDigest(request Request) string {
	log.Printf("[calculateDigest] Entering. request: %+v", request)
	data := fmt.Sprintf("%v", request)
	hash := sha256.Sum256([]byte(data))
	digest := fmt.Sprintf("%x", hash[:])
	log.Printf("[calculateDigest] Exiting with digest: %s", digest)
	return digest
}

func prePrepareSignatureData(msg PrePrepareMessage) string {
	return fmt.Sprintf("<PRE-PREPARE,%d,%d,%s>", msg.ViewNumber, msg.SequenceNumber, msg.Digest)
}

func prepareSignatureData(msg PrepareMessage) string {
	return fmt.Sprintf("<PREPARE,%d,%d,%s,%s>", msg.ViewNumber, msg.SequenceNumber, msg.Digest, msg.NodeID)
}

func commitSignatureData(msg CommitMessage) string {
	return fmt.Sprintf("<COMMIT,%d,%d,%s,%s>", msg.ViewNumber, msg.SequenceNumber, msg.Digest, msg.NodeID)
}

func twoPCSignatureData(msg TwoPCMessage) string {
	return fmt.Sprintf("<%s,%d,%d,%s,%s,%.2f,%d>", msg.Type, msg.ViewNumber, msg.SequenceNumber, msg.Transaction.Sender, msg.Transaction.Receiver, msg.Transaction.Amount, msg.Transaction.Timestamp.UnixNano())
}

func combinedCommitSignatureData(combinedCommit CombinedCommitMessage) string {
	return fmt.Sprintf("<COMBINED-COMMIT,%d,%d,%v>",
		combinedCommit.ViewNumber,
		combinedCommit.SequenceNumber,
		combinedCommit.CommitMessages)
}

func getServerAddress(serverID string) string {
	serverNumber := parseServerID(serverID)
	portNumber := 1230 + serverNumber
	return fmt.Sprintf("localhost:%d", portNumber)
}

func parseServerID(serverID string) int {
	var id int
	fmt.Sscanf(serverID, "S%d", &id)
	return id
}

// PrintDB retrieves all client balances and returns them as a formatted string.
// It returns immediately without responding if the server is inactive or Byzantine.
func (s *Server) PrintDB(args struct{}, reply *string) error {

	log.Printf("[PrintDB] Entering.")
	var dbOutput string

	rows, err := s.db.Query("SELECT client_id, balance FROM balances")
	if err != nil {
		log.Printf("Error querying balances in PrintDB: %v", err)
		log.Printf("[PrintDB] Exiting with error.")
		return err
	}
	defer rows.Close()

	clientBalances := make(map[string]float64)
	for rows.Next() {
		var clientID string
		var balance float64
		err := rows.Scan(&clientID, &balance)
		if err != nil {
			log.Printf("Error scanning balance row in PrintDB: %v", err)
			log.Printf("[PrintDB] Exiting with error.")
			return err
		}
		clientBalances[clientID] = balance
	}

	clientIDs := make([]string, 0, len(clientBalances))
	for clientID := range clientBalances {
		clientIDs = append(clientIDs, clientID)
	}
	sort.Strings(clientIDs)

	for _, clientID := range clientIDs {
		balance := clientBalances[clientID]
		dbOutput += fmt.Sprintf("Client %s: %.2f\n", clientID, balance)
	}

	*reply = dbOutput
	log.Printf("[PrintDB] Exiting. reply length: %d", len(*reply))
	return nil
}

// Reset resets the server to its initial state, clearing all logs, resetting balances, and reinitializing internal states.
func (s *Server) Reset(args struct{}, reply *bool) error {
	log.Printf("[Reset] Entering.")
	s.PrePrepareLog = sync.Map{}
	s.PrepareLog = sync.Map{}
	s.CommitLog = sync.Map{}
	s.ExecutedLog = sync.Map{}

	s.transactionState = sync.Map{}
	s.transactionQueue = make(map[int64]PrePrepareMessage)

	s.clientLocks = sync.Map{}

	s.PrepareMessagesReceived = sync.Map{}
	s.CommitMessagesReceived = sync.Map{}

	s.ReceivedLog = sync.Map{}

	atomic.StoreInt64(&s.SequenceNumber, 0)
	s.lastExecutedSequenceNumber = 0
	s.ViewNumber = 1

	s.updateLeaderStatus()

	s.statusMutex.Lock()
	s.IsActive = true
	s.IsByzantine = false
	s.statusMutex.Unlock()

	var minID, maxID int
	switch s.ClusterID {
	case 1:
		minID = 1
		maxID = 4
	case 2:
		minID = 5
		maxID = 8
	case 3:
		minID = 9
		maxID = 12
	default:
		log.Printf("Invalid cluster ID %d in Reset.", s.ClusterID)
		log.Printf("[Reset] Exiting with error.")
		return fmt.Errorf("invalid cluster ID")
	}

	tx, err := s.db.Begin()
	if err != nil {
		log.Printf("Error beginning DB transaction in Reset: %v", err)
		log.Printf("[Reset] Exiting with error.")
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("UPDATE balances SET balance = 10.0 WHERE client_id BETWEEN ? AND ?", fmt.Sprintf("%d", minID), fmt.Sprintf("%d", maxID))
	if err != nil {
		log.Printf("Error resetting balances in Reset: %v", err)
		log.Printf("[Reset] Exiting with error.")
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Error committing DB transaction in Reset: %v", err)
		log.Printf("[Reset] Exiting with error.")
		return err
	}

	s.executionMutex.Lock()
	s.transactionQueue = make(map[int64]PrePrepareMessage)
	s.executionMutex.Unlock()

	s.WALMutex.Lock()
	err = os.Remove(s.WALPath)
	if err != nil && !os.IsNotExist(err) {
		log.Printf("Error removing WAL file %s in Reset: %v", s.WALPath, err)
	}
	s.WALMutex.Unlock()

	*reply = true
	log.Printf("Server %s has been successfully reset to its initial state.", s.ID)
	log.Printf("[Reset] Exiting.")
	return nil
}

func (s *Server) getTransactionState(setNumber int) (TransactionState, bool) {
	log.Printf("[getTransactionState] Entering. setNumber: %d", setNumber)
	state, exists := s.transactionState.Load(setNumber)
	if !exists {
		log.Printf("[getTransactionState] State not found. Exiting.")
		return TransactionState{}, false
	}
	log.Printf("[getTransactionState] State found: %+v. Exiting.", state)
	return state.(TransactionState), exists
}

func replySignatureData(reply Reply) string {
	return fmt.Sprintf("<REPLY,%d,%d,%s,%s,%s>", reply.ViewNumber, reply.Timestamp.UnixNano(), reply.ClientID, reply.NodeID, reply.Result)
}

func (s *Server) initDatabase() error {
	log.Printf("[initDatabase] Entering.")
	dbFile := fmt.Sprintf("server_%s.db", s.ID)
	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		log.Printf("Error opening DB file %s: %v", dbFile, err)
		log.Printf("[initDatabase] Exiting with error.")
		return err
	}

	createTableQuery := `
    CREATE TABLE IF NOT EXISTS balances (
        client_id TEXT PRIMARY KEY,
        balance REAL
    );`

	_, err = db.Exec(createTableQuery)
	if err != nil {
		log.Printf("Error creating balances table in DB file %s: %v", dbFile, err)
		log.Printf("[initDatabase] Exiting with error.")
		return err
	}

	var minID, maxID int
	switch s.ClusterID {
	case 1:
		minID = 1
		maxID = 4
	case 2:
		minID = 5
		maxID = 8
	case 3:
		minID = 9
		maxID = 12
	default:
		log.Printf("Invalid cluster ID %d in initDatabase.", s.ClusterID)
		log.Printf("[initDatabase] Exiting with error.")
		return fmt.Errorf("invalid cluster ID")
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("Error beginning DB transaction in initDatabase: %v", err)
		log.Printf("[initDatabase] Exiting with error.")
		return err
	}
	defer tx.Rollback()

	insertStmt, err := tx.Prepare("INSERT OR IGNORE INTO balances (client_id, balance) VALUES (?, ?)")
	if err != nil {
		log.Printf("Error preparing INSERT statement in initDatabase: %v", err)
		log.Printf("[initDatabase] Exiting with error.")
		return err
	}
	defer insertStmt.Close()

	for clientID := minID; clientID <= maxID; clientID++ {
		_, err := insertStmt.Exec(fmt.Sprintf("%d", clientID), 10.0)
		if err != nil {
			log.Printf("Error inserting initial balance for client %d in initDatabase: %v", clientID, err)
			log.Printf("[initDatabase] Exiting with error.")
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Error committing DB transaction in initDatabase: %v", err)
		log.Printf("[initDatabase] Exiting with error.")
		return err
	}

	s.db = db
	log.Printf("[initDatabase] Exiting successfully.")
	return nil
}

func main() {
	log.Printf("[main] Entering.")
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run server.go <ServerID>")
		os.Exit(1)
	}
	serverID := os.Args[1]
	log.Printf("[main] serverID: %s", serverID)

	privateKeyPath := fmt.Sprintf("../server_keys/%s_private.pem", serverID)
	publicKeyPath := fmt.Sprintf("../server_keys/%s_public.pem", serverID)

	privateKey, err := loadPrivateKeyFromFile(privateKeyPath)
	if err != nil {
		log.Fatalf("Error Loading Private Key - %v", err)
	}

	publicKey, err := loadPublicKeyFromFile(publicKeyPath)
	if err != nil {
		log.Fatalf("Error Loading Public Key - %v", err)
	}
	f := 1

	clusterID := determineClusterID(serverID)
	if clusterID == -1 {
		log.Fatalf("Failed to determine cluster ID for server %s.", serverID)
	}

	peers, err := getPeers(clusterID, serverID)
	if err != nil {
		log.Fatalf("Error determining peers for server %s: %v", serverID, err)
	}

	// Define all server IDs in the system
	allServerIDs := []string{
		"S1", "S2", "S3", "S4",
		"S5", "S6", "S7", "S8",
		"S9", "S10", "S11", "S12",
	}

	// Load public keys of all servers except self
	otherServerKeys := make(map[string]*rsa.PublicKey)
	for _, id := range allServerIDs {
		if id == serverID {
			continue
		}
		publicKeyPath := fmt.Sprintf("../server_keys/%s_public.pem", id)
		pubKey, err := loadPublicKeyFromFile(publicKeyPath)
		if err != nil {
			log.Fatalf("Error Loading Public Key for Server %s - %v", id, err)
		}
		otherServerKeys[id] = pubKey
	}

	isLeader := serverID == getLeaderIDForCluster(clusterID)
	server := NewServer(serverID, clusterID, isLeader, f, privateKey, publicKey, otherServerKeys)

	server.Peers = peers

	err = server.initDatabase()
	if err != nil {
		log.Fatalf("Failed to initialize database for server %s: %v", server.ID, err)
	}

	server.WALPath = fmt.Sprintf("server_%s_wal.log", server.ID)

	port := fmt.Sprintf("%d", 1230+parseServerID(serverID))
	err = server.StartServer(port)
	if err != nil {
		log.Fatalf("Failed to Start Server %s on Port %s - %v", server.ID, port, err)
	}
	defer server.StopServer()

	log.Printf("Server %s started and listening on port %s.", server.ID, port)
	log.Printf("[main] Exiting main. Server running.")
	select {}
}
