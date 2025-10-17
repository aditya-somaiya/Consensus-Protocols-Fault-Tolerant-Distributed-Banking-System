package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
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
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Message Types
const (
	RequestMsg        = "REQUEST"
	PrePrepareMsg     = "PRE-PREPARE"
	PrepareMsg        = "PREPARE"
	CommitMsg         = "COMMIT"
	ReplyMsg          = "REPLY"
	CombinedCommitMsg = "COMBINED-COMMIT"
	ViewChangeMsg     = "VIEW-CHANGE"
	NewViewMsg        = "NEW-VIEW"
)

// ViewChangeMessage represents a VIEW-CHANGE message
type ViewChangeMessage struct {
	Type       string
	ViewNumber int
	NodeID     string
	P          []PrepareMessage // Set of PREPARE messages for which COLLECT-PREPARE was received
	Signature  string
}

// NewViewMessage represents a NEW-VIEW message
type NewViewMessage struct {
	Type       string
	ViewNumber int
	V          []ViewChangeMessage // Set of VIEW-CHANGE messages
	O          []PrePrepareMessage // Set of PRE-PREPARE messages
	Signature  string
}

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

// Server represents a server node in the network
type Server struct {
	ID             string
	ViewNumber     int
	SequenceNumber int64 // Changed to int64 for atomic operations
	IsLeader       bool
	PrivateKey     *rsa.PrivateKey
	PublicKey      *rsa.PublicKey

	// Logs
	ReceivedLog   sync.Map // map[string]Request
	PrePrepareLog sync.Map // map[int64]PrePrepareMessage
	PrepareLog    sync.Map // map[int64][]PrepareMessage
	CommitLog     sync.Map // map[int64][]CommitMessage
	ExecutedLog   sync.Map // map[int64]Transaction
	// Other servers' public keys
	OtherServerKeys map[string]*rsa.PublicKey
	// Clients' public keys
	ClientKeys map[string]*rsa.PublicKey
	// Networking
	listener  net.Listener
	rpcServer *rpc.Server
	f         int
	// Datastore to maintain client balances
	datastore      map[string]float64
	datastoreMutex sync.RWMutex
	// Per-client mutexes to ensure serial processing per client
	clientLocks map[string]*sync.Mutex

	// New fields for the leader to collect messages
	PrepareMessagesReceived sync.Map // map[int64][]PrepareMessage
	CommitMessagesReceived  sync.Map // map[int64][]CommitMessage

	// Tracks the last executed sequence number
	lastExecutedSequenceNumber int64

	// Queue to hold transactions that arrive out of order
	transactionQueue map[int64]PrePrepareMessage

	// Mutex to protect access to lastExecutedSequenceNumber and transactionQueue
	executionMutex sync.Mutex

	IsActive         bool
	IsByzantine      bool
	ActiveServers    map[string]bool
	ByzantineServers map[string]bool
	statusMutex      sync.Mutex

	viewChangeTimer      *time.Timer
	viewChangeTimerMutex sync.Mutex

	// Log of received VIEW-CHANGE messages
	ViewChangeLog sync.Map // map[int][]ViewChangeMessage

	// Current view number
	currentViewMutex       sync.Mutex
	newViewTimer           *time.Timer
	newViewTimerMutex      sync.Mutex
	pendingPrePrepareCount int
	pendingMutex           sync.Mutex

	NewViewMessages      []NewViewMessage
	newViewMessagesMutex sync.Mutex
}

type StatusUpdate struct {
	ActiveServers    []string
	ByzantineServers []string
}

// Update the NewServer function
func NewServer(id string, isLeader bool, f int, privateKey *rsa.PrivateKey, publicKey *rsa.PublicKey, otherServerKeys map[string]*rsa.PublicKey, clientKeys map[string]*rsa.PublicKey) *Server {
	server := &Server{
		ID:                         id,
		ViewNumber:                 1,
		SequenceNumber:             0,
		IsLeader:                   isLeader,
		PrivateKey:                 privateKey,
		PublicKey:                  &privateKey.PublicKey,
		OtherServerKeys:            otherServerKeys,
		ClientKeys:                 clientKeys,
		f:                          f,
		clientLocks:                make(map[string]*sync.Mutex),
		datastore:                  make(map[string]float64),
		lastExecutedSequenceNumber: 0, // Initialize to 0 or load from persisted state

		IsActive:    true,  // Servers start as active by default
		IsByzantine: false, // Servers start as non-Byzantine by default

		// Initialize mutexes
		currentViewMutex:     sync.Mutex{},
		viewChangeTimerMutex: sync.Mutex{},
	}

	// Initialize a mutex for each client
	for clientID := range clientKeys {
		server.clientLocks[clientID] = &sync.Mutex{}
		server.datastore[clientID] = 10.0
	}

	return server
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
}

// Server methods

// UpdateStatus updates the server's active and Byzantine status
func (s *Server) UpdateStatus(statusUpdate StatusUpdate, reply *bool) error {
	s.statusMutex.Lock()
	defer s.statusMutex.Unlock()

	// Update own status
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
	return nil
}

// Queues a transaction for later execution
func (s *Server) queueTransaction(seqNum int64, prePrepareMsg PrePrepareMessage) {
	if s.transactionQueue == nil {
		s.transactionQueue = make(map[int64]PrePrepareMessage)
	}
	s.transactionQueue[seqNum] = prePrepareMsg
}

// Processes queued transactions that can now be executed
func (s *Server) processQueuedTransactions() {
	expectedSeq := s.lastExecutedSequenceNumber + 1
	for {
		prePrepareMsg, exists := s.transactionQueue[expectedSeq]
		if !exists {
			break // No transaction with the expected sequence number
		}

		transaction := prePrepareMsg.Request.Transaction
		s.resetViewChangeTimer()

		// Execute the transaction
		s.datastoreMutex.Lock()
		if s.datastore[transaction.Sender] < transaction.Amount {
			s.datastoreMutex.Unlock()

			// Add to ExecutedLog as failed transaction
			s.ExecutedLog.Store(expectedSeq, transaction)

			s.pendingMutex.Lock()
			s.pendingPrePrepareCount--
			if s.pendingPrePrepareCount == 0 {
				s.stopViewChangeTimer()
			}
			s.pendingMutex.Unlock()

			// Send REPLY to client indicating failure
			replyMsg := Reply{
				Type:       ReplyMsg,
				ViewNumber: s.ViewNumber,
				Timestamp:  prePrepareMsg.Request.Timestamp,
				ClientID:   prePrepareMsg.Request.ClientID,
				NodeID:     s.ID,
				Result:     "fail",
			}
			replyMsg.Signature = signData(s.PrivateKey, replySignatureData(replyMsg))
			s.sendReplyToClient(replyMsg)

			// Remove from queue
			delete(s.transactionQueue, expectedSeq)

			s.lastExecutedSequenceNumber = expectedSeq
			expectedSeq++
			continue
		}

		s.datastore[transaction.Sender] -= transaction.Amount
		s.datastore[transaction.Receiver] += transaction.Amount
		s.datastoreMutex.Unlock()

		// Update last executed sequence number
		s.lastExecutedSequenceNumber = expectedSeq

		// Add to ExecutedLog
		s.ExecutedLog.Store(expectedSeq, transaction)

		s.pendingMutex.Lock()
		s.pendingPrePrepareCount--
		if s.pendingPrePrepareCount == 0 {
			s.stopViewChangeTimer()
		}
		s.pendingMutex.Unlock()

		// Remove from queue
		delete(s.transactionQueue, expectedSeq)

		// Send REPLY to client indicating success
		replyMsg := Reply{
			Type:       ReplyMsg,
			ViewNumber: s.ViewNumber,
			Timestamp:  prePrepareMsg.Request.Timestamp,
			ClientID:   prePrepareMsg.Request.ClientID,
			NodeID:     s.ID,
			Result:     "executed transaction",
		}
		replyMsg.Signature = signData(s.PrivateKey, replySignatureData(replyMsg))
		s.sendReplyToClient(replyMsg)

		// Prepare for the next expected sequence number
		expectedSeq++
	}
}

func (s *Server) ReceiveTransaction(request Request, reply *Reply) error {

	s.statusMutex.Lock()
	isActive := s.IsActive
	s.statusMutex.Unlock()

	if !isActive {
		return nil
	}

	// Validate client signature
	clientKey, ok := s.ClientKeys[request.ClientID]
	if !ok {
		return fmt.Errorf("unknown client %s", request.ClientID)
	}
	if !verifySignature(clientKey, requestSignatureData(request), request.Signature) {
		return fmt.Errorf("invalid client signature")
	}

	// Enqueue the transaction for processing with per-client serialization
	go s.processTransaction(request)
	return nil
}

func (s *Server) processTransaction(request Request) {
	// Acquire the mutex for the client to ensure serial processing
	clientMutex, exists := s.clientLocks[request.ClientID]
	if !exists {
		return
	}

	clientMutex.Lock()
	defer clientMutex.Unlock()

	// Now proceed with processing
	if s.IsLeader {
		// Atomically increment the sequence number
		sequenceNumber := atomic.AddInt64(&s.SequenceNumber, 1)

		digest := calculateDigest(request)

		prePrepareMsg := PrePrepareMessage{
			Type:           PrePrepareMsg,
			ViewNumber:     s.ViewNumber,
			SequenceNumber: sequenceNumber,
			Digest:         digest,
			Request:        request,
		}
		prePrepareMsg.Signature = signData(s.PrivateKey, prePrepareSignatureData(prePrepareMsg))

		// Add to PrePrepareLog
		s.PrePrepareLog.Store(sequenceNumber, prePrepareMsg)
		// Increment pendingPrePrepareCount and start the timer if it was zero
		s.pendingMutex.Lock()
		s.pendingPrePrepareCount++
		if s.pendingPrePrepareCount == 1 {
			s.startViewChangeTimer()
		}
		s.pendingMutex.Unlock()

		// Broadcast PRE-PREPARE without holding the mutex
		s.broadcastPrePrepare(prePrepareMsg)

		// Wait for PREPARE messages without holding the mutex
		prepareMessages := s.waitForPrepareMessages(sequenceNumber, digest)

		// Process PREPARE messages
		if len(prepareMessages) >= 2*s.f {
			// Broadcast collected PREPARE messages
			s.broadcastCollectPrepare(sequenceNumber, prepareMessages)
		}
	}
}

// Helper methods for leader

// broadcastPrePrepare asynchronously broadcasts the PRE-PREPARE message to other servers
func (s *Server) broadcastPrePrepare(prePrepareMsg PrePrepareMessage) {

	var wg sync.WaitGroup
	s.startViewChangeTimer()

	for serverID := range s.OtherServerKeys {
		if serverID == s.ID {
			continue // Skip sending to self
		}

		// Increment WaitGroup counter
		wg.Add(1)

		go func(serverID string) {
			defer wg.Done() // Ensure this is called at the end of the goroutine

			address := getServerAddress(serverID)

			client, err := rpc.Dial("tcp", address)
			if err != nil {
				return
			}
			defer client.Close()

			var ack bool
			client.Call("Server.HandlePrePrepare", prePrepareMsg, &ack)
		}(serverID)
	}

	// Wait for all goroutines to complete
	wg.Wait()
}

// Only for the leader
func (s *Server) waitForPrepareMessages(sequenceNumber int64, digest string) []PrepareMessage {
	prepareMessages := []PrepareMessage{}
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-timeout:
			return prepareMessages
		case <-ticker.C:
			if value, ok := s.PrepareMessagesReceived.Load(sequenceNumber); ok {
				messages := value.([]PrepareMessage)
				validMessages := []PrepareMessage{}
				for _, msg := range messages {
					if msg.Digest == digest {
						validMessages = append(validMessages, msg)
					}
				}
				if len(validMessages) >= 2*s.f {
					prepareMessages = validMessages
					return prepareMessages
				}
			}
		}
	}
}

func safeAppendToPrepareLog(prepareLog *sync.Map, sequenceNumber int64, prepareMsg PrepareMessage) {
	var messages []PrepareMessage
	value, _ := prepareLog.LoadOrStore(sequenceNumber, []PrepareMessage{})
	messages = value.([]PrepareMessage)
	messages = append(messages, prepareMsg)
	prepareLog.Store(sequenceNumber, messages)
}

func safeAppendToCommitLog(commitLog *sync.Map, sequenceNumber int64, commitMsg CommitMessage) {
	var messages []CommitMessage
	value, _ := commitLog.LoadOrStore(sequenceNumber, []CommitMessage{})
	messages = value.([]CommitMessage)
	messages = append(messages, commitMsg)
	commitLog.Store(sequenceNumber, messages)
}

// broadcastCollectPrepare broadcasts the collected PREPARE messages and handles COMMIT messages
func (s *Server) broadcastCollectPrepare(sequenceNumber int64, prepareMessages []PrepareMessage) {

	// Create a CollectPrepareMessage instance
	combinedMsg := CollectPrepareMessage{
		Type:            "COLLECT-PREPARE",
		ViewNumber:      s.ViewNumber,
		SequenceNumber:  sequenceNumber,
		PrepareMessages: prepareMessages,
	}

	// Sign the combined message
	data := fmt.Sprintf("%v", combinedMsg)
	signature := signData(s.PrivateKey, data)

	// Concurrently collect COMMIT messages from other servers
	commitMessages := []CommitMessage{}
	var mu sync.Mutex
	var wg sync.WaitGroup

	for serverID := range s.OtherServerKeys {
		wg.Add(1)
		go func(serverID string) {
			defer wg.Done()

			// If sending to self, directly call HandleCollectPrepare
			if serverID == s.ID {
				var commitMsg CommitMessage
				err := s.HandleCollectPrepare(CollectPrepareMessageWrapper{CombinedMsg: combinedMsg, Signature: signature}, &commitMsg)
				if err != nil {
				} else {
					mu.Lock()
					commitMessages = append(commitMessages, commitMsg)
					mu.Unlock()
				}
				return
			}

			address := getServerAddress(serverID)

			client, err := rpc.Dial("tcp", address)
			if err != nil {
				return
			}
			defer client.Close()

			// Send COLLECT-PREPARE message
			message := CollectPrepareMessageWrapper{
				CombinedMsg: combinedMsg,
				Signature:   signature,
			}

			// Variable to receive the COMMIT message
			var commitMsg CommitMessage
			err = client.Call("Server.HandleCollectPrepare", message, &commitMsg)
			if err != nil {
			} else {
				// Store received COMMIT message
				mu.Lock()
				commitMessages = append(commitMessages, commitMsg)
				mu.Unlock()
			}
		}(serverID)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Create leader's own COMMIT message and append it
	digest := prepareMessages[0].Digest // Assume all PREPARE messages have the same digest
	// Create leader's own COMMIT message and append it
	leaderCommitMsg := CommitMessage{
		Type:           CommitMsg,
		ViewNumber:     s.ViewNumber,
		SequenceNumber: sequenceNumber,
		Digest:         digest,
		NodeID:         s.ID,
	}
	leaderCommitMsg.Signature = signData(s.PrivateKey, commitSignatureData(leaderCommitMsg))

	// Add leader's own COMMIT message to CommitLog
	safeAppendToCommitLog(&s.CommitLog, sequenceNumber, leaderCommitMsg)

	// Append to commitMessages for CombinedCommitMessage
	mu.Lock()
	commitMessages = append(commitMessages, leaderCommitMsg)
	mu.Unlock()

	// Verify if there are at least 2*f + 1 valid COMMIT messages
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
		}
	}

	if len(validCommits) >= 2*s.f+1 {
		// Create CombinedCommitMessage with valid commits only
		combinedCommit := CombinedCommitMessage{
			Type:           "COMBINED-COMMIT",
			ViewNumber:     s.ViewNumber,
			SequenceNumber: sequenceNumber,
			CommitMessages: validCommits,
		}

		// Sign the combined COMMIT message
		combinedCommitData := combinedCommitSignatureData(combinedCommit)
		combinedCommit.Signature = signData(s.PrivateKey, combinedCommitData)

		// Broadcast CombinedCommitMessage to all other servers asynchronously
		go s.broadcastCombinedCommit(combinedCommit)
	}
}

func (s *Server) startViewChangeTimer() {
	s.viewChangeTimerMutex.Lock()
	defer s.viewChangeTimerMutex.Unlock()

	if s.viewChangeTimer != nil {
		s.viewChangeTimer.Stop()
	}

	s.viewChangeTimer = time.AfterFunc(1800*time.Millisecond, func() {
		s.pendingMutex.Lock()
		pending := s.pendingPrePrepareCount
		s.pendingMutex.Unlock()
		if pending > 0 {
			s.initiateViewChange()
		} else {
			// Do not initiate view change, pending requests have been processed
			s.viewChangeTimerMutex.Lock()
			s.viewChangeTimer = nil
			s.viewChangeTimerMutex.Unlock()
		}
	})
}

func (s *Server) initiateViewChange() {

	s.statusMutex.Lock()
	isActive := s.IsActive
	s.statusMutex.Unlock()

	if !isActive {
		log.Printf("[%s] Skipping view change initiation because server is inactive", s.ID)
		return
	}

	s.pendingMutex.Lock()
	pending := s.pendingPrePrepareCount
	s.pendingMutex.Unlock()

	if pending == 0 {
		log.Printf("[%s] No pending PRE-PREPARE messages, not initiating view change", s.ID)
		return
	}

	s.currentViewMutex.Lock()
	s.ViewNumber += 1 // Move to the next view
	newViewNumber := s.ViewNumber
	s.updateLeaderStatus() // Update leader status based on new view
	s.currentViewMutex.Unlock()

	log.Printf("[%s] Initiating view change to view %d", s.ID, newViewNumber)

	// Collect P set
	P := s.collectPreparedMessages()
	log.Printf("[%s] Collected P set with %d PREPARE messages for view change", s.ID, len(P))

	// Create VIEW-CHANGE message
	viewChangeMsg := ViewChangeMessage{
		Type:       ViewChangeMsg,
		ViewNumber: newViewNumber,
		NodeID:     s.ID,
		P:          P,
	}
	viewChangeMsg.Signature = signData(s.PrivateKey, viewChangeSignatureData(viewChangeMsg))

	log.Printf("[%s] Created VIEW-CHANGE message for view %d", s.ID, newViewNumber)

	// Add to ViewChangeLog
	s.safeAppendToViewChangeLog(newViewNumber, viewChangeMsg)

	// Broadcast VIEW-CHANGE message
	s.broadcastViewChange(viewChangeMsg)
	s.startNewViewTimer()
}

// Collects the set P of prepared messages
func (s *Server) collectPreparedMessages() []PrepareMessage {
	var P []PrepareMessage
	s.PrepareLog.Range(func(key, value interface{}) bool {
		// seqNum := key.(int64)
		prepareMsgs := value.([]PrepareMessage)
		if len(prepareMsgs) >= 2*s.f {
			// For the purpose of P, we need to collect the last PrepareMessage for each sequence number
			latestPrepareMsg := prepareMsgs[len(prepareMsgs)-1]
			P = append(P, latestPrepareMsg)
		}
		return true
	})
	return P
}

// Safe append to ViewChangeLog
func (s *Server) safeAppendToViewChangeLog(viewNumber int, viewChangeMsg ViewChangeMessage) {
	var messages []ViewChangeMessage
	value, _ := s.ViewChangeLog.LoadOrStore(viewNumber, []ViewChangeMessage{})
	messages = value.([]ViewChangeMessage)

	// Check if a message from this NodeID already exists
	exists := false
	for _, msg := range messages {
		if msg.NodeID == viewChangeMsg.NodeID {
			exists = true
			break
		}
	}
	if !exists {
		messages = append(messages, viewChangeMsg)
		s.ViewChangeLog.Store(viewNumber, messages)
	}
}

func (s *Server) broadcastViewChange(viewChangeMsg ViewChangeMessage) {
	log.Printf("[%s] Broadcasting VIEW-CHANGE message for view %d to all other servers", s.ID, viewChangeMsg.ViewNumber)

	var wg sync.WaitGroup

	for serverID := range s.OtherServerKeys {
		wg.Add(1)
		go func(serverID string) {
			defer wg.Done()
			address := getServerAddress(serverID)
			client, err := rpc.Dial("tcp", address)
			if err != nil {
				log.Printf("[%s] Failed to connect to server %s for VIEW-CHANGE", s.ID, serverID)
				return
			}
			defer client.Close()
			var ack bool
			err = client.Call("Server.HandleViewChange", viewChangeMsg, &ack)
			if err != nil {
				log.Printf("[%s] Error sending VIEW-CHANGE to server %s: %v", s.ID, serverID, err)
			} else {
				log.Printf("[%s] Successfully sent VIEW-CHANGE to server %s", s.ID, serverID)
			}
		}(serverID)
	}

	wg.Wait()
	log.Printf("[%s] Completed broadcasting VIEW-CHANGE for view %d", s.ID, viewChangeMsg.ViewNumber)
}

func (s *Server) HandleViewChange(viewChangeMsg ViewChangeMessage, ack *bool) error {
	s.statusMutex.Lock()
	isActive := s.IsActive
	s.statusMutex.Unlock()

	if !isActive {
		log.Printf("[%s] Received VIEW-CHANGE message but server is inactive", s.ID)
		return nil
	}

	// Validate signature
	nodeKey, exists := s.OtherServerKeys[viewChangeMsg.NodeID]
	if !exists && viewChangeMsg.NodeID != s.ID {
		log.Printf("[%s] Unknown node %s in VIEW-CHANGE message", s.ID, viewChangeMsg.NodeID)
		return fmt.Errorf("unknown node %s", viewChangeMsg.NodeID)
	}
	var key *rsa.PublicKey
	if viewChangeMsg.NodeID == s.ID {
		key = s.PublicKey
	} else {
		key = nodeKey
	}
	if !verifySignature(key, viewChangeSignatureData(viewChangeMsg), viewChangeMsg.Signature) {
		log.Printf("[%s] Invalid signature in VIEW-CHANGE message from node %s", s.ID, viewChangeMsg.NodeID)
		return fmt.Errorf("invalid signature from node %s", viewChangeMsg.NodeID)
	}

	log.Printf("[%s] Received valid VIEW-CHANGE message from node %s for view %d with P set size %d",
		s.ID, viewChangeMsg.NodeID, viewChangeMsg.ViewNumber, len(viewChangeMsg.P))

	// Add to ViewChangeLog
	s.safeAppendToViewChangeLog(viewChangeMsg.ViewNumber, viewChangeMsg)

	// If this server is the new leader and has received 2f valid VIEW-CHANGE messages, send NEW-VIEW
	s.currentViewMutex.Lock()
	isNewLeader := s.isLeaderForView(viewChangeMsg.ViewNumber)
	s.currentViewMutex.Unlock()

	if isNewLeader {
		log.Printf("[%s] Server is the new leader for view %d. Attempting to send NEW-VIEW.", s.ID, viewChangeMsg.ViewNumber)
		s.trySendNewView(viewChangeMsg.ViewNumber)
	}

	*ack = true
	return nil
}

func (s *Server) isLeaderForView(viewNumber int) bool {
	leaderID := s.getLeaderIDForView(viewNumber)
	return s.ID == leaderID
}

// trySendNewView attempts to send a NEW-VIEW message if conditions are met
func (s *Server) trySendNewView(viewNumber int) {
	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive || isByzantine {
		log.Printf("[%s] Skipping NEW-VIEW initiation because server is inactive or Byzantine", s.ID)
		return
	}

	value, ok := s.ViewChangeLog.Load(viewNumber)
	if !ok {
		log.Printf("[%s] No VIEW-CHANGE messages found for view %d. Cannot send NEW-VIEW.", s.ID, viewNumber)
		return
	}
	viewChangeMessages := value.([]ViewChangeMessage)
	if len(viewChangeMessages) < 2*s.f {
		log.Printf("[%s] Insufficient VIEW-CHANGE messages (%d) for view %d. Required: %d.", s.ID, len(viewChangeMessages), viewNumber, 2*s.f)
		return
	}

	count := 0
	for _, vcm := range viewChangeMessages {
		if vcm.NodeID != s.ID {
			count++
		}
	}

	if count < 2*s.f {
		log.Printf("[%s] Insufficient VIEW-CHANGE messages from other servers (%d) for view %d. Required: %d.", s.ID, count, viewNumber, 2*s.f)
		return
	}

	// Collect O set
	O := s.calculateOSet(viewChangeMessages)
	log.Printf("[%s] Calculated O set with %d PRE-PREPARE messages for NEW-VIEW", s.ID, len(O))

	// Create NEW-VIEW message
	newViewMsg := NewViewMessage{
		Type:       NewViewMsg,
		ViewNumber: viewNumber,
		V:          viewChangeMessages,
		O:          O,
	}
	newViewMsg.Signature = signData(s.PrivateKey, newViewSignatureData(newViewMsg))

	log.Printf("[%s] Created NEW-VIEW message for view %d", s.ID, viewNumber)

	s.newViewMessagesMutex.Lock()
	alreadyReceived := false
	for _, nvMsg := range s.NewViewMessages {
		if nvMsg.ViewNumber == newViewMsg.ViewNumber {
			alreadyReceived = true
			break
		}
	}
	if !alreadyReceived {
		s.NewViewMessages = append(s.NewViewMessages, newViewMsg)
	}
	s.newViewMessagesMutex.Unlock()
	// Broadcast NEW-VIEW message
	s.broadcastNewView(newViewMsg)
}

func (s *Server) calculateOSet(viewChangeMessages []ViewChangeMessage) []PrePrepareMessage {
	// Find the maximum sequence number maxS among all P sets
	maxS := int64(0)
	for _, vcm := range viewChangeMessages {
		for _, pm := range vcm.P {
			if pm.SequenceNumber > maxS {
				maxS = pm.SequenceNumber
			}
		}
	}

	if maxS == 0 {
		// All P sets are empty, return empty O
		return []PrePrepareMessage{}
	}

	// For sequence numbers from 1 to maxS, craft new PRE-PREPARE messages
	O := []PrePrepareMessage{}
	for seqNum := int64(1); seqNum <= maxS; seqNum++ {
		// Collect the most recent PREPARE message for seqNum
		var latestPrepareMsg PrepareMessage
		for _, vcm := range viewChangeMessages {
			for _, pm := range vcm.P {
				if pm.SequenceNumber == seqNum && pm.ViewNumber > latestPrepareMsg.ViewNumber {
					latestPrepareMsg = pm
				}
			}
		}
		if latestPrepareMsg.NodeID != "" {
			// Craft a new PRE-PREPARE message with new view number
			prePrepareMsg := PrePrepareMessage{
				Type:           PrePrepareMsg,
				ViewNumber:     s.ViewNumber,
				SequenceNumber: seqNum,
				Digest:         latestPrepareMsg.Digest,
				Request:        s.getRequestByDigest(latestPrepareMsg.Digest),
			}
			prePrepareMsg.Signature = signData(s.PrivateKey, prePrepareSignatureData(prePrepareMsg))
			O = append(O, prePrepareMsg)
		}
	}
	return O
}

// Retrieve the original Request by its digest
func (s *Server) getRequestByDigest(digest string) Request {
	var request Request
	s.ReceivedLog.Range(func(key, value interface{}) bool {
		req := value.(Request)
		if calculateDigest(req) == digest {
			request = req
			return false
		}
		return true
	})
	return request
}

func (s *Server) broadcastNewView(newViewMsg NewViewMessage) {
	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive || isByzantine {
		log.Printf("[%s] Skipping broadcast of NEW-VIEW due to status: Active=%v, Byzantine=%v", s.ID, isActive, isByzantine)
		return
	}

	log.Printf("[%s] Broadcasting NEW-VIEW message for view %d to all other servers", s.ID, newViewMsg.ViewNumber)

	var wg sync.WaitGroup

	for serverID := range s.OtherServerKeys {
		wg.Add(1)
		go func(serverID string) {
			defer wg.Done()

			address := getServerAddress(serverID)
			client, err := rpc.Dial("tcp", address)
			if err != nil {
				log.Printf("[%s] Failed to connect to server %s for NEW-VIEW", s.ID, serverID)
				return
			}
			defer client.Close()

			var ack bool
			err = client.Call("Server.HandleNewView", newViewMsg, &ack)
			if err != nil {
				log.Printf("[%s] Error sending NEW-VIEW to server %s: %v", s.ID, serverID, err)
			} else {
				log.Printf("[%s] Successfully sent NEW-VIEW to server %s", s.ID, serverID)
			}
		}(serverID)
	}

	wg.Wait()
	log.Printf("[%s] Completed broadcasting NEW-VIEW for view %d", s.ID, newViewMsg.ViewNumber)
}

func (s *Server) discardState() {
	s.PrePrepareLog = sync.Map{}
	s.PrepareLog = sync.Map{}
	s.CommitLog = sync.Map{}
	s.ExecutedLog = sync.Map{}
	s.ViewChangeLog = sync.Map{}
	s.SequenceNumber = 0
	s.lastExecutedSequenceNumber = 0
}

func (s *Server) stopViewChangeTimer() {
	s.viewChangeTimerMutex.Lock()
	defer s.viewChangeTimerMutex.Unlock()
	if s.viewChangeTimer != nil {
		s.viewChangeTimer.Stop()
		s.viewChangeTimer = nil
	}
}

func (s *Server) getLeaderID() string {
	s.currentViewMutex.Lock()
	defer s.currentViewMutex.Unlock()
	return s.getLeaderIDForView(s.ViewNumber)
}

func (s *Server) getLeaderAddress() string {
	s.currentViewMutex.Lock()
	defer s.currentViewMutex.Unlock()
	return s.getLeaderAddressForView(s.ViewNumber)
}

func (s *Server) getLeaderIDForView(viewNumber int) string {
	totalServers := 3*s.f + 1
	leaderIndex := (viewNumber - 1) % totalServers
	leaderIndex += 1 // Adjust to 1-based indexing
	return fmt.Sprintf("S%d", leaderIndex)
}

func (s *Server) getLeaderAddressForView(viewNumber int) string {
	leaderID := s.getLeaderIDForView(viewNumber)
	return getServerAddress(leaderID)
}

func (s *Server) HandleNewView(newViewMsg NewViewMessage, ack *bool) error {
	s.statusMutex.Lock()
	isActive := s.IsActive
	s.statusMutex.Unlock()

	if !isActive {
		log.Printf("[%s] Received NEW-VIEW message but server is inactive", s.ID)
		return nil
	}

	// Validate leader signature
	leaderID := s.getLeaderIDForView(newViewMsg.ViewNumber)
	leaderKey, exists := s.OtherServerKeys[leaderID]
	if !exists && leaderID != s.ID {
		log.Printf("[%s] Leader %s public key not found for NEW-VIEW", s.ID, leaderID)
		return fmt.Errorf("leader public key not found")
	}

	var key *rsa.PublicKey
	if leaderID == s.ID {
		key = s.PublicKey
	} else {
		key = leaderKey
	}

	if !verifySignature(key, newViewSignatureData(newViewMsg), newViewMsg.Signature) {
		log.Printf("[%s] Invalid leader signature on NEW-VIEW message for view %d", s.ID, newViewMsg.ViewNumber)
		return fmt.Errorf("invalid leader signature on NEW-VIEW")
	}

	log.Printf("[%s] Received valid NEW-VIEW message for view %d from leader %s", s.ID, newViewMsg.ViewNumber, leaderID)
	s.newViewMessagesMutex.Lock()
	alreadyReceived := false
	for _, nvMsg := range s.NewViewMessages {
		if nvMsg.ViewNumber == newViewMsg.ViewNumber {
			alreadyReceived = true
			break
		}
	}
	if !alreadyReceived {
		s.NewViewMessages = append(s.NewViewMessages, newViewMsg)
	}
	s.newViewMessagesMutex.Unlock()

	// Discard current state
	s.discardState()
	log.Printf("[%s] Discarded current state for new view %d", s.ID, newViewMsg.ViewNumber)

	// Reset pendingPrePrepareCount after handling NEW-VIEW
	s.pendingMutex.Lock()
	s.pendingPrePrepareCount = 0
	s.pendingMutex.Unlock()

	// Update view number and leader status
	s.currentViewMutex.Lock()
	s.ViewNumber = newViewMsg.ViewNumber
	s.updateLeaderStatus() // This sets IsLeader based on ViewNumber and ID
	s.currentViewMutex.Unlock()

	log.Printf("[%s] Updated to view %d and updated leader status", s.ID, newViewMsg.ViewNumber)

	// Stop timers
	s.stopViewChangeTimer()
	log.Printf("[%s] Stopped view change timer for new view %d", s.ID, newViewMsg.ViewNumber)

	// Accept PRE-PREPARE messages from O
	for _, prePrepareMsg := range newViewMsg.O {
		var ackInner bool
		err := s.HandlePrePrepare(prePrepareMsg, &ackInner)
		if err != nil {
			log.Printf("[%s] Error handling PRE-PREPARE from O set in NEW-VIEW: %v", s.ID, err)
		} else {
			log.Printf("[%s] Handled PRE-PREPARE from O set in NEW-VIEW for sequence number %d", s.ID, prePrepareMsg.SequenceNumber)
		}
	}

	*ack = true
	return nil
}

// Implement the PrintView RPC method
func (s *Server) PrintView(args struct{}, reply *[]string) error {
	s.newViewMessagesMutex.Lock()
	defer s.newViewMessagesMutex.Unlock()

	var messages []string
	for _, nvMsg := range s.NewViewMessages {
		// Format the NEW-VIEW message and its contents
		viewChangeMessages := make([]string, len(nvMsg.V))
		for i, vcm := range nvMsg.V {
			viewChangeMessages[i] = fmt.Sprintf("VIEW-CHANGE from %s for view %d", vcm.NodeID, vcm.ViewNumber)
		}

		prePrepareMessages := make([]string, len(nvMsg.O))
		for i, ppm := range nvMsg.O {
			prePrepareMessages[i] = fmt.Sprintf("PRE-PREPARE Seq:%d View:%d Digest:%s", ppm.SequenceNumber, ppm.ViewNumber, ppm.Digest)
		}

		message := fmt.Sprintf(
			"NEW-VIEW ViewNumber:%d\nVIEW-CHANGE Messages:\n%s\nPRE-PREPARE Messages:\n%s\n",
			nvMsg.ViewNumber,
			strings.Join(viewChangeMessages, "\n"),
			strings.Join(prePrepareMessages, "\n"),
		)
		messages = append(messages, message)
	}
	*reply = messages
	return nil
}

// Resets the view change timer
func (s *Server) resetViewChangeTimer() {
	s.viewChangeTimerMutex.Lock()
	defer s.viewChangeTimerMutex.Unlock()

	if s.viewChangeTimer != nil {
		s.viewChangeTimer.Reset(2200 * time.Millisecond)
	} else {
		s.startViewChangeTimer()
	}
}

// broadcastCombinedCommit broadcasts the CombinedCommitMessage to all other servers including itself
func (s *Server) broadcastCombinedCommit(combinedCommit CombinedCommitMessage) {

	// Use the helper function for consistency
	combinedCommitData := combinedCommitSignatureData(combinedCommit)
	combinedCommit.Signature = signData(s.PrivateKey, combinedCommitData)

	var wg sync.WaitGroup

	// Include self in the broadcast
	serverIDs := make([]string, 0, len(s.OtherServerKeys)+1)
	for serverID := range s.OtherServerKeys {
		serverIDs = append(serverIDs, serverID)
	}
	serverIDs = append(serverIDs, s.ID) // Include self

	for _, serverID := range serverIDs {
		wg.Add(1)
		go func(serverID string) {
			defer wg.Done()

			// If sending to self, directly call HandleCombinedCommit
			if serverID == s.ID {
				var ack bool
				err := s.HandleCombinedCommit(combinedCommit, &ack)
				if err != nil {
				}
				return
			}

			address := getServerAddress(serverID)

			client, err := rpc.Dial("tcp", address)
			if err != nil {
				return
			}
			defer client.Close()

			var ack bool
			client.Call("Server.HandleCombinedCommit", combinedCommit, &ack)
		}(serverID)
	}

	wg.Wait()
}

// HandleCombinedCommit handles CombinedCommitMessage from the leader
func (s *Server) HandleCombinedCommit(combinedCommit CombinedCommitMessage, ack *bool) error {
	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive || isByzantine {
		return nil
	}

	// Step 1: Validate the leader's signature on the CombinedCommitMessage
	leaderID := s.getLeaderID()
	leaderKey, exists := s.OtherServerKeys[leaderID]
	if !exists && leaderID != s.ID {
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
		return fmt.Errorf("invalid leader signature on COMBINED-COMMIT")
	}

	// Step 2: Validate each CommitMessage in CombinedCommitMessage
	prePrepareValue, ok := s.PrePrepareLog.Load(combinedCommit.SequenceNumber)
	if !ok {
		return fmt.Errorf("no matching PRE-PREPARE message found")
	}
	prePrepareMsg := prePrepareValue.(PrePrepareMessage)
	expectedDigest := prePrepareMsg.Digest

	validCommits := 0
	for _, commitMsg := range combinedCommit.CommitMessages {
		// Check if the digest matches that of the PrePrepareMessage
		if commitMsg.Digest != expectedDigest {
			continue
		}

		// Validate signature of each CommitMessage
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
		}
	}

	if validCommits >= 2*s.f+1 {
		// Proceed to execute the transaction ensuring sequential execution
		s.executionMutex.Lock()
		defer s.executionMutex.Unlock()

		expectedSeq := s.lastExecutedSequenceNumber + 1
		if combinedCommit.SequenceNumber == expectedSeq {
			// Execute the transaction
			transaction := prePrepareMsg.Request.Transaction
			s.resetViewChangeTimer()

			s.datastoreMutex.Lock()
			senderBalance := s.datastore[transaction.Sender]
			if senderBalance < transaction.Amount {
				// Insufficient funds
				s.datastoreMutex.Unlock()

				// Add to ExecutedLog as failed transaction
				s.ExecutedLog.Store(combinedCommit.SequenceNumber, Transaction{
					SetNumber: transaction.SetNumber,
					Sender:    transaction.Sender,
					Receiver:  transaction.Receiver,
					Amount:    transaction.Amount,
					Timestamp: transaction.Timestamp,
				})

				s.pendingMutex.Lock()
				s.pendingPrePrepareCount--
				if s.pendingPrePrepareCount == 0 {
					s.stopViewChangeTimer()
				}
				s.pendingMutex.Unlock()

				// Update last executed sequence number
				s.lastExecutedSequenceNumber = combinedCommit.SequenceNumber

				// Optionally, process any queued transactions that can now be executed
				s.processQueuedTransactions()

				// Send REPLY to client indicating failure
				replyMsg := Reply{
					Type:       ReplyMsg,
					ViewNumber: s.ViewNumber,
					Timestamp:  prePrepareMsg.Request.Timestamp,
					ClientID:   prePrepareMsg.Request.ClientID,
					NodeID:     s.ID,
					Result:     "fail",
				}
				replyMsg.Signature = signData(s.PrivateKey, replySignatureData(replyMsg))
				s.sendReplyToClient(replyMsg)

				*ack = true
				return nil
			}

			// Sufficient funds; proceed with balance updates
			s.datastore[transaction.Sender] -= transaction.Amount
			s.datastore[transaction.Receiver] += transaction.Amount
			s.datastoreMutex.Unlock()

			// Update last executed sequence number
			s.lastExecutedSequenceNumber = combinedCommit.SequenceNumber

			// Add to ExecutedLog
			s.ExecutedLog.Store(combinedCommit.SequenceNumber, transaction)

			s.pendingMutex.Lock()
			s.pendingPrePrepareCount--
			if s.pendingPrePrepareCount == 0 {
				s.stopViewChangeTimer()
			}
			s.pendingMutex.Unlock()

			// Optionally, process any queued transactions that can now be executed
			s.processQueuedTransactions()

			// Send REPLY to client indicating success
			replyMsg := Reply{
				Type:       ReplyMsg,
				ViewNumber: s.ViewNumber,
				Timestamp:  prePrepareMsg.Request.Timestamp,
				ClientID:   prePrepareMsg.Request.ClientID,
				NodeID:     s.ID,
				Result:     "executed transaction",
			}
			replyMsg.Signature = signData(s.PrivateKey, replySignatureData(replyMsg))
			s.sendReplyToClient(replyMsg)
		} else if combinedCommit.SequenceNumber > expectedSeq {
			// Queue the transaction for later execution
			s.queueTransaction(combinedCommit.SequenceNumber, prePrepareMsg)
		}

		*ack = true
		return nil
	}

	*ack = true
	return nil
}

func (s *Server) HandlePrePrepare(prePrepareMsg PrePrepareMessage, ack *bool) error {

	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive {
		return nil
	}

	// Validate leader signature
	leaderID := s.getLeaderID()
	leaderKey, exists := s.OtherServerKeys[leaderID]
	if !exists && leaderID != s.ID {
		return fmt.Errorf("leader public key not found")
	}
	var key *rsa.PublicKey
	if leaderID == s.ID {
		key = s.PublicKey
	} else {
		key = leaderKey
	}
	if !verifySignature(key, prePrepareSignatureData(prePrepareMsg), prePrepareMsg.Signature) {
		return fmt.Errorf("invalid leader signature")
	}

	// Validate client signature
	clientKey, ok := s.ClientKeys[prePrepareMsg.Request.ClientID]
	if !ok {
		return fmt.Errorf("unknown client %s", prePrepareMsg.Request.ClientID)
	}
	if !verifySignature(clientKey, requestSignatureData(prePrepareMsg.Request), prePrepareMsg.Request.Signature) {
		return fmt.Errorf("invalid client signature")
	}

	// Add to PrePrepareLog
	s.PrePrepareLog.Store(prePrepareMsg.SequenceNumber, prePrepareMsg)

	// Increment pendingPrePrepareCount and start the timer if it was zero
	s.pendingMutex.Lock()
	s.pendingPrePrepareCount++
	if s.pendingPrePrepareCount == 1 {
		s.startViewChangeTimer()
	}
	s.pendingMutex.Unlock()

	if isByzantine {
		// Optionally, Byzantine servers can perform malicious actions here
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

	// Add own PREPARE message to PrepareLog
	safeAppendToPrepareLog(&s.PrepareLog, prepareMsg.SequenceNumber, prepareMsg)
	s.startViewChangeTimer()

	// Sending PREPARE message to Leader
	leaderAddress := s.getLeaderAddress()
	client, err := rpc.Dial("tcp", leaderAddress)
	if err != nil {
		return fmt.Errorf("could not connect to leader: %v", err)
	}
	defer client.Close()
	var leaderAck bool
	err = client.Call("Server.HandlePrepare", prepareMsg, &leaderAck)
	if err != nil {
		return fmt.Errorf("error sending PREPARE to leader: %v", err)
	}
	*ack = true
	return nil
}

// HandlePrepare handles PREPARE messages from other servers
func (s *Server) HandlePrepare(prepareMsg PrepareMessage, ack *bool) error {
	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive || isByzantine {
		return nil
	}

	// Validate node signature
	nodeKey, exists := s.OtherServerKeys[prepareMsg.NodeID]
	if !exists && prepareMsg.NodeID != s.ID {
		return fmt.Errorf("unknown node %s", prepareMsg.NodeID)
	}
	var key *rsa.PublicKey
	if prepareMsg.NodeID == s.ID {
		key = s.PublicKey
	} else {
		key = nodeKey
	}
	if !verifySignature(key, prepareSignatureData(prepareMsg), prepareMsg.Signature) {
		return fmt.Errorf("invalid signature from node %s", prepareMsg.NodeID)
	}

	// Add to PrepareLog
	if s.IsLeader {
		safeAppendToPrepareLog(&s.PrepareMessagesReceived, prepareMsg.SequenceNumber, prepareMsg)
	}

	*ack = true
	return nil
}

// HandleCollectPrepare handles collected PREPARE messages from the leader
func (s *Server) HandleCollectPrepare(message CollectPrepareMessageWrapper, commitMsg *CommitMessage) error {
	s.statusMutex.Lock()
	isActive := s.IsActive
	isByzantine := s.IsByzantine
	s.statusMutex.Unlock()

	if !isActive || isByzantine {
		return nil
	}

	// Validate leader signature
	leaderID := s.getLeaderID()
	leaderKey, exists := s.OtherServerKeys[leaderID]
	if !exists && leaderID != s.ID {
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
		return fmt.Errorf("invalid leader signature on COLLECT-PREPARE")
	}

	// Extract combined message
	sequenceNumber := message.CombinedMsg.SequenceNumber
	prepareMessages := message.CombinedMsg.PrepareMessages

	// Validate prepare messages
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
		}
	}

	if validCount >= 2*s.f {

		// Create COMMIT message
		*commitMsg = CommitMessage{
			Type:           CommitMsg,
			ViewNumber:     s.ViewNumber,
			SequenceNumber: sequenceNumber,
			Digest:         message.CombinedMsg.PrepareMessages[0].Digest, // Assuming all digests are same
			NodeID:         s.ID,
		}
		commitMsg.Signature = signData(s.PrivateKey, commitSignatureData(*commitMsg))

		// Add own COMMIT message to CommitLog
		safeAppendToCommitLog(&s.CommitLog, sequenceNumber, *commitMsg)
	} else {
		return fmt.Errorf("insufficient valid PREPARE messages")
	}
	return nil
}

// sendReplyToClient sends a reply to the client via RPC
func (s *Server) sendReplyToClient(reply Reply) error {
	client, err := rpc.Dial("tcp", clientAddress)
	if err != nil {
		return err
	}
	defer client.Close()

	var ack bool
	err = client.Call("ClientRPC.ReceiveReply", reply, &ack)
	if err != nil {
		return err
	}
	return nil
}

// PrintLog prints the server's processed messages logs
func (s *Server) PrintLog(args struct{}, reply *string) error {
	var logOutput string

	// Iterate over PrePrepareLog
	logOutput += "PrePrepareLog:\n"
	s.PrePrepareLog.Range(func(key, value interface{}) bool {
		seqNum := key.(int64)
		msg := value.(PrePrepareMessage)
		logOutput += fmt.Sprintf("Seq:%d Client:%s\n", seqNum, msg.Request.ClientID)
		return true
	})

	// Iterate over PrepareLog
	logOutput += "PrepareLog:\n"
	s.PrepareLog.Range(func(key, value interface{}) bool {
		seqNum := key.(int64)
		msgs := value.([]PrepareMessage)
		for _, msg := range msgs {
			logOutput += fmt.Sprintf("Seq:%d Client:%s PREPARE from %s\n", seqNum, "N/A", msg.NodeID)
		}
		return true
	})

	// Iterate over CommitLog
	logOutput += "CommitLog:\n"
	s.CommitLog.Range(func(key, value interface{}) bool {
		seqNum := key.(int64)
		msgs := value.([]CommitMessage)
		for _, msg := range msgs {
			logOutput += fmt.Sprintf("Seq:%d Client:%s COMMIT from %s\n", seqNum, "N/A", msg.NodeID)
		}
		return true
	})

	// Iterate over ExecutedLog
	logOutput += "ExecutedLog:\n"
	s.ExecutedLog.Range(func(key, value interface{}) bool {
		seqNum := key.(int64)
		tx := value.(Transaction)
		logOutput += fmt.Sprintf("Seq:%d Client:%s Executed\n", seqNum, tx.Sender)
		return true
	})

	*reply = logOutput
	return nil
}

// startNewViewTimer starts a 900ms timer to wait for a NewView message
func (s *Server) startNewViewTimer() {
	s.newViewTimerMutex.Lock()
	defer s.newViewTimerMutex.Unlock()

	if s.newViewTimer != nil {
		s.newViewTimer.Stop()
	}

	s.newViewTimer = time.AfterFunc(2200*time.Millisecond, func() {
		log.Printf("[%s] NewView timer expired, initiating another view change", s.ID)
		s.initiateViewChange()
	})
}

// stopNewViewTimer stops the NewView timer when a NewView is received
func (s *Server) stopNewViewTimer() {
	s.newViewTimerMutex.Lock()
	defer s.newViewTimerMutex.Unlock()
	if s.newViewTimer != nil {
		s.newViewTimer.Stop()
		s.newViewTimer = nil
	}
}

func (s *Server) PrintDB(args struct{}, reply *string) error {
	var dbOutput string

	// Collect all client IDs
	s.datastoreMutex.RLock()
	clientIDs := make([]string, 0, len(s.datastore))
	for clientID := range s.datastore {
		clientIDs = append(clientIDs, clientID)
	}
	s.datastoreMutex.RUnlock()

	// Sort the client IDs in increasing order
	sort.Strings(clientIDs)

	// Iterate through the sorted client IDs to build the output
	s.datastoreMutex.RLock()
	for _, clientID := range clientIDs {
		balance := s.datastore[clientID]
		dbOutput += fmt.Sprintf("Client %s: %.2f\n", clientID, balance)
	}
	s.datastoreMutex.RUnlock()

	*reply = dbOutput
	return nil
}

func (s *Server) PrintStatus(sequenceNumber int64, reply *string) error {
	// Check if the sequence number exists in PrePrepareLog
	_, prePrepareExists := s.PrePrepareLog.Load(sequenceNumber)
	if !prePrepareExists {
		*reply = "X"
		return nil
	}

	if s.IsLeader {
		// Leader logic: Check from highest to lowest status
		if _, executed := s.ExecutedLog.Load(sequenceNumber); executed {
			*reply = "E"
			return nil
		}

		if commitMsgs, ok := s.CommitLog.Load(sequenceNumber); ok && len(commitMsgs.([]CommitMessage)) >= 2*s.f+1 {
			*reply = "C"
			return nil
		}

		if prepareMsgs, ok := s.PrepareLog.Load(sequenceNumber); ok && len(prepareMsgs.([]PrepareMessage)) >= 2*s.f {
			*reply = "P"
			return nil
		}

		// If only PrePrepare exists
		*reply = "PP"
		return nil
	} else {
		// Non-leader logic: Check from highest to lowest status
		if _, executed := s.ExecutedLog.Load(sequenceNumber); executed {
			*reply = "E"
			return nil
		}

		if commitMsgs, ok := s.CommitLog.Load(sequenceNumber); ok && len(commitMsgs.([]CommitMessage)) >= 2*s.f+1 {
			*reply = "C"
			return nil
		}

		if prepareMsgs, ok := s.PrepareLog.Load(sequenceNumber); ok && len(prepareMsgs.([]PrepareMessage)) >= 2*s.f {
			*reply = "P"
			return nil
		}

		// If only PrePrepare exists
		*reply = "PP"
		return nil
	}
}

func (s *Server) Reset(args struct{}, reply *bool) error {
	// 1. Clear PrePrepareLog
	s.PrePrepareLog = sync.Map{}

	// 2. Clear PrepareLog
	s.PrepareLog = sync.Map{}

	// 3. Clear CommitLog
	s.CommitLog = sync.Map{}
	s.pendingMutex.Lock()
	s.pendingPrePrepareCount = 0
	s.pendingMutex.Unlock()

	// Stop any running timers
	s.stopViewChangeTimer()
	s.stopNewViewTimer()

	// 4. Clear ExecutedLog
	s.ExecutedLog = sync.Map{}

	// 5. Reset Datastore to initial balances
	s.datastoreMutex.Lock()
	for clientID := range s.datastore {
		s.datastore[clientID] = 10.0 // Reset to initial balance or desired default
	}
	s.datastoreMutex.Unlock()

	// 6. Reset view number to 1 and set leader accordingly
	s.currentViewMutex.Lock()
	s.ViewNumber = 1
	s.currentViewMutex.Unlock()

	// 7. Update leader status based on the new view number
	s.updateLeaderStatus() // This sets IsLeader based on ViewNumber and ID

	// 8. Clear ViewChangeLog
	s.ViewChangeLog = sync.Map{}

	// 9. Clear PrepareMessagesReceived and CommitMessagesReceived logs
	s.PrepareMessagesReceived = sync.Map{}
	s.CommitMessagesReceived = sync.Map{}

	// 10. Reset sequence numbers and counters
	atomic.StoreInt64(&s.SequenceNumber, 0)
	s.lastExecutedSequenceNumber = 0

	// 11. Reset transaction queue
	s.executionMutex.Lock()
	s.transactionQueue = make(map[int64]PrePrepareMessage)
	s.executionMutex.Unlock()

	// 12. Clear NewViewMessages
	s.newViewMessagesMutex.Lock()
	s.NewViewMessages = []NewViewMessage{}
	s.newViewMessagesMutex.Unlock()

	// 13. Clear ReceivedLog
	s.ReceivedLog = sync.Map{}

	// 14. Reset status flags if necessary
	s.statusMutex.Lock()
	s.IsActive = true     // Assuming servers start as active after reset
	s.IsByzantine = false // Assuming servers are non-Byzantine after reset
	s.statusMutex.Unlock()

	*reply = true
	return nil
}

// Networking

func (s *Server) StartServer(port string) error {
	s.rpcServer = rpc.NewServer()
	err := s.rpcServer.Register(s)
	if err != nil {
		return err
	}
	s.listener, err = net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	// Update leader status at server start
	s.updateLeaderStatus()

	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				continue
			}
			go func() {
				s.rpcServer.ServeConn(conn)
			}()
		}
	}()
	return nil
}

func (s *Server) updateLeaderStatus() {
	s.IsLeader = s.ID == s.getLeaderIDForView(s.ViewNumber)
}

// StopServer gracefully shuts down the RPC server
func (s *Server) StopServer() {
	s.listener.Close()
}

// Utility functions

// signData signs the provided data string using the server's private key
func signData(privateKey *rsa.PrivateKey, data string) string {
	hash := sha256.Sum256([]byte(data))
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hash[:])
	if err != nil {
		return ""
	}
	signedData := base64.StdEncoding.EncodeToString(signature)
	return signedData
}

// verifySignature verifies the signature of the data using the provided public key
func verifySignature(publicKey *rsa.PublicKey, data, signature string) bool {
	hash := sha256.Sum256([]byte(data))
	signatureBytes, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return false
	}
	err = rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, hash[:], signatureBytes)
	return err == nil
}

// calculateDigest calculates the SHA-256 digest of the request
func calculateDigest(request Request) string {
	data := fmt.Sprintf("%v", request)
	hash := sha256.Sum256([]byte(data))
	digest := fmt.Sprintf("%x", hash[:])
	return digest
}

// requestSignatureData formats the data used for signing a request
func requestSignatureData(request Request) string {
	transaction := request.Transaction
	return fmt.Sprintf("<REQUEST,%s,%s,%.2f,%d,%s>", transaction.Sender, transaction.Receiver, transaction.Amount, request.Timestamp.UnixNano(), request.ClientID)
}

// prePrepareSignatureData formats the data used for signing a PRE-PREPARE message
func prePrepareSignatureData(msg PrePrepareMessage) string {
	return fmt.Sprintf("<PRE-PREPARE,%d,%d,%s>", msg.ViewNumber, msg.SequenceNumber, msg.Digest)
}

// prepareSignatureData formats the data used for signing a PREPARE message
func prepareSignatureData(msg PrepareMessage) string {
	return fmt.Sprintf("<PREPARE,%d,%d,%s,%s>", msg.ViewNumber, msg.SequenceNumber, msg.Digest, msg.NodeID)
}

// commitSignatureData formats the data used for signing a COMMIT message
func commitSignatureData(msg CommitMessage) string {
	return fmt.Sprintf("<COMMIT,%d,%d,%s,%s>", msg.ViewNumber, msg.SequenceNumber, msg.Digest, msg.NodeID)
}

func viewChangeSignatureData(msg ViewChangeMessage) string {
	return fmt.Sprintf("<VIEW-CHANGE,%d,%s,%v>", msg.ViewNumber, msg.NodeID, msg.P)
}

func newViewSignatureData(msg NewViewMessage) string {
	return fmt.Sprintf("<NEW-VIEW,%d,%v,%v>", msg.ViewNumber, msg.V, msg.O)
}

// combinedCommitSignatureData formats the data used for signing a CombinedCommitMessage
func combinedCommitSignatureData(combinedCommit CombinedCommitMessage) string {
	return fmt.Sprintf("<COMBINED-COMMIT,%d,%d,%v>",
		combinedCommit.ViewNumber,
		combinedCommit.SequenceNumber,
		combinedCommit.CommitMessages)
}

// replySignatureData formats the data used for signing a Reply message
func replySignatureData(reply Reply) string {
	data := fmt.Sprintf("<REPLY,%d,%d,%s,%s,%s>", reply.ViewNumber, reply.Timestamp.UnixNano(), reply.ClientID, reply.NodeID, reply.Result)
	return data
}

// getServerAddress returns the network address of a given server ID
func getServerAddress(serverID string) string {
	// For simplicity, assuming ports start from 1231
	portNumber := 1230 + parseServerID(serverID)
	address := fmt.Sprintf("localhost:%d", portNumber)
	return address
}

// parseServerID extracts the numerical ID from a server ID string (e.g., "S1" -> 1)
func parseServerID(serverID string) int {
	var id int
	fmt.Sscanf(serverID, "S%d", &id)
	return id
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
	if block == nil || block.Type != "RSA PUBLIC KEY" {
		return nil, errors.New("failed to decode PEM block containing public key")
	}
	publicKey, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	return publicKey, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run server.go <ServerID>")
		os.Exit(1)
	}
	serverID := os.Args[1]

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
	f := 2

	otherServerKeys := make(map[string]*rsa.PublicKey)
	totalServers := 3*f + 1
	for i := 1; i <= totalServers; i++ {
		id := fmt.Sprintf("S%d", i)
		// Load each server's public key
		publicKeyPath := fmt.Sprintf("../server_keys/%s_public.pem", id)
		pubKey, err := loadPublicKeyFromFile(publicKeyPath)
		if err != nil {
			log.Fatalf("Error Loading Public Key for Server %s - %v", id, err)
		}
		otherServerKeys[id] = pubKey
	}

	clientKeys := make(map[string]*rsa.PublicKey)
	clientIDs := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	for _, id := range clientIDs {
		publicKeyPath := fmt.Sprintf("../client_keys/%s_public.pem", id)
		pubKey, err := loadPublicKeyFromFile(publicKeyPath)
		if err != nil {
			log.Fatalf("Failed to Load Public Key for Client %s - %v", id, err)
		}
		clientKeys[id] = pubKey
	}

	// Initialize server
	isLeader := serverID == "S1"
	server := NewServer(serverID, isLeader, f, privateKey, publicKey, otherServerKeys, clientKeys)
	port := fmt.Sprintf("%d", 1230+parseServerID(serverID))
	err = server.StartServer(port)
	if err != nil {
		log.Fatalf("Failed to Start Server on Port %s - %v", port, err)
	}
	defer server.StopServer()

	// Keep the server running
	select {}
}
