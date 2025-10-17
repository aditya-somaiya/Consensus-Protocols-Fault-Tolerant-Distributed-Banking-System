package models

import "time"

// Transaction represents a single transaction.
type Transaction struct {
	ID        int
	SetNumber int
	Sender    string
	Receiver  string
	Amount    float64
	Timestamp time.Time
}

// MajorBlock represents the consolidated block of transactions.
type MajorBlock struct {
	SequenceNumber int
	BallotNumber   int
	Transactions   []Transaction
}

// PromiseResponse represents the response to a Prepare message.
type PromiseResponse struct {
	AcceptNum        int
	AcceptVal        *MajorBlock
	LocalLog         []Transaction
	Success          bool
	BallotNumber     int
	LastCommittedSN  int
	MissingBlocks    []MajorBlock
	CurrentMaxBallot int
}

type SyncRequest struct {
	FromSequence int
}

type SyncResponse struct {
	MajorBlocks []MajorBlock
}

type PrepareRequest struct {
	BallotNumber    int
	LeaderCommitted int
	LeaderAddress   string
}

type PerformanceMetrics struct {
	AverageLatency time.Duration
	Throughput     float64
}
