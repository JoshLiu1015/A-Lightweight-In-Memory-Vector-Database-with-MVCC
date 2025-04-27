package mvcc

import (
	"fmt"
	"math"
	"sync"
)

type Store struct {
    records      map[string]*Record // latest version for each record
    transactions map[int]*Transaction
    lock         sync.RWMutex
    currentTxnID int
}

// Create a new store
func NewStore() *Store {
    return &Store{
        records:      make(map[string]*Record),
        transactions: make(map[int]*Transaction),
        currentTxnID: 0,
    }
}


func (store *Store) beginTransaction() int {
	store.lock.Lock()
	defer store.lock.Unlock()

	store.currentTxnID++
	txn := &Transaction{
		ID: store.currentTxnID,
		Status: Active,
	}
	store.transactions[txn.ID] = txn
	return txn.ID
}


func (store *Store) insert(txnId int, record *Record) error {
	record.BeginTS = txnId
	record.EndTS = math.MaxInt32
	record.Deleted = false
	
	store.lock.Lock()
	defer store.lock.Unlock()

	if store.records[record.ID] != nil {
		return fmt.Errorf("record with ID %s already exists", record.ID)
	}

	store.records[record.ID] = record
	return nil
}

func (store *Store) update(txnId int, record *Record) error {
	record.BeginTS = txnId
	record.EndTS = math.MaxInt32
	record.Deleted = false
	
	store.lock.Lock()
	defer store.lock.Unlock()

	head := store.records[record.ID]
	if head == nil {
		return fmt.Errorf("record with ID %s not found", record.ID)
	}

	record.Next = head
	store.records[record.ID] = record
	return nil
}
