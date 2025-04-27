package mvcc

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"
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

	// stall until the transaction is committed
	for store.transactions[head.CreatedByTxnID].Status == Active && head.CreatedByTxnID != txnId {
		runtime.Gosched() // yield the processor to allow other goroutines to run
		time.Sleep(1 * time.Millisecond)
	}

	record.Next = head
	store.records[record.ID] = record
	return nil
}

func (store *Store) delete(txnId int, recordID string) error {
	store.lock.Lock()
	defer store.lock.Unlock()

	head := store.records[recordID]
	if head == nil {
		return fmt.Errorf("record with ID %s not found", recordID)
	}

	head.Deleted = true
	head.EndTS = txnId
	store.records[recordID] = head
	return nil
}


func (store *Store) read(txnId int) []*Record {
	store.lock.RLock()
	defer store.lock.RUnlock()

	validRecords := make([]*Record, 0)

	for _, head := range store.records {
		current := head
		for head != nil {
			creatorTxn := store.transactions[current.CreatedByTxnID]
            if creatorTxn != nil && creatorTxn.Status != Committed {
                // Skip uncommitted versions
                current = current.Next
                continue
            }
			
			if current.BeginTS <= txnId && txnId < current.EndTS && !current.Deleted {
                validRecords = append(validRecords, current)
                break // Stop at first visible version for this recordID
            }
			
			current = current.Next
		}
	}

	return validRecords
}


func (store *Store) CommitTransaction(txnId int) error {
    store.lock.Lock()
    defer store.lock.Unlock()

    txn := store.transactions[txnId]
    if txn == nil {
        return fmt.Errorf("transaction %d not found", txnId)
    }

    for _, head := range store.records {
        current := head
        for current != nil {
            if current.CreatedByTxnID == txnId {
                if current.Next != nil {
                    current.Next.EndTS = current.BeginTS
                }
            }
            current = current.Next
        }
    }

    txn.Status = Committed
    return nil
}
