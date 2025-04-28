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
func newStore() *Store {
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
		id: store.currentTxnID,
		status: Active,
	}
	store.transactions[txn.id] = txn
	return txn.id
}


func (store *Store) insert(txnId int, record *Record) error {
	record.beginTS = txnId
	record.endTS = math.MaxInt32
	record.deleted = false
	
	store.lock.Lock()
	defer store.lock.Unlock()

	if store.records[record.id] != nil {
		return fmt.Errorf("record with ID %s already exists", record.id)
	}

	store.records[record.id] = record
	return nil
}

func (store *Store) update(txnId int, record *Record) error {
	record.beginTS = txnId
	record.endTS = math.MaxInt32
	record.deleted = false
	
	store.lock.Lock()
	defer store.lock.Unlock()

	head := store.records[record.id]
	if head == nil {
		return fmt.Errorf("record with ID %s not found", record.id)
	}

	// stall until the transaction is committed
	for store.transactions[head.createdByTxnID].status == Active && head.createdByTxnID != txnId {
		runtime.Gosched() // yield the processor to allow other goroutines to run
		time.Sleep(1 * time.Millisecond)
	}

	record.next = head
	store.records[record.id] = record
	return nil
}

func (store *Store) delete(txnId int, recordID string) error {
	store.lock.Lock()
	defer store.lock.Unlock()

	head := store.records[recordID]
	if head == nil {
		return fmt.Errorf("record with ID %s not found", recordID)
	}

	head.deleted = true
	head.endTS = txnId
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
			creatorTxn := store.transactions[current.createdByTxnID]
            if creatorTxn != nil && creatorTxn.status != Committed {
                // Skip uncommitted versions
                current = current.next
                continue
            }
			
			if current.beginTS <= txnId && txnId < current.endTS && !current.deleted {
                validRecords = append(validRecords, current)
                break // Stop at first visible version for this recordID
            }
			
			current = current.next
		}
	}

	return validRecords
}


func (store *Store) commitTransaction(txnId int) error {
    store.lock.Lock()
    defer store.lock.Unlock()

    txn := store.transactions[txnId]
    if txn == nil {
        return fmt.Errorf("transaction %d not found", txnId)
    }

    for _, head := range store.records {
        current := head
        for current != nil {
            if current.createdByTxnID == txnId {
                if current.next != nil {
                    current.next.endTS = current.beginTS
                }
            }
            current = current.next
        }
    }

    txn.status = Committed
    return nil
}
