package mvcc

type TransactionStatus int

const (
    Active TransactionStatus = iota
    Committed
    Aborted
)

type Transaction struct {
    id     int
    status TransactionStatus
    startTS int // same as ID for simplicity
}
