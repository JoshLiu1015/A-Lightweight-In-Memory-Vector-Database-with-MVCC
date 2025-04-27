package mvcc

type TransactionStatus int

const (
    Active TransactionStatus = iota
    Committed
    Aborted
)

type Transaction struct {
    ID     int
    Status TransactionStatus
    StartTS int // same as ID for simplicity
}
