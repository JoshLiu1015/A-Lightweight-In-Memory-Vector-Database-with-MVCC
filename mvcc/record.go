package mvcc

type Record struct {
	ID       string // unique identifier for the record
    Vector   []float64
    Metadata map[string]string
    BeginTS  int  // begin timestamp
    EndTS    int  // end timestamp (math.MaxInt32 if still valid)
    Deleted  bool // true if deleted
	CreatedByTxnID    int  // transaction ID that created this version
    Next     *Record // newer version
}
