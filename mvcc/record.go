package mvcc

type Record struct {
	id       string // unique identifier for the record
    vector   []float64
    metadata map[string]string
    beginTS  int  // begin timestamp
    endTS    int  // end timestamp (math.MaxInt32 if still valid)
    deleted  bool // true if deleted
	createdByTxnID    int  // transaction ID that created this version
    next     *Record // newer version
}
