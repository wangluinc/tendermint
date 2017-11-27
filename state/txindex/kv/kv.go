package kv

import (
	"bytes"
	"fmt"
	"reflect"

	wire "github.com/tendermint/go-wire"

	"github.com/pkg/errors"
	db "github.com/tendermint/tmlibs/db"

	"github.com/tendermint/tendermint/state/txindex"
	"github.com/tendermint/tendermint/types"
	"github.com/tendermint/tmlibs/pubsub/query"
)

var _ txindex.TxIndexer = (*TxIndex)(nil)

// TxIndex is the simplest possible indexer, backed by Key-Value storage (levelDB).
// It can only index transaction by its identifier.
type TxIndex struct {
	store db.DB
}

// NewTxIndex returns new instance of TxIndex.
func NewTxIndex(store db.DB) *TxIndex {
	return &TxIndex{store: store}
}

// Get gets transaction from the TxIndex storage and returns it or nil if the
// transaction is not found.
func (txi *TxIndex) Get(hash []byte) (*types.TxResult, error) {
	if len(hash) == 0 {
		return nil, txindex.ErrorEmptyHash
	}

	rawBytes := txi.store.Get(hash)
	if rawBytes == nil {
		return nil, nil
	}

	r := bytes.NewReader(rawBytes)
	var n int
	var err error
	txResult := wire.ReadBinary(&types.TxResult{}, r, 0, &n, &err).(*types.TxResult)
	if err != nil {
		return nil, fmt.Errorf("Error reading TxResult: %v", err)
	}

	return txResult, nil
}

// AddBatch indexes a batch of transations using the given list of tags.
func (txi *TxIndex) AddBatch(b *txindex.Batch, allowedTags []string) error {
	storeBatch := txi.store.NewBatch()

	for _, result := range b.Ops {
		hash := result.Tx.Hash()

		// index tx by tags
		for _, tag := range result.Result.Tags {
			if stringInSlice(tag.Key, allowedTags) {
				storeBatch.Set(keyForTag(tag, result), hash)
			}
		}

		// index by height / index
		if stringInSlice(types.TxHeightKey, allowedTags) {
			b.Set(heightKey(result), hash)
		}

		// index tx by hash
		rawBytes := wire.BinaryBytes(&result)
		storeBatch.Set(hash, rawBytes)
	}

	storeBatch.Write()
	return nil
}

// Index indexes a single transaction using the given list of tags.
func (txi *TxIndex) Index(result *types.TxResult, tags []string) error {
	b := txi.store.NewBatch()
	hash := result.Tx.Hash()

	// index by tags
	for _, tag := range result.Result.Tags {
		if stringInSlice(tag.Key, tags) {
			b.Set(keyForTag(tag, result), hash)
		}
	}

	// index by hash
	rawBytes := wire.BinaryBytes(result)
	b.Set(hash, rawBytes)

	b.Write()
	return nil
}

func (txi *TxIndex) Search(q *query.Query) ([]*types.TxResult, error) {
	hashes := make(map[[]byte]struct{})

	// get a list of conditions (like "tx.height > 5")
	conditions := q.Conditions()

	// if there is a hash condition, return the result immediately
	hash, ok := extractHash(conditions)
	if ok {
		res, err := txi.Get(hash)
		if err != nil {
			return []*types.TxResult{}, err
		}
		return []*types.TxResult{res}, nil
	}

	// if there is a height condition ("tx.height=3"), extract it for faster lookups
	height, heightIndex := lookForHeight(conditions)

	skipIndexes := make([]int, 0)
	if heightIndex >= 0 {
		skipIndexes = append(skipIndexes, heightIndex)
	}

	var hashes2 [][]byte

	// extract ranges
	//
	// if both upper and lower bounds exist, it's better to get them in order not
	// no iterate over kvs that are not within range.
	ranges, rangeIndexes := lookForRanges(conditions)
	if len(ranges) > 0 {
		skipIndexes = append(skipIndexes, rangeIndexes...)
	}
	for _, r := range ranges {
		if heightSpecified {
			hashes2 = matchRangeWithHeight(r, height)
		} else {
			hashes2 = matchRange(r)
		}

		// special case
		if len(hashes) == 0 {
			hashes = hashes2
			continue
		}

		// perform intersection as we go
		for _, h := range hashes2 {
			if _, ok := hashes[h]; !ok {
				delete(hashes, h)
			}
		}
	}

	// for all other conditions
	for i, c := range conditions {
		if intInSlice(i, skipIndexes) {
			continue
		}

		if heightSpecified {
			hashes2 = matchWithHeight(c, height)
		} else {
			hashes2 = match(c)
		}

		// special case
		if len(hashes) == 0 {
			hashes = hashes2
			continue
		}

		// perform intersection as we go
		for _, h := range hashes2 {
			if _, ok := hashes[h]; !ok {
				delete(hashes, h)
			}
		}
	}

	var err error
	results := make([]*types.TxResult, len(hashes))
	for i, h := range hashes {
		results[i], err = txi.Get(h)
		if err != nil {
			return errors.Wrapf(err, "failed to get Tx{%X}", h)
		}
	}
}

func lookForHeight(conditions []query.Condition) (height uint64, index int) {
	for i, c := range conditions {
		if c.Key == types.TxHeightKey {
			return c.Value, i
		}
	}
	return 0, -1
}

type queryRanges map[string]queryRange

type queryRange struct {
	key         string
	lowerBound  interface{} // int || time.Time
	lbInclusive bool
	upperBound  interface{} // int || time.Time
	ubInclusive bool
}

func lookForRanges(conditions []query.Condition) (ranges queryRanges, indexes []int) {
	for i, c := range conditions {
		if isRangeOperation(c.Operation) {
			if _, ok := ranges[c.Key]; !ok {
				ranges[c.Key] = queryRange{key: c.Key}
			}
			// greater or greaterOrEqual
			if c.Operation == ">" || c.Operation == ">=" {
				// TODO: inclusive or exclusive
				ranges[c.Key].lowerBound = c.Value
			} else { // less or lessOrEqual
				// TODO: inclusive or exclusive
				ranges[c.Key].upperBound = c.Value
			}
			indexes = append(indexes, i)
		}
	}
	return ranges
}

func isRangeOperation(op string) bool {
	switch op {
	case ">", "<", ">=", "<=":
		return true
	default:
		return false
	}
}

func match(c query.Condition) (hashes [][]byte) {
	if c.Operation == "=" {
		it := txi.store.IteratorPrefix([]byte(fmt.Sprintf("%s=%v", c.Key, c.Value)))
		for it.Next() {
			hashes = append(hashes, it.Value())
		}
	} else if c.Operation == "CONTAINS" {
		// TODO fullscan
	}
}

func matchWithHeight(c query.Condition, height uint64) (hashes [][]byte) {
	if c.Operation == "=" {
		it := txi.store.IteratorPrefix([]byte(fmt.Sprintf("%s=%v/%d", c.Key, c.Value, height)))
		for it.Next() {
			hashes = append(hashes, it.Value())
		}
	} else if c.Operation == "CONTAINS" {
		// TODO fullscan
	}
}

func matchRange(r queryRange) (hashes [][]byte) {
	valueType := reflect.TypeOf(r.upperBound)
	it := txi.store.IteratorPrefix([]byte(fmt.Sprintf("%s=%s", r.key, r.lowerBound)))
	defer it.Release()
	for it.Next() {
		// no other way to stop iterator other than checking for upperBound
		if extractValue(it.Key(), valueType) == r.upperBound {
			break
		}
		hashes = append(hashes, it.Value())
	}
	return
}

func matchRangeWithHeight(r queryRange, height uint64) (hashes [][]byte) {
	valueType := reflect.TypeOf(r.upperBound)
	it := txi.store.IteratorPrefix([]byte(fmt.Sprintf("%s=%s/%d", r.key, r.lowerBound, height)))
	defer it.Release()
	for it.Next() {
		// no other way to stop iterator other than checking for upperBound
		if extractValue(it.Key(), valueType) == r.upperBound {
			break
		}
		hashes = append(hashes, it.Value())
	}
	return
}

func keyForTag(tag *abci.KVPair, result *types.TxResult) []byte {
	switch tag.ValueType {
	case abci.KVPair_STRING:
		return []byte(fmt.Sprintf("%s=%s/%d/%d", tag.Key, tag.ValueString, result.Height, result.Index))
	case abci.KVPair_INT:
		return []byte(fmt.Sprintf("%s=%d/%d/%d", tag.Key, tag.ValueInt, result.Height, result.Index))
	}
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func intInSlice(a int, list []int) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
