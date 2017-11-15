package buid

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
)

func TestLexicographicOrder(t *testing.T) {
	p := NewProcess(2)
	ts := time.Now().UTC().Truncate(time.Hour)
	_ = p.NewID(1, ts)

	dir := "test_order"
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	db, err := openBadger(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(txn *badger.Txn) error {
		{
			id := p.NewID(1, ts)
			_, key := id.Split()
			if err := txn.Set(key[:], []byte{0}); err != nil {
				return err
			}
		}
		{
			id := p.NewID(1, ts.Add(time.Microsecond))
			_, key := id.Split()
			if err := txn.Set(key[:], []byte{1}); err != nil {
				return err
			}
		}
		{
			id := p.NewID(1, ts.Add(999999*time.Microsecond))
			_, key := id.Split()
			if err := txn.Set(key[:], []byte{2}); err != nil {
				return err
			}
		}
		{
			id := p.NewID(1, ts.Add(time.Second))
			_, key := id.Split()
			if err := txn.Set(key[:], []byte{3}); err != nil {
				return err
			}
		}
		{
			id := p.NewID(1, ts.Add(time.Second+time.Microsecond))
			_, key := id.Split()
			if err := txn.Set(key[:], []byte{4}); err != nil {
				return err
			}
		}
		{
			id := p.NewID(1, ts.Add(59*time.Second))
			_, key := id.Split()
			if err := txn.Set(key[:], []byte{5}); err != nil {
				return err
			}
		}
		{
			id := p.NewID(1, ts.Add(time.Minute))
			_, key := id.Split()
			if err := txn.Set(key[:], []byte{6}); err != nil {
				return err
			}
		}
		{
			id := p.NewID(1, ts.Add(59*time.Minute))
			_, key := id.Split()
			if err := txn.Set(key[:], []byte{7}); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var seq []int
	if err := db.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		it := txn.NewIterator(opt)
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			value, err := item.Value()
			if err != nil {
				return err
			}
			seq = append(seq, int(value[0]))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := range seq {
		if seq[i] != i {
			log.Fatalf("out of order %v", seq)
		}
	}
}

func openBadger(dir string) (*badger.DB, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = opts.Dir
	return badger.Open(opts)
}
