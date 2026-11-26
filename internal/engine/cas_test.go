package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// TestCompareAndSwapValuePrecondition covers the value-based
// (expectExists=true) shape, including the absent-vs-empty distinction.
func TestCompareAndSwapValuePrecondition(t *testing.T) {
	db := openTestDB(t, false)
	key := []byte("switch")

	// Key absent: a value-based precondition must conflict, even when the
	// expected value is the empty string.
	if err := db.CompareAndSwap(key, []byte(""), []byte("on"), true); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("CAS on absent key with empty expect: err=%v want ErrCASConflict", err)
	}
	if err := db.CompareAndSwap(key, []byte("old"), []byte("on"), true); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("CAS on absent key: err=%v want ErrCASConflict", err)
	}

	// Seed the key with an explicit empty value.
	if err := db.Put(key, []byte("")); err != nil {
		t.Fatalf("Put empty: %v", err)
	}
	// Present-but-empty is a different precondition from absent: the same
	// empty expect that failed above must now apply.
	if err := db.CompareAndSwap(key, []byte(""), []byte("armed"), true); err != nil {
		t.Fatalf("CAS empty->armed: %v", err)
	}
	if got, err := db.Get(key); err != nil || string(got) != "armed" {
		t.Fatalf("after empty->armed: got=%q err=%v", got, err)
	}

	// Stale precondition: another writer moved the key to "on"; a request
	// still expecting "armed" must be refused.
	if err := db.Put(key, []byte("on")); err != nil {
		t.Fatalf("Put on: %v", err)
	}
	if err := db.CompareAndSwap(key, []byte("armed"), []byte("off"), true); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("stale CAS: err=%v want ErrCASConflict", err)
	}
	// The refused write must not have changed anything.
	if got, _ := db.Get(key); string(got) != "on" {
		t.Fatalf("value after failed CAS = %q want %q", got, "on")
	}

	// Current precondition applies, including replacing with an empty new
	// value.
	if err := db.CompareAndSwap(key, []byte("on"), []byte(""), true); err != nil {
		t.Fatalf("CAS on->empty: %v", err)
	}
	if got, err := db.Get(key); err != nil || len(got) != 0 {
		t.Fatalf("after on->empty: got=%q err=%v", got, err)
	}
}

// TestCompareAndSwapIfAbsent covers the insert-if-absent
// (expectExists=false) shape.
func TestCompareAndSwapIfAbsent(t *testing.T) {
	db := openTestDB(t, false)
	key := []byte("singleton")

	// Absent: created.
	if err := db.CompareAndSwap(key, nil, []byte("v1"), false); err != nil {
		t.Fatalf("first put-if-absent: %v", err)
	}
	if got, _ := db.Get(key); string(got) != "v1" {
		t.Fatalf("got=%q want v1", got)
	}

	// Present (any value, including an empty one): conflicts.
	if err := db.CompareAndSwap(key, nil, []byte("v2"), false); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("second put-if-absent: err=%v want ErrCASConflict", err)
	}
	if got, _ := db.Get(key); string(got) != "v1" {
		t.Fatalf("value after conflict = %q want v1", got)
	}

	if err := db.Put(key, []byte("")); err != nil {
		t.Fatalf("Put empty: %v", err)
	}
	if err := db.CompareAndSwap(key, nil, []byte("v3"), false); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("put-if-absent over empty value: err=%v want ErrCASConflict", err)
	}
	got, err := db.Get(key)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty value overwritten? got=%q err=%v", got, err)
	}

	// After a delete the key is absent again, so the create succeeds.
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := db.CompareAndSwap(key, nil, []byte("v4"), false); err != nil {
		t.Fatalf("put-if-absent after delete: %v", err)
	}
	if got, _ := db.Get(key); string(got) != "v4" {
		t.Fatalf("got=%q want v4", got)
	}

	// API guard: expectValue is meaningless in absent mode.
	if err := db.CompareAndSwap(key, []byte("x"), []byte("v"), false); err == nil {
		t.Fatal("expectValue non-nil with expectExists=false should be rejected")
	}
}

// TestCompareAndSwapConflictAppendsNothing pins the "normal unapplied
// result" contract: a conflict must leave both the keydir and the
// segment byte count untouched.
func TestCompareAndSwapConflictAppendsNothing(t *testing.T) {
	db := openTestDB(t, false)
	key := []byte("k")
	if err := db.Put(key, []byte("old")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	before := db.Stats().BytesOnDisk

	for i := 0; i < 5; i++ {
		err := db.CompareAndSwap(key, []byte("stale"), []byte(fmt.Sprintf("x%d", i)), true)
		if !errors.Is(err, ErrCASConflict) {
			t.Fatalf("conflict %d: err=%v want ErrCASConflict", i, err)
		}
	}
	if after := db.Stats().BytesOnDisk; after != before {
		t.Fatalf("BytesOnDisk changed after conflicts: before=%d after=%d", before, after)
	}
	if got, _ := db.Get(key); string(got) != "old" {
		t.Fatalf("got=%q want old", got)
	}

	// A successful CAS does append exactly one record.
	if err := db.CompareAndSwap(key, []byte("old"), []byte("new"), true); err != nil {
		t.Fatalf("winning CAS: %v", err)
	}
	if after := db.Stats().BytesOnDisk; after <= before {
		t.Fatalf("winning CAS appended nothing: before=%d after=%d", before, after)
	}
}

// TestCompareAndSwapConcurrentLinearizes is the headline concurrency
// property: every contender starts from the same observed old value and
// writes a distinct new value. Exactly one may apply; all others must
// receive ErrCASConflict, and the final stored value must be the single
// winner's.
func TestCompareAndSwapConcurrentLinearizes(t *testing.T) {
	db := openTestDB(t, false)
	key := []byte("shared")
	if err := db.Put(key, []byte("old")); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	const contenders = 64
	var (
		wg       sync.WaitGroup
		applied  atomic.Int64
		conflict atomic.Int64
		otherErr atomic.Int64
	)
	start := make(chan struct{})
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			newVal := []byte(fmt.Sprintf("writer-%02d", n))
			err := db.CompareAndSwap(key, []byte("old"), newVal, true)
			switch {
			case err == nil:
				applied.Add(1)
			case errors.Is(err, ErrCASConflict):
				conflict.Add(1)
			default:
				otherErr.Add(1)
				t.Errorf("unexpected CAS error: %v", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if got := applied.Load(); got != 1 {
		t.Fatalf("applied=%d want exactly 1 (conflicts=%d other=%d)", got, conflict.Load(), otherErr.Load())
	}
	if got := conflict.Load(); got != contenders-1 {
		t.Fatalf("conflicts=%d want %d", got, contenders-1)
	}
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	if string(got) == "old" || len(got) == 0 {
		t.Fatalf("final value %q is not a winning write", got)
	}

	// A second round must find zero successes: nobody still holds "old".
	var wg2 sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			err := db.CompareAndSwap(key, []byte("old"), []byte("late"), true)
			if !errors.Is(err, ErrCASConflict) {
				t.Errorf("late CAS err=%v want ErrCASConflict", err)
			}
		}()
	}
	wg2.Wait()
	if after, _ := db.Get(key); string(after) != string(got) {
		t.Fatalf("value changed %q -> %q from losing writes", got, after)
	}
}

// TestCompareAndSwapSurvivesReopen verifies the durability half of the
// contract: the winning conditional write is recovered after reopening
// the data directory, while conflicts left nothing to recover.
func TestCompareAndSwapSurvivesReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	db, err := Open(Options{Dir: dir, SyncOnPut: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	key := []byte("switch")
	if err := db.Put(key, []byte("old")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := db.CompareAndSwap(key, []byte("old"), []byte("winner"), true); err != nil {
		t.Fatalf("winning CAS: %v", err)
	}
	if err := db.CompareAndSwap(key, []byte("old"), []byte("loser"), true); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("losing CAS: err=%v want ErrCASConflict", err)
	}
	if err := db.CompareAndSwap(key, nil, []byte("creator"), false); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("absent CAS on present key: err=%v want ErrCASConflict", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir, SyncOnPut: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	got, err := reopened.Get(key)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(got) != "winner" {
		t.Fatalf("after reopen got=%q want winner (losing writes must not exist on disk)", got)
	}

	// The reopened engine enforces the same preconditions.
	if err := reopened.CompareAndSwap(key, []byte("old"), []byte("x"), true); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("stale CAS after reopen: err=%v want ErrCASConflict", err)
	}
	if err := reopened.CompareAndSwap(key, []byte("winner"), []byte("again"), true); err != nil {
		t.Fatalf("CAS after reopen: %v", err)
	}
}

// TestCompareAndSwapValidation mirrors Put's input validation on the
// conditional path.
func TestCompareAndSwapValidation(t *testing.T) {
	db := openTestDB(t, false)
	cases := []struct {
		name   string
		key    []byte
		newVal []byte
	}{
		{"empty key", nil, []byte("v")},
		{"oversize key", make([]byte, maxKeyLen+1), []byte("v")},
		{"oversize value", []byte("k"), make([]byte, maxValLen+1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := db.CompareAndSwap(tc.key, []byte("e"), tc.newVal, true); err == nil {
				t.Fatal("want validation error, got nil")
			}
		})
	}
}
