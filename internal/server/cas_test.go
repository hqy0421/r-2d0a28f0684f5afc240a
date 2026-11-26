package server

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"little-db/internal/engine"
	"little-db/internal/wire"
)

// TestServerCASStatuses exercises both precondition shapes through the
// full wire path, including the distinct absent-key vs empty-value
// semantics and the error envelope on a failed precondition.
func TestServerCASStatuses(t *testing.T) {
	_, addr := startServer(t, nil)
	conn := dial(t, addr)

	// If-value on an absent key: PRECONDITION_FAILED, even with an empty
	// expected value.
	st, body := roundTrip(t, conn, &wire.CASRequest{
		Key: []byte("k"), NewValue: []byte("v"), Expected: []byte(""), Mode: wire.CASIfValue,
	})
	if st != wire.StatusPreconditionFailed {
		t.Fatalf("if-value on absent key: status=%v want PRECONDITION_FAILED", st)
	}
	if msg, err := wire.DecodeError(body); err != nil {
		t.Fatalf("DecodeError: %v", err)
	} else if msg == "" {
		t.Fatal("precondition-failed frame should carry an error envelope")
	}

	// Put an explicit empty value.
	if st, _ := roundTrip(t, conn, &wire.PutRequest{Key: []byte("k"), Value: []byte("")}); st != wire.StatusOK {
		t.Fatalf("seed empty PUT: %v", st)
	}

	// The same empty expect now applies — present-empty ≠ absent.
	st, _ = roundTrip(t, conn, &wire.CASRequest{
		Key: []byte("k"), NewValue: []byte("armed"), Expected: []byte{}, Mode: wire.CASIfValue,
	})
	if st != wire.StatusOK {
		t.Fatalf("empty->armed CAS: %v", st)
	}
	st, body = roundTrip(t, conn, &wire.GetRequest{Key: []byte("k")})
	if st != wire.StatusOK {
		t.Fatalf("GET: %v", st)
	}
	val, err := wire.DecodeGetOK(body)
	if err != nil || string(val) != "armed" {
		t.Fatalf("got=%q err=%v want armed", val, err)
	}

	// Stale expect fails without changing the value.
	st, _ = roundTrip(t, conn, &wire.CASRequest{
		Key: []byte("k"), NewValue: []byte("nope"), Expected: []byte("old"), Mode: wire.CASIfValue,
	})
	if st != wire.StatusPreconditionFailed {
		t.Fatalf("stale CAS: status=%v want PRECONDITION_FAILED", st)
	}
	st, body = roundTrip(t, conn, &wire.GetRequest{Key: []byte("k")})
	if st != wire.StatusOK {
		t.Fatalf("GET after conflict: %v", st)
	}
	val, _ = wire.DecodeGetOK(body)
	if string(val) != "armed" {
		t.Fatalf("value changed by failed CAS: %q", val)
	}

	// If-absent fails on a present key (even one holding an empty value),
	// and succeeds once the key is deleted.
	st, _ = roundTrip(t, conn, &wire.CASRequest{
		Key: []byte("k"), NewValue: []byte("created"), Mode: wire.CASIfAbsent,
	})
	if st != wire.StatusPreconditionFailed {
		t.Fatalf("if-absent on present key: %v", st)
	}
	if st, _ := roundTrip(t, conn, &wire.DeleteRequest{Key: []byte("k")}); st != wire.StatusOK {
		t.Fatalf("DELETE: %v", st)
	}
	st, _ = roundTrip(t, conn, &wire.CASRequest{
		Key: []byte("k"), NewValue: []byte("created"), Mode: wire.CASIfAbsent,
	})
	if st != wire.StatusOK {
		t.Fatalf("if-absent after delete: %v", st)
	}
}

// TestServerCASBadRequest maps malformed conditional writes to
// BAD_REQUEST while keeping the connection alive (a second request on
// the same conn must still work).
func TestServerCASBadRequest(t *testing.T) {
	_, addr := startServer(t, nil)
	conn := dial(t, addr)

	// Hand-built frame: OpCAS (0x08) with a 3-byte body, below the
	// minimum legal CAS body.
	frame := []byte{0, 0, 0, 4, uint8(wire.OpCAS), 0, 0, 0}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, _, err := wire.ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if wire.Status(tag) != wire.StatusBadRequest {
		t.Fatalf("status=%v want BAD_REQUEST", tag)
	}

	// Connection stays usable.
	st, _ := roundTrip(t, conn, &wire.PingRequest{})
	if st != wire.StatusOK {
		t.Fatalf("PING after bad CAS: %v", st)
	}
}

// TestServerCASConcurrentWinners is the cross-process-shaped scenario:
// N independent TCP clients all conditional-write from the same old
// value. Exactly one OK comes back; every other client sees
// PRECONDITION_FAILED, and a subsequent GET returns the winner.
func TestServerCASConcurrentWinners(t *testing.T) {
	_, addr := startServer(t, nil)
	seed := dial(t, addr)
	if st, _ := roundTrip(t, seed, &wire.PutRequest{Key: []byte("shared"), Value: []byte("old")}); st != wire.StatusOK {
		t.Fatalf("seed PUT: %v", st)
	}

	const clients = 32
	var okCount, conflictCount, otherCount atomic.Int64
	var wg sync.WaitGroup
	startGate := make(chan struct{})
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			conn := dial(t, addr)
			<-startGate
			st, _ := roundTrip(t, conn, &wire.CASRequest{
				Key:      []byte("shared"),
				NewValue: []byte("writer-" + strconv.Itoa(n)),
				Expected: []byte("old"),
				Mode:     wire.CASIfValue,
			})
			switch st {
			case wire.StatusOK:
				okCount.Add(1)
			case wire.StatusPreconditionFailed:
				conflictCount.Add(1)
			default:
				otherCount.Add(1)
				t.Errorf("unexpected status %v", st)
			}
		}(i)
	}
	close(startGate)
	wg.Wait()

	if okCount.Load() != 1 {
		t.Fatalf("OK responses=%d want 1 (conflicts=%d other=%d)", okCount.Load(), conflictCount.Load(), otherCount.Load())
	}
	if conflictCount.Load() != clients-1 {
		t.Fatalf("conflicts=%d want %d", conflictCount.Load(), clients-1)
	}
	st, body := roundTrip(t, seed, &wire.GetRequest{Key: []byte("shared")})
	if st != wire.StatusOK {
		t.Fatalf("final GET: %v", st)
	}
	val, err := wire.DecodeGetOK(body)
	if err != nil || string(val) == "old" {
		t.Fatalf("final value=%q err=%v, want a winning write", val, err)
	}
}

// TestServerCASErrorClassification pins classifyEngineErr's mapping for
// the conflict sentinel.
func TestServerCASErrorClassification(t *testing.T) {
	status, _ := classifyEngineErr(engine.ErrCASConflict)
	if status != wire.StatusPreconditionFailed {
		t.Fatalf("classify=%v want PRECONDITION_FAILED", status)
	}
}
