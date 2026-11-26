package client

import (
	"bytes"
	"errors"
	"testing"

	"little-db/internal/wire"
)

func TestClientCompareAndSwapOK(t *testing.T) {
	var gotReq *wire.CASRequest
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		gotReq = req.(*wire.CASRequest)
		return [][2]any{okFrame(nil)}
	})
	c := newTestClient(t, conn)
	if err := c.CompareAndSwap([]byte("k"), []byte("old"), []byte("new")); err != nil {
		t.Fatalf("CompareAndSwap: %v", err)
	}
	if gotReq.Mode != wire.CASIfValue ||
		!bytes.Equal(gotReq.Key, []byte("k")) ||
		!bytes.Equal(gotReq.Expected, []byte("old")) ||
		!bytes.Equal(gotReq.NewValue, []byte("new")) {
		t.Fatalf("server saw %#v", gotReq)
	}
}

func TestClientCompareAndSwapPreconditionFailed(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		return [][2]any{errFrame(wire.StatusPreconditionFailed, "cas precondition not met")}
	})
	c := newTestClient(t, conn)
	err := c.CompareAndSwap([]byte("k"), []byte("old"), []byte("new"))
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("err=%v want ErrPreconditionFailed", err)
	}
}

func TestClientPutIfAbsentWireShape(t *testing.T) {
	var gotReq *wire.CASRequest
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		gotReq = req.(*wire.CASRequest)
		return [][2]any{okFrame(nil)}
	})
	c := newTestClient(t, conn)
	if err := c.PutIfAbsent([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("PutIfAbsent: %v", err)
	}
	if gotReq.Mode != wire.CASIfAbsent {
		t.Fatalf("mode=%v want CASIfAbsent", gotReq.Mode)
	}
	if gotReq.Expected != nil {
		t.Fatalf("expected bytes must be nil in absent mode, got %q", gotReq.Expected)
	}
	if !bytes.Equal(gotReq.NewValue, []byte("v")) {
		t.Fatalf("new value=%q", gotReq.NewValue)
	}
}

func TestClientPutIfAbsentConflictSentinel(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		return [][2]any{errFrame(wire.StatusPreconditionFailed, "exists")}
	})
	c := newTestClient(t, conn)
	err := c.PutIfAbsent([]byte("k"), []byte("v"))
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("err=%v want ErrPreconditionFailed", err)
	}
}

// TestClientCASOtherStatusIsRemoteError ensures non-precondition
// failures are not swallowed into the sentinel.
func TestClientCASOtherStatusIsRemoteError(t *testing.T) {
	conn, _ := newMockPair(t, func(req wire.Request) [][2]any {
		return [][2]any{errFrame(wire.StatusClosed, "shutting down")}
	})
	c := newTestClient(t, conn)
	err := c.CompareAndSwap([]byte("k"), []byte("e"), []byte("v"))
	var re *wire.RemoteError
	if !errors.As(err, &re) || re.Status != wire.StatusClosed {
		t.Fatalf("err=%v want RemoteError{CLOSED}", err)
	}
	if errors.Is(err, ErrPreconditionFailed) {
		t.Fatal("CLOSED must not be reported as a precondition failure")
	}
}
