package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestCASRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		req  *CASRequest
	}{
		{
			name: "if-value basic",
			req:  &CASRequest{Key: []byte("k"), NewValue: []byte("on"), Expected: []byte("off"), Mode: CASIfValue},
		},
		{
			name: "if-value empty expected",
			req:  &CASRequest{Key: []byte("k"), NewValue: []byte("on"), Expected: []byte{}, Mode: CASIfValue},
		},
		{
			name: "if-value empty new value",
			req:  &CASRequest{Key: []byte("k"), NewValue: []byte{}, Expected: []byte("off"), Mode: CASIfValue},
		},
		{
			name: "if-absent",
			req:  &CASRequest{Key: []byte("k"), NewValue: []byte("on"), Expected: nil, Mode: CASIfAbsent},
		},
		{
			name: "binary key and values",
			req: &CASRequest{
				Key:      []byte{0x00, 0xff, 0x10},
				NewValue: bytes.Repeat([]byte{0xab}, 1000),
				Expected: bytes.Repeat([]byte{0x01}, 500),
				Mode:     CASIfValue,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := EncodeRequest(tc.req)
			if err != nil {
				t.Fatalf("EncodeRequest: %v", err)
			}
			tag, body, err := ReadFrame(bytes.NewReader(frame))
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if Op(tag) != OpCAS {
				t.Fatalf("tag=%v want OpCAS", Op(tag))
			}
			got, err := DecodeRequest(OpCAS, body)
			if err != nil {
				t.Fatalf("DecodeRequest: %v", err)
			}
			cas, ok := got.(*CASRequest)
			if !ok {
				t.Fatalf("got %T want *CASRequest", got)
			}
			if cas.Mode != tc.req.Mode ||
				!bytes.Equal(cas.Key, tc.req.Key) ||
				!bytes.Equal(cas.NewValue, tc.req.NewValue) ||
				!bytes.Equal(cas.Expected, tc.req.Expected) {
				t.Fatalf("round-trip mismatch:\nwant %#v\ngot  %#v", tc.req, cas)
			}
			if !requestsEqual(got, tc.req) {
				t.Fatalf("requestsEqual rejected matching requests")
			}
		})
	}
}

func TestCASDecodeRejectsInvalid(t *testing.T) {
	good := func() []byte {
		body, err := encodeRequestBody(&CASRequest{
			Key:      []byte("k"),
			NewValue: []byte("on"),
			Expected: []byte("off"),
			Mode:     CASIfValue,
		})
		if err != nil {
			t.Fatalf("seed encode: %v", err)
		}
		return body
	}

	cases := []struct {
		name string
		body []byte
	}{
		{"truncated", []byte{0x00, 0x00, 0x00}},
		{"empty key", func() []byte {
			b := good()
			copy(b[0:4], []byte{0, 0, 0, 0})
			return b
		}()},
		{"unknown mode", func() []byte {
			b := good()
			// mode sits right after the 4-byte klen + 1-byte key.
			b[4+1] = 0x09
			return b
		}()},
		{"if-absent with expected bytes", func() []byte {
			body, err := encodeRequestBody(&CASRequest{
				Key:      []byte("k"),
				NewValue: []byte("on"),
				Expected: []byte("surprise"),
				Mode:     CASIfAbsent,
			})
			if err == nil {
				t.Fatalf("encoding if-absent with expected bytes should fail, got body len=%d", len(body))
			}
			// Hand-build a frame the encoder refuses so the decoder is
			// tested independently: klen=1, key=k, mode=1(absent),
			// vlen=0, elen=2, expect="xx".
			b := []byte{0, 0, 0, 1, 'k', 1, 0, 0, 0, 0, 0, 0, 0, 2, 'x', 'x'}
			return b
		}()},
		{"trailing bytes", append(good(), 0x00)},
		{"body too short for declared key", func() []byte {
			b := good()
			return b[:len(b)-3]
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeRequest(OpCAS, tc.body)
			var pe *ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("err=%v want *ProtocolError", err)
			}
		})
	}
}

func TestCASEncodeRejectsBadMode(t *testing.T) {
	_, err := encodeRequestBody(&CASRequest{
		Key:      []byte("k"),
		NewValue: []byte("v"),
		Mode:     CASMode(99),
	})
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("err=%v want *ProtocolError", err)
	}
}

// TestCASStatusAndOpNames pins the stable wire names used in logs and
// the CLI's statusName mapping.
func TestCASStatusAndOpNames(t *testing.T) {
	if OpCAS.String() != "CAS" {
		t.Fatalf("OpCAS.String()=%q", OpCAS.String())
	}
	if StatusPreconditionFailed.String() != "PRECONDITION_FAILED" {
		t.Fatalf("StatusPreconditionFailed.String()=%q", StatusPreconditionFailed.String())
	}
	if uint8(StatusPreconditionFailed) != 0x06 || uint8(OpCAS) != 0x08 {
		t.Fatal("wire constants drifted from documented values")
	}
}
