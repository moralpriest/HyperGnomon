package indexer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/code"
	"github.com/creachadair/jrpc2/handler"
	jrpc2server "github.com/creachadair/jrpc2/server"
	"github.com/deroproject/derohe/rpc"
)

// fakeGetSCServer spins up an in-memory jrpc2 server that answers
// DERO.GetSC with per-SCID behavior:
//
//   - any SCID not matched below returns a valid snapshot with one variable
//   - "rpc-error" returns an RPC error (simulates an overloaded daemon)
//   - "decode-error" returns a JSON value that cannot unmarshal into
//     rpc.GetSC_Result (simulates a malformed daemon result)
//
// The returned *jrpc2.Client yields genuine batch responses (including error
// responses), so classifyRefreshResponse can be exercised against real
// jrpc2.Response objects instead of hand-built fakes.
func fakeGetSCServer(t *testing.T) *jrpc2.Client {
	t.Helper()
	loc := jrpc2server.NewLocal(handler.Map{
		"DERO.GetSC": func(ctx context.Context, req *jrpc2.Request) (any, error) {
			var p rpc.GetSC_Params
			if err := json.Unmarshal([]byte(req.ParamString()), &p); err != nil {
				return nil, jrpc2.Errorf(code.InvalidParams, "bad params: %v", err)
			}
			switch p.SCID {
			case "rpc-error":
				return nil, jrpc2.Errorf(code.InternalError, "node overloaded")
			case "decode-error":
				// A JSON string cannot unmarshal into the GetSC_Result struct.
				return json.RawMessage(`"not an object"`), nil
			default:
				// Full (Variables=true) fetches surface string keys as a map;
				// fast-path (KeysString) fetches surface the same values as an
				// ordered ValuesString slice matching the requested keys.
				if len(p.KeysString) > 0 {
					values := make([]string, len(p.KeysString))
					for i := range p.KeysString {
						values[i] = "Test App"
					}
					return rpc.GetSC_Result{ValuesString: values}, nil
				}
				return rpc.GetSC_Result{
					VariableStringKeys: map[string]any{"nameHdr": "Test App"},
				}, nil
			}
		},
	}, nil)
	t.Cleanup(func() { loc.Close() })
	return loc.Client
}

// TestClassifyRefreshResponse pins the error-classification contract that
// the refresher relies on: per-item RPC errors (daemon overloaded, SCID
// gone, rate-limited) must surface as scidErr, and only a result the daemon
// actually returned that we cannot parse may count as a decode error.
func TestClassifyRefreshResponse(t *testing.T) {
	client := fakeGetSCServer(t)

	results, err := client.Batch(context.Background(), []jrpc2.Spec{
		{Method: "DERO.GetSC", Params: rpc.GetSC_Params{SCID: "good", Variables: true}},
		{Method: "DERO.GetSC", Params: rpc.GetSC_Params{SCID: "rpc-error", Variables: true}},
		{Method: "DERO.GetSC", Params: rpc.GetSC_Params{SCID: "decode-error", Variables: true}},
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("Batch returned %d results, want 3", len(results))
	}

	// SCID 0: valid snapshot → no errors, vars extracted.
	scidErr, decodeErr, vars := classifyRefreshResponse(results[0], nil)
	if scidErr || decodeErr {
		t.Errorf("valid response classified as scidErr=%v decodeErr=%v, want neither", scidErr, decodeErr)
	}
	if len(vars) == 0 {
		t.Error("valid response produced no variables, want >= 1")
	}

	// SCID 1: RPC error → scidErr, not decodeErr.
	scidErr, decodeErr, vars = classifyRefreshResponse(results[1], nil)
	if !scidErr {
		t.Error("RPC-error response not classified as scidErr")
	}
	if decodeErr {
		t.Error("RPC-error response incorrectly classified as decodeErr")
	}
	if len(vars) != 0 {
		t.Errorf("RPC-error response produced %d vars, want 0", len(vars))
	}

	// SCID 2: malformed result → decodeErr, not scidErr.
	scidErr, decodeErr, vars = classifyRefreshResponse(results[2], nil)
	if scidErr {
		t.Error("decode-error response incorrectly classified as scidErr")
	}
	if !decodeErr {
		t.Error("decode-error response not classified as decodeErr")
	}
	if len(vars) != 0 {
		t.Errorf("decode-error response produced %d vars, want 0", len(vars))
	}
}

// TestClassifyRefreshResponseFastKeys verifies the fast-path (KeysString)
// classification path also parses values into variables when the daemon
// returns them.
func TestClassifyRefreshResponseFastKeys(t *testing.T) {
	client := fakeGetSCServer(t)

	results, err := client.Batch(context.Background(), []jrpc2.Spec{
		{Method: "DERO.GetSC", Params: rpc.GetSC_Params{SCID: "good", Variables: false, KeysString: []string{"nameHdr"}}},
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	scidErr, decodeErr, vars := classifyRefreshResponse(results[0], []string{"nameHdr"})
	if scidErr || decodeErr {
		t.Fatalf("fast-key response classified as scidErr=%v decodeErr=%v, want neither", scidErr, decodeErr)
	}
	if len(vars) == 0 {
		t.Error("fast-key response produced no variables, want >= 1")
	}
}

// TestIsRefreshOverloaded pins the overload decision boundary: exactly half
// is NOT overloaded (strict >), anything above is, and an empty class is
// never overloaded.
func TestIsRefreshOverloaded(t *testing.T) {
	cases := []struct {
		name         string
		total        int
		scidErrors   int64
		decodeErrors int64
		want         bool
	}{
		{"empty class never overloaded", 0, 0, 0, false},
		{"no failures", 114, 0, 0, false},
		{"below threshold", 114, 40, 0, false},
		{"exactly half is not overloaded", 114, 57, 0, false},
		{"just above threshold", 114, 58, 0, true},
		{"all failed via scid errors", 114, 114, 0, true},
		{"all failed via decode errors", 114, 0, 114, true},
		{"mixed errors cross threshold", 114, 40, 20, true},
		{"single scid failed", 1, 1, 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRefreshOverloaded(tc.total, tc.scidErrors, tc.decodeErrors); got != tc.want {
				t.Errorf("isRefreshOverloaded(%d, %d, %d) = %v, want %v",
					tc.total, tc.scidErrors, tc.decodeErrors, got, tc.want)
			}
		})
	}
}
