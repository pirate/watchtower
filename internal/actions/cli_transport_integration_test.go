//go:build integration

package actions

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// cliRequestTrace retains no headers, bodies, or query strings. Name checks
// retain only booleans for the real unnamed-create, then exact-rename contract.
type cliRequestTrace struct {
	Method        string
	Path          string
	CreateUnnamed bool
	RenameScoped  bool
}

// realCLIRelay forwards raw bytes to the actual Engine. The observer parses a
// copy of request bytes and discards bodies; it never manufactures responses,
// changes requests, blocks selected operations, or substitutes an API client.
type realCLIRelay struct {
	listener    net.Listener
	cancel      context.CancelFunc
	accepted    chan struct{}
	connections sync.WaitGroup
	finishOnce  sync.Once
	mu          sync.Mutex
	requests    []cliRequestTrace
	complete    bool
	problem     bool
	host        string
}

func startRealCLIRelay(t *testing.T, ctx context.Context, realHost, fixtureName string) *realCLIRelay {
	t.Helper()
	require.True(t, strings.HasPrefix(realHost, "unix:///"))
	// macOS Unix socket names are short; the standard test directory can exceed
	// that limit. This separately captured private directory is removed below.
	dir, err := os.MkdirTemp("", "wt-relay-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })

	path := filepath.Join(dir, "engine.sock")

	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "unix", path)
	require.NoError(t, err)

	relayCtx, cancel := context.WithCancel(ctx)
	r := &realCLIRelay{listener: listener, cancel: cancel, accepted: make(chan struct{}), host: "unix://" + path}

	t.Cleanup(r.finish)
	go func() {
		defer close(r.accepted)

		for {
			front, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			r.connections.Add(1)
			go r.forward(relayCtx, front, strings.TrimPrefix(realHost, "unix://"), fixtureName)
		}
	}()

	return r
}

func (r *realCLIRelay) forward(ctx context.Context, front net.Conn, enginePath, fixtureName string) {
	defer r.connections.Done()
	defer front.Close()

	var dialer net.Dialer

	back, err := dialer.DialContext(ctx, "unix", enginePath)
	if err != nil {
		r.markProblem()

		return
	}
	defer back.Close()

	stop := context.AfterFunc(ctx, func() { _ = front.Close(); _ = back.Close() })
	defer stop()

	readCopy, writeCopy := io.Pipe()

	parsed := make(chan struct{})
	go func() {
		defer close(parsed)
		defer readCopy.Close()

		r.observe(bufio.NewReader(readCopy), fixtureName)
	}()

	responses := make(chan struct{})
	go func() {
		defer close(responses)

		_, _ = io.Copy(front, back)
		_ = front.Close()
	}()
	// Every request byte handed to the backend passes through the observer.
	// Returning from the parser early makes the tee fail, and marks the trace
	// incomplete rather than silently losing attribution evidence.
	_, _ = io.Copy(back, io.TeeReader(front, writeCopy))
	_ = writeCopy.Close()
	_ = back.Close()

	<-parsed
	<-responses
}

func (r *realCLIRelay) observe(reader *bufio.Reader, fixtureName string) {
	for {
		request, err := http.ReadRequest(reader)
		if err == io.EOF {
			return
		}

		if err != nil {
			r.markProblem()

			return
		}

		name := request.URL.Query().Get("name")
		entry := cliRequestTrace{Method: request.Method, Path: request.URL.Path, CreateUnnamed: name == "", RenameScoped: name == fixtureName}

		r.mu.Lock()
		r.requests = append(r.requests, entry)
		r.mu.Unlock()

		_, err = io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()

		if err != nil {
			r.markProblem()

			return
		}
	}
}

func (r *realCLIRelay) markProblem() {
	r.mu.Lock()
	r.problem = true
	r.mu.Unlock()
}

func (r *realCLIRelay) finish() {
	r.finishOnce.Do(func() {
		_ = r.listener.Close()
		<-r.accepted
		r.cancel()
		r.connections.Wait()
		r.mu.Lock()
		r.complete = !r.problem
		r.mu.Unlock()
	})
}

func (r *realCLIRelay) assertOwnedRequests(t *testing.T, f *realStopFixture) {
	t.Helper()
	r.finish()
	require.True(t, r.complete, "the passive real-transport request trace must be complete")
	require.NotEmpty(t, r.requests, "the actual CLI must have used the traced real transport")

	owned := map[string]bool{f.id: true}
	for _, id := range f.ids {
		owned[id] = true
	}

	creates, renames := 0, 0

	for _, request := range r.requests {
		t.Logf("Actual CLI request: %s %s", request.Method, request.Path)

		if request.Method == http.MethodGet || request.Method == http.MethodHead {
			continue
		}

		path := strings.TrimPrefix(request.Path, "/")

		parts := strings.Split(path, "/")
		if len(parts) > 0 && strings.HasPrefix(parts[0], "v") && strings.Contains(parts[0], ".") {
			parts = parts[1:]
		}

		require.GreaterOrEqual(t, len(parts), 2, "reject global mutating endpoint: %s %s", request.Method, request.Path)
		require.Equal(t, "containers", parts[0], "reject non-container mutating endpoint: %s %s", request.Method, request.Path)

		if request.Method == http.MethodPost && len(parts) == 2 && parts[1] == "create" {
			require.True(t, request.CreateUnnamed, "the real recreation path must create with an empty name")

			creates++

			continue
		}

		require.True(t, owned[parts[1]], "mutating request targets a non-test container: %s %s", request.Method, request.Path)

		if request.Method == http.MethodPost && len(parts) == 3 && parts[2] == "rename" {
			require.NotEqual(t, f.id, parts[1], "only the independently verified replacement may be renamed")
			require.True(t, request.RenameScoped, "replacement must be renamed to the exact fixture name")

			renames++

			continue
		}

		allowed := request.Method == http.MethodDelete && len(parts) == 2
		if len(parts) == 3 {
			allowed = (request.Method == http.MethodPost && (parts[2] == "stop" || parts[2] == "start" || parts[2] == "wait")) ||
				(request.Method == http.MethodPut && parts[2] == "archive")
		}

		require.True(t, allowed, "unexpected mutating endpoint: %s %s", request.Method, request.Path)
	}

	// Create has no target ID or name in this production path. Match its count
	// to the independently inspected, owner-labeled replacement, and require
	// exactly one name-scoped rename for each. The separate ambient Engine
	// event assertion also rejects any non-owned create, even if later removed.
	wantCreates := len(owned) - 1
	require.Equal(t, wantCreates, creates, "every create must account for one verified replacement")
	require.Equal(t, wantCreates, renames, "every replacement must receive the exact fixture name")
}
