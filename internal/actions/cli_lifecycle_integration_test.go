//go:build integration

package actions

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cerrdefs "github.com/containerd/errdefs"
	dockerContainer "github.com/moby/moby/api/types/container"
	dockerClient "github.com/moby/moby/client"
)

// These tests launch the real executable, including CLI configuration, normal
// image discovery, scheduler, signal handling, and replacement. They require
// the same explicitly selected local Engine and cached images as the phase
// tests. They do not use overridden production functions or fake transports.
func TestRealEngineCLILifecycle(t *testing.T) {
	binary := buildRealCLI(t)
	for _, mode := range []string{"run once", "no restart", "scheduled SIGTERM", "idle SIGTERM"} {
		t.Run(mode, func(t *testing.T) {
			began := time.Now()
			f, ctx := newRealStopFixture(t, cliScope)

			before := cliBoundary(t, ctx, f)
			if mode != "idle SIGTERM" {
				_, err := f.api.ImageTag(ctx, dockerClient.ImageTagOptions{Source: f.newImageID, Target: f.tag})
				require.NoError(t, err)
			}

			args := []string{"--run-once"}

			switch mode {
			case "no restart":
				args = append(args, "--no-restart")
			case "scheduled SIGTERM":
				args = []string{"--interval", "1"}
			case "idle SIGTERM":
				args = []string{"--interval", "3600"}
			}

			process := startRealCLI(t, ctx, binary, f, args...)

			switch mode {
			case "scheduled SIGTERM":
				select {
				case <-f.term:
				case <-ctx.Done():
					t.Fatal("the scheduled update never sent TERM to its target")
				}

				require.NoError(t, process.command.Process.Signal(syscall.SIGTERM))

				stillRunning, err := f.api.ContainerInspect(ctx, f.id, dockerClient.ContainerInspectOptions{})
				require.NoError(t, err)
				require.True(t, stillRunning.Container.State.Running)
			case "idle SIGTERM":
				require.Eventually(t, func() bool {
					data, err := os.ReadFile(process.logPath)

					return err == nil && strings.Contains(string(data), "Next scheduled run:")
				}, 10*time.Second, 20*time.Millisecond, "the actual scheduler must initialize before the signal")
				require.NoError(t, process.command.Process.Signal(syscall.SIGTERM))
			}

			process.wait(t, ctx)
			output := process.logs(t)
			t.Logf("Actual CLI output:\n%s", output)

			current, err := f.api.ContainerInspect(ctx, f.name, dockerClient.ContainerInspectOptions{})
			require.NoError(t, err)
			f.capture(t, ctx, current.Container.ID)

			if mode == "idle SIGTERM" {
				require.Equal(t, f.id, current.Container.ID)
				require.True(t, current.Container.State.Running)
				require.NotContains(t, output, "Found new image")
			} else {
				require.NotEqual(t, f.id, current.Container.ID)
				require.Equal(t, f.newImageID, current.Container.Image)
				require.Equal(t, mode != "no restart", current.Container.State.Running)
				require.Contains(t, output, "Found new image")

				_, err = f.api.ContainerInspect(ctx, f.id, dockerClient.ContainerInspectOptions{})
				require.True(t, cerrdefs.IsNotFound(err))
			}

			require.Equal(t, f.payload, f.readCopiedFile(t, ctx, current.Container.ID))

			if mode == "scheduled SIGTERM" {
				require.Contains(t, output, "Failed to stop container")
				require.Contains(t, output, "Recovered delayed container stop")
				require.NotContains(t, output, "Timeout waiting for running update")
			}

			process.relay.assertOwnedRequests(t, f)
			assertCLIBoundary(t, ctx, f, before, began)
		})
	}
}

// This is an explicit known-limit test, not a claim of automatic recovery.
// Opt in separately because it intentionally exercises the real 60s process
// shutdown bound. No shorter fake clock or modified production timeout is used.
func TestRealEngineCLIShutdownBound(t *testing.T) {
	if os.Getenv("WATCHTOWER_TEST_LONG_SHUTDOWN") != "1" {
		t.Skip("set WATCHTOWER_TEST_LONG_SHUTDOWN=1 for the real >60-second shutdown case")
	}

	binary := buildRealCLI(t)
	began := time.Now()
	f, ctx := newRealStopFixtureWithTimeout(t, 110*time.Second, cliScope, func(options *dockerClient.ContainerCreateOptions) {
		options.Config.Cmd = []string{"trap 'echo TERM; sleep 75; exit 0' TERM; echo READY; while :; do sleep 0.1 & wait $!; done"}
		seconds := 90
		options.Config.StopTimeout = &seconds
	})
	before := cliBoundary(t, ctx, f)
	_, err := f.api.ImageTag(ctx, dockerClient.ImageTagOptions{Source: f.newImageID, Target: f.tag})
	require.NoError(t, err)

	stopped := f.api.ContainerWait(ctx, f.id, dockerClient.ContainerWaitOptions{Condition: dockerContainer.WaitConditionNotRunning})

	process := startRealCLI(t, ctx, binary, f, "--interval", "1")
	select {
	case <-f.term:
	case <-ctx.Done():
		t.Fatal("the real scheduled update never stopped its target")
	}

	signaledAt := time.Now()

	require.NoError(t, process.command.Process.Signal(syscall.SIGTERM))
	process.wait(t, ctx)
	require.GreaterOrEqual(t, time.Since(signaledAt), 60*time.Second, "exercise the full existing shutdown grace, not an earlier process exit")
	t.Logf("Actual CLI shutdown output:\n%s", process.logs(t))
	require.Contains(t, process.logs(t), "Timeout waiting for running update to finish, proceeding with shutdown.")
	require.Contains(t, process.logs(t), "Failed to stop container")
	require.NotContains(t, process.logs(t), "Recovered delayed container stop")

	stillRunning, err := f.api.ContainerInspect(ctx, f.id, dockerClient.ContainerInspectOptions{})
	require.NoError(t, err)
	require.True(t, stillRunning.Container.State.Running, "the process must exit before the genuinely delayed target")

	select {
	case result := <-stopped.Result:
		require.Nil(t, result.Error)
		require.Zero(t, result.StatusCode, "the target must finish gracefully, not via SIGKILL")
	case waitErr := <-stopped.Error:
		require.NoError(t, waitErr)
	}

	late, err := f.api.ContainerInspect(ctx, f.name, dockerClient.ContainerInspectOptions{})
	require.NoError(t, err)
	require.Equal(t, f.id, late.Container.ID)
	require.Equal(t, dockerContainer.StateExited, late.Container.State.Status)
	// A subsequent real invocation must not turn this into general revival of
	// stopped containers. Without durable intent it cannot adopt this attempt.
	later := startRealCLI(t, ctx, binary, f, "--run-once")
	later.wait(t, ctx)
	t.Logf("Subsequent actual CLI output:\n%s", later.logs(t))

	unchanged, err := f.api.ContainerInspect(ctx, f.name, dockerClient.ContainerInspectOptions{})
	require.NoError(t, err)
	require.Equal(t, f.id, unchanged.Container.ID)
	require.Equal(t, dockerContainer.StateExited, unchanged.Container.State.Status)
	require.Equal(t, f.payload, f.readCopiedFile(t, ctx, f.id))
	process.relay.assertOwnedRequests(t, f)
	later.relay.assertOwnedRequests(t, f)
	assertCLIBoundary(t, ctx, f, before, began)
}

func cliScope(options *dockerClient.ContainerCreateOptions) {
	options.Config.Labels["com.centurylinklabs.watchtower.scope"] = options.Name
	options.Config.Labels["com.centurylinklabs.watchtower.enable"] = "true"
}

func buildRealCLI(t *testing.T) string {
	t.Helper()

	if os.Getenv("WATCHTOWER_TEST_DOCKER_HOST") == "" || os.Getenv("WATCHTOWER_TEST_IMAGE") == "" || os.Getenv("WATCHTOWER_TEST_UPDATE_IMAGE") == "" {
		t.Skip("set the explicit real-engine host and cached image variables")
	}

	binary := filepath.Join(t.TempDir(), "watchtower")
	build := exec.CommandContext(t.Context(), "go", "build", "-race", "-o", binary, "../..")
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))

	return binary
}

type realCLIProcess struct {
	command *exec.Cmd
	done    chan struct{}
	logPath string
	relay   *realCLIRelay
	err     error
}

func startRealCLI(t *testing.T, ctx context.Context, binary string, f *realStopFixture, extra ...string) *realCLIProcess {
	t.Helper()
	// Daemon startup performs orphan cleanup outside the monitoring scope.
	// Refuse to run rather than expose any unrelated orchestrator to that path.
	filters := make(dockerClient.Filters)
	filters.Add("label", "com.centurylinklabs.watchtower.ephemeral-orchestrator"+"=true")
	orchestrators, err := f.api.ContainerList(ctx, dockerClient.ContainerListOptions{All: true, Filters: filters})
	require.NoError(t, err)

	orchestratorCount := len(orchestrators.Items)
	require.Zero(t, orchestratorCount, "refusing CLI startup with unrelated ephemeral orchestrators present")

	current, err := f.api.ContainerInspect(ctx, f.id, dockerClient.ContainerInspectOptions{})
	require.NoError(t, err)
	require.Equal(t, f.name, current.Container.Config.Labels["com.centurylinklabs.watchtower.scope"])
	require.Equal(t, "true", current.Container.Config.Labels["com.centurylinklabs.watchtower.enable"])
	require.Equal(t, f.token, current.Container.Config.Labels["watchtower.test.stop-recovery"])
	relay := startRealCLIRelay(t, ctx, os.Getenv("WATCHTOWER_TEST_DOCKER_HOST"), f.name)
	work := t.TempDir()
	path := filepath.Join(work, "watchtower.log")
	logFile, err := os.Create(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, logFile.Close()) })

	args := make([]string, 0, 13+len(extra))
	args = append(args, "--no-pull", "--enable-lifecycle-hooks=false", "--label-enable", "--scope", f.name, "--stop-timeout", "10s", "--log-level", "info", "--log-format", "json", "--no-color")
	args = append(args, extra...)
	args = append(args, f.name)
	command := exec.CommandContext(ctx, binary, args...)
	command.Dir = work
	// Do not inherit notifications, registry credentials, hooks, proxy setup,
	// user config paths, or other Watchtower/Docker settings into this process.
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + work, "DOCKER_CONFIG=" + work, "DOCKER_HOST=" + relay.host, "NO_COLOR=1"}
	command.Stdout, command.Stderr = logFile, logFile
	require.NoError(t, command.Start())

	process := &realCLIProcess{command: command, done: make(chan struct{}), logPath: path, relay: relay}
	go func() {
		process.err = command.Wait()
		close(process.done)
	}()

	t.Cleanup(func() {
		select {
		case <-process.done:
		default:
			_ = command.Process.Kill()

			<-process.done
		}

		relay.finish()
		t.Logf("Retained actual CLI output (including failed cases):\n%s", process.logs(t))

		for _, request := range relay.requests {
			t.Logf("Retained CLI request: %s %s", request.Method, request.Path)
		}

		t.Logf("Passive trace complete: %t", relay.complete)
	})

	return process
}

func (p *realCLIProcess) wait(t *testing.T, ctx context.Context) {
	t.Helper()

	select {
	case <-p.done:
		require.NoError(t, p.err, p.logs(t))
	case <-ctx.Done():
		t.Fatalf("CLI did not finish: %s", p.logs(t))
	}
}

func (p *realCLIProcess) logs(t *testing.T) string {
	t.Helper()

	data, err := os.ReadFile(p.logPath)
	require.NoError(t, err)

	return string(data)
}

// Retain only ordinary running-container metadata, never configs or payloads.
func cliBoundary(t *testing.T, ctx context.Context, f *realStopFixture) map[string]string {
	t.Helper()

	listed, err := f.api.ContainerList(ctx, dockerClient.ContainerListOptions{})
	require.NoError(t, err)

	result := make(map[string]string)

	for _, item := range listed.Items {
		if item.Labels["watchtower.test.stop-recovery"] != f.token {
			result[item.ID] = fmt.Sprintf("%s/%s/%s", item.ImageID, item.State, strings.Join(item.Names, ","))
		}
	}

	return result
}

func assertCLIBoundary(t *testing.T, ctx context.Context, f *realStopFixture, before map[string]string, began time.Time) {
	t.Helper()
	require.Equal(t, before, cliBoundary(t, ctx, f), "unrelated running-container metadata changed")

	filters := make(dockerClient.Filters)
	filters.Add("type", "container")

	for _, action := range []string{"create", "destroy", "rename", "update", "kill", "start", "stop", "die", "pause", "unpause", "restart"} {
		filters.Add("event", action)
	}

	events := f.api.Events(ctx, dockerClient.EventsListOptions{Since: began.Format(time.RFC3339Nano), Until: time.Now().Add(time.Second).Format(time.RFC3339Nano), Filters: filters})
	seenInitialStart := false

	for {
		select {
		case event := <-events.Messages:
			require.Equal(t, f.token, event.Actor.Attributes["watchtower.test.stop-recovery"], "non-test container mutation: ID=%s action=%s", event.Actor.ID, event.Action)

			if event.Actor.ID == f.id && event.Action == "start" {
				seenInitialStart = true
			}
		case eventErr := <-events.Err:
			require.ErrorIs(t, eventErr, io.EOF, "event stream failed: %v", eventErr)
			require.True(t, seenInitialStart, "initial real start must remain in history, proving the tested event window was not truncated")

			return
		}
	}
}
