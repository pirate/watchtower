//go:build integration

package container

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cerrdefs "github.com/containerd/errdefs"
	dockerContainer "github.com/moby/moby/api/types/container"
	dockerClient "github.com/moby/moby/client"
)

// Exercise the wait's real cancellation, rather than a fake timer or transport.
// This tests the bounded recovery phase, not the full replacement pipeline.
func TestRealEngineStopRecoveryDeadline(t *testing.T) {
	host, image := os.Getenv("WATCHTOWER_TEST_DOCKER_HOST"), os.Getenv("WATCHTOWER_TEST_IMAGE")
	if host == "" || image == "" {
		t.Skip("set WATCHTOWER_TEST_DOCKER_HOST and WATCHTOWER_TEST_IMAGE")
	}

	require.True(t, strings.HasPrefix(host, "unix:///"))

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	api, err := dockerClient.New(dockerClient.WithHost(host))
	require.NoError(t, err)

	defer api.Close()

	base, err := api.ImageInspect(ctx, image)
	require.NoError(t, err, "test image must already be cached")
	require.Empty(t, base.Config.Volumes)

	name := fmt.Sprintf("watchtower-stop-bound-%d", time.Now().UnixNano())
	created, err := api.ContainerCreate(ctx, dockerClient.ContainerCreateOptions{
		Name: name,
		Config: &dockerContainer.Config{
			Image: base.ID, User: "65534:65534", Tty: true,
			Entrypoint: []string{"/bin/sh", "-c"},
			Cmd:        []string{"trap 'sleep 5; exit 0' TERM; echo READY; while :; do sleep 0.1 & wait $!; done"},
			Labels:     map[string]string{"watchtower.test.stop-recovery": name},
		},
		HostConfig: &dockerContainer.HostConfig{NetworkMode: "none", CapDrop: []string{"ALL"}},
	})
	require.NoError(t, err)

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		_, removeErr := api.ContainerRemove(cleanupCtx, created.ID, dockerClient.ContainerRemoveOptions{Force: true})
		if !cerrdefs.IsNotFound(removeErr) {
			require.NoError(t, removeErr)
		}
	}()

	_, err = api.ContainerStart(ctx, created.ID, dockerClient.ContainerStartOptions{})
	require.NoError(t, err)
	stream, err := api.ContainerLogs(ctx, created.ID, dockerClient.ContainerLogsOptions{ShowStdout: true, Follow: true})
	require.NoError(t, err)

	defer stream.Close()

	scanner := bufio.NewScanner(stream)
	require.True(t, scanner.Scan())
	require.Equal(t, "READY", strings.TrimSpace(scanner.Text()))

	owned, err := api.ContainerInspect(ctx, created.ID, dockerClient.ContainerInspectOptions{})
	require.NoError(t, err)

	stopCtx, stopCancel := context.WithTimeout(ctx, time.Second)
	defer stopCancel()

	seconds := 10
	_, err = api.ContainerStop(stopCtx, created.ID, dockerClient.ContainerStopOptions{Timeout: &seconds})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	before, err := api.ContainerInspect(ctx, created.ID, dockerClient.ContainerInspectOptions{})
	require.NoError(t, err)
	require.True(t, before.Container.State.Running, "daemon stop must still be in flight")

	recoveryCtx, recoveryCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer recoveryCancel()

	err = reconcileStoppedSource(recoveryCtx, api, &owned.Container, false)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	after, err := api.ContainerInspect(ctx, created.ID, dockerClient.ContainerInspectOptions{})
	require.NoError(t, err, "a bounded-out reconciliation must not remove the container")
	require.True(t, after.Container.State.Running)
	require.Equal(t, owned.Container.State.StartedAt, after.Container.State.StartedAt)

	// The same real daemon stop completes after this phase's deadline. It is
	// not adopted after the helper returns. This uses a short real context for
	// the helper, not a fake clock or the full CLI's production timeout policy.
	lateExit := api.ContainerWait(ctx, created.ID, dockerClient.ContainerWaitOptions{Condition: dockerContainer.WaitConditionNotRunning})
	select {
	case result := <-lateExit.Result:
		require.Nil(t, result.Error)
		require.Zero(t, result.StatusCode)
	case waitErr := <-lateExit.Error:
		require.NoError(t, waitErr)
	}

	late, err := api.ContainerInspect(ctx, created.ID, dockerClient.ContainerInspectOptions{})
	require.NoError(t, err)
	require.Equal(t, dockerContainer.StateExited, late.Container.State.Status)
	require.Equal(t, owned.Container.State.StartedAt, late.Container.State.StartedAt)
}
