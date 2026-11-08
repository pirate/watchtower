//go:build integration

package restart

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	cerrdefs "github.com/containerd/errdefs"
	dockerContainer "github.com/moby/moby/api/types/container"
	dockerNetwork "github.com/moby/moby/api/types/network"
	dockerClient "github.com/moby/moby/client"

	containerClient "github.com/nicholas-fedor/watchtower/internal/container/client"
	updateConfig "github.com/nicholas-fedor/watchtower/internal/settings/config/update"
	"github.com/nicholas-fedor/watchtower/internal/update/cleanup"
	updateParams "github.com/nicholas-fedor/watchtower/internal/update/params"
	"github.com/nicholas-fedor/watchtower/internal/update/staleness"
	"github.com/nicholas-fedor/watchtower/internal/update/state"
	"github.com/nicholas-fedor/watchtower/pkg/types"
)

// This suite uses a real, explicitly selected local Docker Engine and an
// already-cached image with /bin/sh. It never pulls, publishes ports, mounts
// anything, or changes Docker contexts. No mock clients or clocks are used.
//
// WATCHTOWER_TEST_DOCKER_HOST=unix:///path/to/local/docker.sock \
// WATCHTOWER_TEST_IMAGE=ghcr.io/astral-sh/uv:python3.13-bookworm-slim \
// WATCHTOWER_TEST_UPDATE_IMAGE=ghcr.io/astral-sh/uv:python3.14-bookworm-slim \
// go test -tags=integration ./internal/container/client ./internal/update/restart -run TestRealEngine -timeout=180s -v.
func TestRealEngineLateStopCompletion(t *testing.T) {
	for _, test := range []struct {
		name         string
		params       updateParams.Params
		cancelOnTerm bool
		network      bool
	}{
		{name: "batch deadline"},
		{name: "network deadline", network: true},
		{name: "rolling deadline", params: updateParams.Params{RollingRestart: true}},
		{name: "no restart", params: updateParams.Params{NoRestart: true}},
		{name: "whole session canceled after stop", cancelOnTerm: true},
	} {
		t.Run(test.name, func(t *testing.T) { testRealLateStop(t, test.params, test.cancelOnTerm, test.network) })
	}
}

// This covers real local image comparison and the replacement phase, not CLI
// startup or scheduling. It never scans or acts on unrelated engine workloads.
func testRealLateStop(t *testing.T, params updateParams.Params, cancelOnTerm, network bool) {
	t.Helper()

	var configure []func(*dockerClient.ContainerCreateOptions)

	if network {
		networkID := realIsolatedNetwork(t)

		configure = append(configure, func(options *dockerClient.ContainerCreateOptions) {
			options.HostConfig.NetworkMode = dockerContainer.NetworkMode(networkID)
		})
	}

	f, ctx := newRealStopFixture(t, configure...)
	params.Timeout = 10 * time.Second
	source, progress := f.prepareUpdate(t, ctx, params)
	images := []cleanup.RemovedImageInfo{}

	// Register the real Engine wait before issuing stop, so removal cannot hide
	// the original process's exit from the test.
	wait := f.api.ContainerWait(ctx, f.id, dockerClient.ContainerWaitOptions{
		Condition: dockerContainer.WaitConditionNotRunning,
	})
	exit := make(chan dockerContainer.WaitResponse, 1)
	exitErr := make(chan error, 1)

	go func() {
		select {
		case result := <-wait.Result:
			exit <- result
		case err := <-wait.Error:
			exitErr <- err
		}
	}()

	stopCtx, cancel := context.WithTimeout(ctx, time.Second)
	if cancelOnTerm {
		cancel()
		stopCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- Run(&f.log, stopCtx, []types.Container{source}, f.client, params, &images, &progress, nil)
	}()

	select {
	case <-f.term:
	case <-ctx.Done():
		t.Fatal("real container never acknowledged SIGTERM")
	}

	if cancelOnTerm {
		cancel()
	}

	<-stopCtx.Done()

	if cancelOnTerm {
		require.ErrorIs(t, stopCtx.Err(), context.Canceled)
	} else {
		require.ErrorIs(t, stopCtx.Err(), context.DeadlineExceeded)
	}

	atDeadline, err := f.api.ContainerInspect(ctx, f.id, dockerClient.ContainerInspectOptions{})
	require.NoError(t, err)
	require.True(t, atDeadline.Container.State.Running, "the real stop must still be in flight at the client deadline")

	select {
	case result := <-exit:
		require.Nil(t, result.Error)
		require.Zero(t, result.StatusCode, "fixture must exit gracefully, not through SIGKILL")
	case err := <-exitErr:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("real container never completed its delayed graceful exit")
	}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("replacement did not finish")
	}

	require.Contains(t, f.logs.String(), "Failed to stop container", "prove the actual stop request failed, rather than merely taking the ordinary success path")

	replacement, err := f.api.ContainerInspect(ctx, f.name, dockerClient.ContainerInspectOptions{})
	require.NoError(t, err)
	f.capture(t, ctx, replacement.Container.ID)
	require.NotEqual(t, f.id, replacement.Container.ID, "an uncertain stop must not strand the old exited container")
	require.Equal(t, !params.NoRestart, replacement.Container.State.Running)
	require.Equal(t, f.newImageID, replacement.Container.Image, "replacement must actually run the newly tagged real image")
	require.Equal(t, types.ContainerID(replacement.Container.ID), progress[source.ID()].NewContainerID())

	_, err = f.api.ContainerInspect(ctx, f.id, dockerClient.ContainerInspectOptions{})
	require.True(t, cerrdefs.IsNotFound(err), "the owned old container should be removed only after its exit")
	require.Equal(t, f.payload, f.readFile(t, ctx, replacement.Container.ID, "/tmp/watchtower-copy.txt"))
	require.Contains(t, f.logs.String(), "Recovered delayed container stop")
}

func TestRealEngineStopRecoveryRespectsOperatorChanges(t *testing.T) {
	for _, change := range []string{"rename", "resources", "network membership", "network aliases", "network identity", "remove and replace"} {
		t.Run(change, func(t *testing.T) {
			var (
				initialNetwork, otherNetwork string
				configure                    []func(*dockerClient.ContainerCreateOptions)
			)

			if strings.HasPrefix(change, "network") {
				initialNetwork = realIsolatedNetwork(t, change == "network identity")
				otherNetwork = realIsolatedNetwork(t)

				configure = append(configure, func(options *dockerClient.ContainerCreateOptions) {
					options.HostConfig.NetworkMode = dockerContainer.NetworkMode(initialNetwork)
				})
			}

			f, ctx := newRealStopFixture(t, configure...)
			params := updateParams.Params{Timeout: 10 * time.Second}
			source, progress := f.prepareUpdate(t, ctx, params)
			before, err := f.api.ContainerInspect(ctx, f.id, dockerClient.ContainerInspectOptions{})
			require.NoError(t, err)

			stopCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()

			done := make(chan error, 1)

			go func() {
				images := []cleanup.RemovedImageInfo{}
				done <- Run(&f.log, stopCtx, []types.Container{source}, f.client, params, &images, &progress, nil)
			}()

			select {
			case <-f.term:
			case <-ctx.Done():
				t.Fatal("stop was never issued")
			}

			<-stopCtx.Done()

			manualID := ""

			switch change {
			case "rename":
				_, err = f.api.ContainerRename(ctx, f.id, dockerClient.ContainerRenameOptions{NewName: f.name + "-operator"})
			case "resources":
				_, err = f.api.ContainerUpdate(ctx, f.id, dockerClient.ContainerUpdateOptions{Resources: &dockerContainer.Resources{CPUShares: 512}})
			case "network membership":
				_, err = f.api.NetworkConnect(ctx, otherNetwork, dockerClient.NetworkConnectOptions{Container: f.id})
			case "network aliases":
				_, err = f.api.NetworkDisconnect(ctx, initialNetwork, dockerClient.NetworkDisconnectOptions{Container: f.id})
				require.NoError(t, err)
				_, err = f.api.NetworkConnect(ctx, initialNetwork, dockerClient.NetworkConnectOptions{
					Container: f.id, EndpointConfig: &dockerNetwork.EndpointSettings{Aliases: []string{"operator-alias"}},
				})
			case "network identity":
				networkInfo, inspectErr := f.api.NetworkInspect(ctx, initialNetwork, dockerClient.NetworkInspectOptions{})
				require.NoError(t, inspectErr)

				_, err = f.api.NetworkDisconnect(ctx, initialNetwork, dockerClient.NetworkDisconnectOptions{Container: f.id})
				require.NoError(t, err)
				_, err = f.api.NetworkRemove(ctx, initialNetwork, dockerClient.NetworkRemoveOptions{})
				require.NoError(t, err)

				created, createErr := f.api.NetworkCreate(ctx, networkInfo.Network.Name, dockerClient.NetworkCreateOptions{Driver: "bridge", Internal: true})
				require.NoError(t, createErr)
				require.NotEqual(t, networkInfo.Network.ID, created.ID)
				t.Cleanup(func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cleanupCancel()

					_, _ = f.api.NetworkDisconnect(cleanupCtx, created.ID, dockerClient.NetworkDisconnectOptions{Container: f.id})
					_, removeErr := f.api.NetworkRemove(cleanupCtx, created.ID, dockerClient.NetworkRemoveOptions{})
					require.NoError(t, removeErr)
				})

				_, err = f.api.NetworkConnect(ctx, created.ID, dockerClient.NetworkConnectOptions{Container: f.id})
			case "remove and replace":
				_, err = f.api.ContainerRemove(ctx, f.id, dockerClient.ContainerRemoveOptions{Force: true})
				require.NoError(t, err)

				created, createErr := f.api.ContainerCreate(ctx, dockerClient.ContainerCreateOptions{
					Name: f.name, Config: before.Container.Config, HostConfig: before.Container.HostConfig,
				})
				require.NoError(t, createErr)

				manualID = created.ID
				f.capture(t, ctx, manualID)
				_, err = f.api.ContainerStart(ctx, manualID, dockerClient.ContainerStartOptions{})
			}

			require.NoError(t, err)

			if strings.HasPrefix(change, "network") {
				after, inspectErr := f.api.ContainerInspect(ctx, f.id, dockerClient.ContainerInspectOptions{})
				require.NoError(t, inspectErr)
				require.True(t, reflect.DeepEqual(before.Container.HostConfig, after.Container.HostConfig), "exercise NetworkSettings-only changes")
			}

			select {
			case err = <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("reconciliation did not finish")
			}

			require.Empty(t, progress[source.ID()].NewContainerID())
			require.Contains(t, f.logs.String(), "Failed to stop container")

			if manualID != "" {
				manual, inspectErr := f.api.ContainerInspect(ctx, f.name, dockerClient.ContainerInspectOptions{})
				require.NoError(t, inspectErr)
				require.Equal(t, manualID, manual.Container.ID)
				require.True(t, manual.Container.State.Running)
			} else {
				kept, inspectErr := f.api.ContainerInspect(ctx, f.id, dockerClient.ContainerInspectOptions{})
				require.NoError(t, inspectErr, "operator-modified container must not be removed")
				require.Equal(t, source.ContainerInfo().State.StartedAt, kept.Container.State.StartedAt)
				require.Equal(t, f.payload, f.readFile(t, ctx, f.id, "/tmp/watchtower-copy.txt"))
			}
		})
	}
}

func TestRealEngineStopRecoveryDoesNotTakeOverNewIntent(t *testing.T) {
	for _, action := range []string{"canceled before stop", "manually stopped after scan", "new process generation"} {
		t.Run(action, func(t *testing.T) {
			f, ctx := newRealStopFixture(t)
			params := updateParams.Params{Timeout: 10 * time.Second}
			source, progress := f.prepareUpdate(t, ctx, params)

			updateCtx := ctx
			if action == "canceled before stop" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()

				updateCtx = canceled
			} else {
				seconds := 10
				_, err := f.api.ContainerStop(ctx, f.id, dockerClient.ContainerStopOptions{Timeout: &seconds})
				require.NoError(t, err)

				if action == "new process generation" {
					_, err = f.api.ContainerStart(ctx, f.id, dockerClient.ContainerStartOptions{})
					require.NoError(t, err)
				}
			}

			images := []cleanup.RemovedImageInfo{}
			require.NoError(t, Run(&f.log, updateCtx, []types.Container{source}, f.client, params, &images, &progress, nil))
			current, err := f.api.ContainerInspect(ctx, f.id, dockerClient.ContainerInspectOptions{})
			require.NoError(t, err)
			require.Empty(t, progress[source.ID()].NewContainerID())
			require.Equal(t, action != "manually stopped after scan", current.Container.State.Running)

			if action == "new process generation" {
				require.NotEqual(t, source.ContainerInfo().State.StartedAt, current.Container.State.StartedAt)
			}
		})
	}
}

// The scan resolves namespace/volumes-from IDs to names. In rolling mode,
// stopping the consumer must not require the already-replaced provider ID.
// This is the restart phase's documented dependency-order contract; it does
// not bypass or claim coverage of the CLI's dependency planning restrictions.
func TestRealEngineDependencyReferences(t *testing.T) {
	for _, dependency := range []string{"network namespace", "volumes from"} {
		t.Run(dependency, func(t *testing.T) {
			provider, ctx := newRealStopFixture(t)
			consumer, _ := newRealStopFixture(t, func(options *dockerClient.ContainerCreateOptions) {
				if dependency == "network namespace" {
					options.HostConfig.NetworkMode = dockerContainer.NetworkMode("container:" + provider.id)
				} else {
					options.HostConfig.VolumesFrom = []string{provider.id + ":ro"}
				}
			})
			params := updateParams.Params{Timeout: 10 * time.Second, RollingRestart: true}
			first, progress := provider.prepareUpdate(t, ctx, params)
			second, consumerProgress := consumer.prepareUpdate(t, ctx, params)
			progress[second.ID()] = consumerProgress[second.ID()]
			images := []cleanup.RemovedImageInfo{}
			require.NoError(t, Run(&provider.log, ctx, []types.Container{first, second}, provider.client, params, &images, &progress, nil))

			for _, f := range []*realStopFixture{provider, consumer} {
				current, err := f.api.ContainerInspect(ctx, f.name, dockerClient.ContainerInspectOptions{})
				require.NoError(t, err)
				f.capture(t, ctx, current.Container.ID)
				require.NotEqual(t, f.id, current.Container.ID, provider.logs.String())
				require.True(t, current.Container.State.Running)
				require.Empty(t, current.Container.Mounts, "the dependency fixture must not create real mounts")
				require.Equal(t, f.newImageID, current.Container.Image)
				require.Equal(t, f.payload, f.readFile(t, ctx, current.Container.ID, "/tmp/watchtower-copy.txt"))
				require.Equal(t, types.ContainerID(current.Container.ID), progress[types.ContainerID(f.id)].NewContainerID())
			}
		})
	}
}

func TestRealEngineAutoRemoveStop(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprintf("uncertain=%t", uncertain), func(t *testing.T) {
			f, ctx := newRealStopFixture(t, func(options *dockerClient.ContainerCreateOptions) { options.HostConfig.AutoRemove = true })
			params := updateParams.Params{Timeout: 10 * time.Second}
			source, progress := f.prepareUpdate(t, ctx, params)
			updateCtx := ctx

			removed := f.api.ContainerWait(ctx, f.id, dockerClient.ContainerWaitOptions{Condition: dockerContainer.WaitConditionRemoved})
			if uncertain {
				deadlineCtx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()

				updateCtx = deadlineCtx
			}

			images := []cleanup.RemovedImageInfo{}
			require.NoError(t, Run(&f.log, updateCtx, []types.Container{source}, f.client, params, &images, &progress, nil))

			if uncertain {
				// With the old container gone, identity/configuration cannot be
				// revalidated. Do not turn absence into authority to resurrect it.
				require.Empty(t, progress[source.ID()].NewContainerID())
				require.Contains(t, f.logs.String(), "Failed to stop container")
			}
			// AutoRemove continues asynchronously after the process exit event.
			// Await the Engine's removal event, not a timing-sensitive inspect.
			select {
			case result := <-removed.Result:
				require.Nil(t, result.Error)
			case err := <-removed.Error:
				require.NoError(t, err)
			}

			current, err := f.api.ContainerInspect(ctx, f.name, dockerClient.ContainerInspectOptions{})
			if uncertain {
				require.True(t, cerrdefs.IsNotFound(err))
			} else {
				require.NoError(t, err)
				f.capture(t, ctx, current.Container.ID)
				require.NotEqual(t, f.id, current.Container.ID)
				require.True(t, current.Container.State.Running)
				require.Equal(t, f.newImageID, current.Container.Image)
			}
		})
	}
}

func realIsolatedNetwork(t *testing.T, named ...bool) string {
	t.Helper()

	host := os.Getenv("WATCHTOWER_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("set WATCHTOWER_TEST_DOCKER_HOST")
	}

	require.True(t, strings.HasPrefix(host, "unix:///"))
	api, err := dockerClient.New(dockerClient.WithHost(host))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, api.Close()) })

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	name := fmt.Sprintf("watchtower-stop-network-%d", time.Now().UnixNano())
	created, err := api.NetworkCreate(ctx, name, dockerClient.NetworkCreateOptions{Driver: "bridge", Internal: true, Labels: map[string]string{"watchtower.test.stop-recovery": name}})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()

		_, removeErr := api.NetworkRemove(cleanupCtx, created.ID, dockerClient.NetworkRemoveOptions{})
		if !cerrdefs.IsNotFound(removeErr) {
			require.NoError(t, removeErr)
		}
	})

	if len(named) > 0 && named[0] {
		return name
	}

	return created.ID
}

type realStopFixture struct {
	api        *dockerClient.Client
	client     containerClient.Client
	log        zerolog.Logger
	logs       bytes.Buffer
	id         string
	name       string
	token      string
	payload    string
	tag        string
	newImageID string
	term       chan struct{}
	ids        []string
}

func newRealStopFixture(t *testing.T, configure ...func(*dockerClient.ContainerCreateOptions)) (*realStopFixture, context.Context) {
	t.Helper()

	host := os.Getenv("WATCHTOWER_TEST_DOCKER_HOST")
	image := os.Getenv("WATCHTOWER_TEST_IMAGE")

	updateImage := os.Getenv("WATCHTOWER_TEST_UPDATE_IMAGE")
	if host == "" || image == "" || updateImage == "" {
		t.Skip("set WATCHTOWER_TEST_DOCKER_HOST, WATCHTOWER_TEST_IMAGE and WATCHTOWER_TEST_UPDATE_IMAGE to run the real-engine suite")
	}

	require.True(t, strings.HasPrefix(host, "unix:///"), "integration tests require an explicit local Unix socket")
	t.Setenv("DOCKER_HOST", host)
	t.Setenv("DOCKER_API_VERSION", "")
	t.Setenv("DOCKER_TLS_VERIFY", "")
	t.Setenv("DOCKER_CERT_PATH", "")
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	t.Cleanup(cancel)

	api, err := dockerClient.New(dockerClient.WithHost(host))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, api.Close()) })

	base, err := api.ImageInspect(ctx, image)
	require.NoError(t, err, "test image must already be cached; this suite never pulls")
	next, err := api.ImageInspect(ctx, updateImage)
	require.NoError(t, err, "updated test image must already be cached; this suite never pulls")
	require.Empty(t, base.Config.Volumes, "base image must not create implicit mounts")
	require.Empty(t, next.Config.Volumes, "updated image must not create implicit mounts")
	require.NotEqual(t, base.ID, next.ID)

	token := strconv.FormatInt(time.Now().UnixNano(), 10)
	f := &realStopFixture{api: api, token: token, name: "watchtower-stop-test-" + token, payload: "copy-file-payload-" + token, term: make(chan struct{})}
	f.tag, f.newImageID = "watchtower-stop-test:"+token, next.ID
	_, err = api.ImageTag(ctx, dockerClient.ImageTagOptions{Source: base.ID, Target: f.tag})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()

		_, removeErr := api.ImageRemove(cleanupCtx, f.tag, dockerClient.ImageRemoveOptions{})
		require.NoError(t, removeErr, "remove only this test-owned tag, never the shared image")
	})

	f.log = zerolog.New(zerolog.SyncWriter(&f.logs))
	f.client, err = containerClient.NewClient(ctx, &f.log, containerClient.ClientOptions{})
	require.NoError(t, err)

	options := dockerClient.ContainerCreateOptions{
		Name: f.name,
		Config: &dockerContainer.Config{
			Image:      f.tag,
			User:       "65534:65534",
			Entrypoint: []string{"/bin/sh", "-c"},
			Cmd:        []string{"trap 'echo TERM; sleep 5; exit 0' TERM; echo READY; while :; do sleep 0.1 & wait $!; done"},
			Tty:        true,
			Labels: map[string]string{
				"watchtower.test.stop-recovery":            token,
				"com.centurylinklabs.watchtower.copy-file": "/tmp/watchtower-copy.txt",
			},
		},
		HostConfig: &dockerContainer.HostConfig{NetworkMode: "none", CapDrop: []string{"ALL"}},
	}
	for _, apply := range configure {
		apply(&options)
	}

	created, err := api.ContainerCreate(ctx, options)
	require.NoError(t, err)

	f.id = created.ID
	f.ids = append(f.ids, created.ID)

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		// A replacement can be created before an assertion fails. Capture only
		// this fixture's exact name with its unique ownership label.
		current, inspectErr := api.ContainerInspect(cleanupCtx, f.name, dockerClient.ContainerInspectOptions{})
		if inspectErr == nil && current.Container.Config.Labels["watchtower.test.stop-recovery"] == token {
			f.ids = append(f.ids, current.Container.ID)
		}

		for _, id := range f.ids {
			_, removeErr := api.ContainerRemove(cleanupCtx, id, dockerClient.ContainerRemoveOptions{Force: true})
			if cerrdefs.IsConflict(removeErr) {
				wait := api.ContainerWait(cleanupCtx, id, dockerClient.ContainerWaitOptions{Condition: dockerContainer.WaitConditionRemoved})
				select {
				case result := <-wait.Result:
					require.Nil(t, result.Error)
				case waitErr := <-wait.Error:
					if !cerrdefs.IsNotFound(waitErr) {
						require.NoError(t, waitErr)
					}
				}
			} else if !cerrdefs.IsNotFound(removeErr) {
				require.NoError(t, removeErr)
			}
		}
	})
	f.copyFile(t, ctx, f.id, "watchtower-copy.txt", f.payload)
	_, err = api.ContainerStart(ctx, f.id, dockerClient.ContainerStartOptions{})
	require.NoError(t, err)
	stream, err := api.ContainerLogs(ctx, f.id, dockerClient.ContainerLogsOptions{ShowStdout: true, Follow: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })

	ready := make(chan struct{})

	go func() {
		scanner := bufio.NewScanner(stream)
		for scanner.Scan() {
			switch strings.TrimSpace(scanner.Text()) {
			case "READY":
				close(ready)
			case "TERM":
				close(f.term)
			}
		}
	}()

	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("test container did not become ready")
	}

	return f, ctx
}

// prepareUpdate derives update intent from Watchtower's real no-pull image
// comparison, after moving only the fixture's private tag to a different real
// image. The old container's copied file is absent from that new base image.
func (f *realStopFixture) prepareUpdate(t *testing.T, ctx context.Context, params updateParams.Params) (types.Container, state.Progress) {
	t.Helper()

	source, err := f.client.GetContainer(ctx, types.ContainerID(f.id))
	require.NoError(t, err)
	_, err = f.api.ImageTag(ctx, dockerClient.ImageTagOptions{Source: f.newImageID, Target: f.tag})
	require.NoError(t, err)
	stale, latest, _, err := f.client.IsContainerStale(ctx, source, updateConfig.Policy{NoPull: true})
	require.NoError(t, err)
	require.True(t, stale)
	require.Equal(t, types.ImageID(f.newImageID), latest)

	shouldUpdate := staleness.ShouldUpdate(source, stale, params)
	require.True(t, shouldUpdate)
	require.NoError(t, source.VerifyConfiguration())
	source.SetStale(stale && shouldUpdate)

	progress := state.Progress{}
	progress.AddScanned(&f.log, source, latest, params)

	return source, progress
}

func (f *realStopFixture) capture(t *testing.T, ctx context.Context, id string) {
	t.Helper()

	inspect, err := f.api.ContainerInspect(ctx, id, dockerClient.ContainerInspectOptions{})
	require.NoError(t, err)
	require.Equal(t, f.token, inspect.Container.Config.Labels["watchtower.test.stop-recovery"])
	f.ids = append(f.ids, id)
}

func (f *realStopFixture) copyFile(t *testing.T, ctx context.Context, id, name, content string) {
	t.Helper()

	var data bytes.Buffer

	w := tar.NewWriter(&data)
	require.NoError(t, w.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Uid: 65534, Gid: 65534, Size: int64(len(content))}))
	_, err := io.WriteString(w, content)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	_, err = f.api.CopyToContainer(ctx, id, dockerClient.CopyToContainerOptions{DestinationPath: "/tmp", Content: &data})
	require.NoError(t, err)
}

func (f *realStopFixture) readFile(t *testing.T, ctx context.Context, id, path string) string {
	t.Helper()

	result, err := f.api.CopyFromContainer(ctx, id, dockerClient.CopyFromContainerOptions{SourcePath: path})
	require.NoError(t, err)

	defer result.Content.Close()

	r := tar.NewReader(result.Content)
	_, err = r.Next()
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)

	return string(data)
}
