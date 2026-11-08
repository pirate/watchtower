package client

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	dockerContainer "github.com/moby/moby/api/types/container"
	dockerNetwork "github.com/moby/moby/api/types/network"
	dockerClient "github.com/moby/moby/client"
)

var (
	errStopTargetChanged = errors.New("container changed before its stop completed; refusing stale replacement")
	errWaitStopFailed    = errors.New("wait for uncertain stop failed")
)

// hasStopGeneration requires enough inspect data to identify the exact process
// generation and configuration. An incomplete inspect must not enable recovery.
func hasStopGeneration(info *dockerContainer.InspectResponse) bool {
	if info == nil || info.ID == "" || info.Name == "" || info.State == nil || info.Config == nil || info.HostConfig == nil {
		return false
	}

	started, err := time.Parse(time.RFC3339Nano, info.State.StartedAt)

	return err == nil && !started.IsZero()
}

// sameStopTarget ignores state fields expected to change during a normal stop,
// but not process generation, identity, name, or user-controlled configuration.
// Both arguments are raw Engine inspections, so this does not depend on another
// container still existing to resolve its namespace or volumes-from reference.
// Transient network addresses are not compared.
func sameStopTarget(before, now *dockerContainer.InspectResponse) bool {
	return hasStopGeneration(before) && hasStopGeneration(now) &&
		before.ID == now.ID && before.Name == now.Name && before.Image == now.Image &&
		before.State.StartedAt == now.State.StartedAt &&
		before.RestartCount == now.RestartCount &&
		reflect.DeepEqual(before.Config, now.Config) &&
		reflect.DeepEqual(before.HostConfig, now.HostConfig) &&
		sameStopNetworks(before.NetworkSettings, now.NetworkSettings)
}

// sameStopNetworks compares membership and user-controlled endpoint options.
// Runtime addresses, endpoint IDs, and generated DNS names change at shutdown.
// A requested static address remains in IPAMConfig and is not ignored.
func sameStopNetworks(before, now *dockerContainer.NetworkSettings) bool {
	if before == nil || now == nil {
		return before == now
	}

	if len(before.Networks) != len(now.Networks) {
		return false
	}

	configuration := func(endpoint *dockerNetwork.EndpointSettings) *dockerNetwork.EndpointSettings {
		if endpoint == nil {
			return nil
		}

		return &dockerNetwork.EndpointSettings{
			NetworkID: endpoint.NetworkID, IPAMConfig: endpoint.IPAMConfig, Links: endpoint.Links,
			Aliases: endpoint.Aliases, DriverOpts: endpoint.DriverOpts, GwPriority: endpoint.GwPriority,
		}
	}

	for name, previous := range before.Networks {
		current, exists := now.Networks[name]
		if !exists || !reflect.DeepEqual(configuration(previous), configuration(current)) {
			return false
		}
	}

	return true
}

// sameScannedStopTarget checks the scan against a fresh raw inspection. The
// scan normalizes namespace and volumes-from references from IDs to names.
// Those two fields cannot be changed on an existing Docker container, so the
// exact ID already protects them and we omit their normalized representation
// here. Every mutable HostConfig field is still compared. Recovery compares
// two raw inspections and omits nothing from HostConfig.
func sameScannedStopTarget(scanned, fresh *dockerContainer.InspectResponse) bool {
	if !hasStopGeneration(scanned) || !hasStopGeneration(fresh) {
		return false
	}

	left, right := *scanned, *fresh
	leftHost, rightHost := *scanned.HostConfig, *fresh.HostConfig
	leftHost.NetworkMode, rightHost.NetworkMode = "", ""
	leftHost.VolumesFrom, rightHost.VolumesFrom = nil, nil
	left.HostConfig, right.HostConfig = &leftHost, &rightHost

	return sameStopTarget(&left, &right)
}

// uncertainStopError excludes definitive rejection, absence, and state conflicts.
// Transport/deadline errors and server failures may arrive after a stop began.
func uncertainStopError(err error) bool {
	return !cerrdefs.IsNotFound(err) && !cerrdefs.IsInvalidArgument(err) &&
		!cerrdefs.IsPermissionDenied(err) && !cerrdefs.IsUnauthorized(err) &&
		!cerrdefs.IsConflict(err) && !cerrdefs.IsFailedPrecondition(err) &&
		!cerrdefs.IsNotImplemented(err)
}

// reconcileStoppedSource finishes only an already-issued stop of owned. It does
// not send another signal, enumerate stopped containers, or retain work for a
// later update session. The caller supplies a bounded, live recovery context.
func reconcileStoppedSource(
	ctx context.Context,
	api dockerClient.APIClient,
	owned *dockerContainer.InspectResponse,
	removeVolumes bool,
) error {
	inspect := func() (*dockerContainer.InspectResponse, error) {
		fresh, err := api.ContainerInspect(ctx, owned.ID, dockerClient.ContainerInspectOptions{})
		if err != nil {
			return nil, fmt.Errorf("inspect uncertain stop: %w", err)
		}

		if !sameStopTarget(owned, &fresh.Container) {
			return nil, errStopTargetChanged
		}

		return &fresh.Container, nil
	}

	fresh, err := inspect()
	if err != nil {
		return err
	}

	if fresh.State.Running {
		wait := api.ContainerWait(ctx, owned.ID, dockerClient.ContainerWaitOptions{
			Condition: dockerContainer.WaitConditionNotRunning,
		})
		select {
		case result := <-wait.Result:
			if result.Error != nil {
				return fmt.Errorf("%w: %s", errWaitStopFailed, result.Error.Message)
			}
		case waitErr := <-wait.Error:
			return fmt.Errorf("wait for uncertain stop: %w", waitErr)
		}

		fresh, err = inspect()
		if err != nil {
			return err
		}
	}
	// A restarted, paused, dead, or otherwise unresolved container is not a
	// completed graceful/forced exit of this operation's original process.
	info := fresh
	if info.State.Running || info.State.Restarting || info.State.Paused || info.State.Dead || info.State.Status != dockerContainer.StateExited {
		return errStopTargetChanged
	}
	// Inspection and removal are not atomic: later operator changes can race.
	// Force=false makes a concurrent restart reject removal instead of killing
	// the new running process. A missing target is not recreated from old data.
	_, err = api.ContainerRemove(ctx, owned.ID, dockerClient.ContainerRemoveOptions{RemoveVolumes: removeVolumes})
	if err != nil {
		return fmt.Errorf("remove container after confirmed stop: %w", err)
	}

	return nil
}
