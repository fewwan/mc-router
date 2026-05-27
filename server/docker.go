package server

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/sirupsen/logrus"
)

type IDockerWatcher interface {
	Start(ctx context.Context) error
}

const (
	DockerRouterLabelHost                 = "mc-router.host"
	DockerRouterLabelPort                 = "mc-router.port"
	DockerRouterLabelDefault              = "mc-router.default"
	DockerRouterLabelNetwork              = "mc-router.network"
	DockerRouterLabelAutoScaleUp          = "mc-router.auto-scale-up"
	DockerRouterLabelAutoScaleDown        = "mc-router.auto-scale-down"
	DockerRouterLabelAutoScaleAsleepMOTD  = "mc-router.auto-scale-asleep-motd"
	DockerRouterLabelAutoScaleLoadingMOTD = "mc-router.auto-scale-loading-motd"
	DockerRouterLabelAutoScaleWith        = "mc-router.auto-scale-with"
)

type dockerWatcherConfig struct {
	autoScaleUp   bool
	autoScaleDown bool
	socket        string
	timeout       time.Duration
	apiVersion    string
}

func (c *dockerWatcherConfig) apiVersionOpt() client.Opt {
	if c.apiVersion != "" {
		logrus.WithField("apiVersion", c.apiVersion).Debug("Using specific Docker API version")
		return client.WithVersion(c.apiVersion)
	} else {
		logrus.Debug("Using Docker API version negotiation")
		return client.WithAPIVersionNegotiation()
	}
}

func NewDockerWatcher(socket string, timeout time.Duration, autoScaleUp bool, autoScaleDown bool, dockerApiVersion string) IDockerWatcher {
	return &dockerWatcherImpl{
		config: dockerWatcherConfig{
			socket:        socket,
			timeout:       timeout,
			autoScaleUp:   autoScaleUp,
			autoScaleDown: autoScaleDown,
			apiVersion:    dockerApiVersion,
		},
	}
}

type dockerWatcherImpl struct {
	sync.RWMutex
	config       dockerWatcherConfig
	client       *client.Client
	containerMap map[string]*routableContainer
	sidecars     map[string]*sidecarContainer
	monitorLock  sync.Mutex
}

func (w *dockerWatcherImpl) startContainer(ctx context.Context, containerID string) error {
	inspect, err := w.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return err
	}
	if inspect.State == nil {
		return fmt.Errorf("unable to determine container state")
	}
	if inspect.State.Paused {
		logrus.WithFields(logrus.Fields{"containerID": containerID}).Debug("Unpausing container")
		if err := w.client.ContainerUnpause(ctx, containerID); err != nil {
			return err
		}
	} else if !inspect.State.Running {
		logrus.WithFields(logrus.Fields{"containerID": containerID}).Debug("Starting container")
		if err := w.client.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (w *dockerWatcherImpl) stopContainer(ctx context.Context, containerID string) error {
	inspect, err := w.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return err
	}
	if inspect.State != nil && inspect.State.Running {
		timeout := 60
		logrus.WithFields(logrus.Fields{"containerID": containerID}).Debug("Stopping container")
		if err := w.client.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout}); err != nil {
			return err
		}
	}
	return nil
}

func (w *dockerWatcherImpl) anyDependencyRunning(dependencies []string) bool {
	for _, host := range dependencies {
		if rc, ok := w.containerMap[host]; ok && rc.containerEndpoint != "" {
			logrus.WithField("host", host).Debug("Dependency is running")
			return true
		}
	}
	return false
}

func (w *dockerWatcherImpl) makeWakerFunc(rc *routableContainer) WakerFunc {
	if rc == nil || !rc.autoScaleUp {
		return nil
	}
	return func(ctx context.Context) (string, error) {
		containerID := rc.containerID
		if containerID == "" {
			return "", fmt.Errorf("missing container id for wake")
		}

		if err := w.startContainer(ctx, containerID); err != nil {
			return "", err
		}

		inspect, err := w.client.ContainerInspect(ctx, containerID)
		if err != nil {
			return "", err
		}
		data, ok := w.parseContainerData(&inspect)
		if !ok {
			return "", fmt.Errorf("failed to parse container data after starting")
		}
		if data.ip == "" {
			return "", fmt.Errorf("container has no accessible IP after starting")
		}
		endpoint := net.JoinHostPort(data.ip, strconv.Itoa(int(data.port)))

		// Route table updates via Docker `start`/`network connect` events.

		// Wait until the container is reachable
		deadline := time.Now().Add(60 * time.Second)
		for {
			conn, err := net.DialTimeout("tcp", endpoint, 1*time.Second)
			if err == nil {
				_ = conn.Close()
				break
			}
			if ctx.Err() != nil {
				return endpoint, ctx.Err()
			}
			if time.Now().After(deadline) {
				return endpoint, fmt.Errorf("timeout waiting for container to become reachable at %s", endpoint)
			}
			select {
			case <-ctx.Done():
				return endpoint, ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}

		return endpoint, nil
	}
}

func (w *dockerWatcherImpl) makeSleeperFunc(rc *routableContainer) SleeperFunc {
	if rc == nil || !rc.autoScaleDown {
		return nil
	}
	return func(ctx context.Context) error {
		containerID := rc.containerID
		if containerID == "" {
			return fmt.Errorf("missing container id for sleep")
		}
		return w.stopContainer(ctx, containerID)
	}
}

// monitorContainers does a full re-list of Docker containers and reconciles
// the route table against it. Used for initial sync at startup and for
// resync after the event stream reconnects (to catch any events missed
// during disconnect).
func (w *dockerWatcherImpl) monitorContainers(ctx context.Context) error {
	w.monitorLock.Lock()
	defer w.monitorLock.Unlock()

	logrus.Trace("Listing Docker containers")
	rcs, sidecars, err := w.listContainers(ctx)
	if err != nil {
		logrus.WithError(err).Error("Docker failed to list containers")
		return err
	}

	byID := map[string][]*routableContainer{}
	for _, rc := range rcs {
		byID[rc.containerID] = append(byID[rc.containerID], rc)
	}
	sidecarsByID := map[string]*sidecarContainer{}
	for _, s := range sidecars {
		sidecarsByID[s.containerID] = s
	}

	// We need to iterate over ALL IDs that were found (either as rcs or sidecars)
	allIDs := map[string]bool{}
	for id := range byID {
		allIDs[id] = true
	}
	for id := range sidecarsByID {
		allIDs[id] = true
	}

	for id := range allIDs {
		w.applyContainerRoutesLocked(ctx, id, byID[id], sidecarsByID[id])
	}

	// Remove entries whose container is no longer present at all
	for name, rc := range w.containerMap {
		if _, present := allIDs[rc.containerID]; present {
			continue
		}
		delete(w.containerMap, name)
		if name != "" {
			Routes.DeleteMapping(name)
		} else {
			Routes.SetDefaultRoute("", "", nil, nil, "", "")
		}
		logrus.WithField("routableContainer", rc).Debug("DELETE")
	}

	// Remove sidecars that are no longer present
	for id := range w.sidecars {
		if !allIDs[id] {
			delete(w.sidecars, id)
		}
	}

	return nil
}

// applyEvent reacts to a single Docker event by reconciling only the routes
// belonging to the affected container — no full re-list.
func (w *dockerWatcherImpl) applyEvent(ctx context.Context, ev events.Message) error {
	containerID := ev.Actor.ID
	if ev.Type == events.NetworkEventType {
		containerID = ev.Actor.Attributes["container"]
	}
	if containerID == "" {
		logrus.WithField("event", ev).Warn("network event missing container attribute, skipping")
		return nil
	}

	var desired []*routableContainer
	var sidecar *sidecarContainer
	if !(ev.Type == events.ContainerEventType && ev.Action == events.ActionDestroy) {
		got, sc, err := w.containersForID(ctx, containerID)
		if err != nil {
			return err
		}
		desired = got
		sidecar = sc
	}

	w.monitorLock.Lock()
	defer w.monitorLock.Unlock()

	// Only trace events that affect a routed container — either one we already
	// track or one becoming routable now. Filters out unrelated daemon noise.
	relevant := len(desired) > 0 || sidecar != nil
	if !relevant {
		for _, rc := range w.containerMap {
			if rc.containerID == containerID {
				relevant = true
				break
			}
		}
	}
	if !relevant {
		if _, exists := w.sidecars[containerID]; exists {
			relevant = true
		}
	}
	if relevant {
		logrus.WithFields(logrus.Fields{"type": ev.Type, "action": ev.Action, "id": containerID}).Trace("Docker event")
	}

	w.applyContainerRoutesLocked(ctx, containerID, desired, sidecar)
	return nil
}

// containersForID inspects a single container and returns the routableContainers
// it should produce. Returns nil if the container is gone or not routable.
func (w *dockerWatcherImpl) containersForID(ctx context.Context, containerID string) ([]*routableContainer, *sidecarContainer, error) {
	inspect, err := w.client.ContainerInspect(ctx, containerID)
	if err != nil {
		if cerrdefs.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	data, ok := w.parseContainerData(&inspect)
	if !ok {
		return nil, nil, nil
	}
	endpoint := ""
	if !data.notRunning {
		endpoint = fmt.Sprintf("%s:%d", data.ip, data.port)
	}
	var result []*routableContainer
	for _, host := range data.hosts {
		result = append(result, &routableContainer{
			containerEndpoint:     endpoint,
			externalContainerName: host,
			containerID:           containerID,
			autoScaleUp:           data.autoScaleUp,
			autoScaleDown:         data.autoScaleDown,
			autoScaleAsleepMOTD:   data.autoScaleAsleepMOTD,
			autoScaleLoadingMOTD:  data.autoScaleLoadingMOTD,
		})
	}
	if data.def != nil && *data.def {
		result = append(result, &routableContainer{
			containerEndpoint:     endpoint,
			externalContainerName: "",
			containerID:           containerID,
			autoScaleUp:           data.autoScaleUp,
			autoScaleDown:         data.autoScaleDown,
			autoScaleAsleepMOTD:   data.autoScaleAsleepMOTD,
			autoScaleLoadingMOTD:  data.autoScaleLoadingMOTD,
		})
	}
	var sidecar *sidecarContainer
	if len(data.autoScaleWith) > 0 {
		sidecar = &sidecarContainer{
			containerID:   containerID,
			autoScaleWith: data.autoScaleWith,
		}
	}
	return result, sidecar, nil
}

// applyContainerRoutesLocked reconciles the routes for a single containerID
// against the desired set. Caller must hold monitorLock.
func (w *dockerWatcherImpl) applyContainerRoutesLocked(ctx context.Context, containerID string, desired []*routableContainer, sidecar *sidecarContainer) {
	desiredByName := map[string]*routableContainer{}
	for _, rc := range desired {
		desiredByName[rc.externalContainerName] = rc
	}

	var hostsAffected []string

	// Drop entries previously owned by this container that are no longer desired
	for name, rc := range w.containerMap {
		if rc.containerID != containerID {
			continue
		}
		if _, keep := desiredByName[name]; keep {
			continue
		}
		delete(w.containerMap, name)
		if name != "" {
			Routes.DeleteMapping(name)
		} else {
			Routes.SetDefaultRoute("", "", nil, nil, "", "")
		}
		logrus.WithField("routableContainer", rc).Debug("DELETE")
		hostsAffected = append(hostsAffected, name)
	}

	for _, rs := range desired {
		oldRs, exists := w.containerMap[rs.externalContainerName]
		if !exists {
			w.containerMap[rs.externalContainerName] = rs
			wakerFunc := w.makeWakerFunc(rs)
			sleeperFunc := w.makeSleeperFunc(rs)
			if rs.externalContainerName != "" {
				Routes.CreateMapping(rs.externalContainerName, rs.containerEndpoint, "", wakerFunc, sleeperFunc, rs.autoScaleAsleepMOTD, rs.autoScaleLoadingMOTD)
			} else {
				Routes.SetDefaultRoute(rs.containerEndpoint, "", wakerFunc, sleeperFunc, rs.autoScaleAsleepMOTD, rs.autoScaleLoadingMOTD)
			}
			logrus.WithField("routableContainer", rs).Debug("ADD")
			hostsAffected = append(hostsAffected, rs.externalContainerName)
			continue
		}
		if oldRs.containerEndpoint == rs.containerEndpoint &&
			oldRs.containerID == rs.containerID &&
			oldRs.autoScaleUp == rs.autoScaleUp &&
			oldRs.autoScaleDown == rs.autoScaleDown &&
			oldRs.autoScaleAsleepMOTD == rs.autoScaleAsleepMOTD &&
			oldRs.autoScaleLoadingMOTD == rs.autoScaleLoadingMOTD {
			continue
		}
		w.containerMap[rs.externalContainerName] = rs
		wakerFunc := w.makeWakerFunc(rs)
		sleeperFunc := w.makeSleeperFunc(rs)
		if rs.externalContainerName != "" {
			Routes.DeleteMapping(rs.externalContainerName)
			Routes.CreateMapping(rs.externalContainerName, rs.containerEndpoint, "", wakerFunc, sleeperFunc, rs.autoScaleAsleepMOTD, rs.autoScaleLoadingMOTD)
		} else {
			Routes.SetDefaultRoute(rs.containerEndpoint, "", wakerFunc, sleeperFunc, rs.autoScaleAsleepMOTD, rs.autoScaleLoadingMOTD)
		}
		logrus.WithFields(logrus.Fields{"old": oldRs, "new": rs}).Debug("UPDATE")
		hostsAffected = append(hostsAffected, rs.externalContainerName)
	}

	// Update sidecars map
	if sidecar != nil {
		w.sidecars[containerID] = sidecar
	} else {
		delete(w.sidecars, containerID)
	}

	// Process sidecars affected by any host state changes
	if len(hostsAffected) > 0 {
		w.RLock()
		affectedSidecars := make(map[string]*sidecarContainer)
		for sid, s := range w.sidecars {
			for _, h := range s.autoScaleWith {
				for _, affected := range hostsAffected {
					if h == affected {
						affectedSidecars[sid] = s
						break
					}
				}
				if _, ok := affectedSidecars[sid]; ok {
					break
				}
			}
		}
		w.RUnlock()

		for sid, s := range affectedSidecars {
			if w.anyDependencyRunning(s.autoScaleWith) {
				_ = w.startContainer(ctx, sid)
			} else {
				_ = w.stopContainer(ctx, sid)
			}
		}
	}
}

func (w *dockerWatcherImpl) Start(ctx context.Context) error {
	var err error

	opts := []client.Opt{
		client.FromEnv,
		client.WithTimeout(w.config.timeout),
		client.WithHTTPHeaders(map[string]string{
			"User-Agent": "mc-router ",
		}),
		w.config.apiVersionOpt(),
	}
	if w.config.socket != "" {
		opts = append(opts, client.WithHost(w.config.socket))
	}

	w.client, err = client.NewClientWithOpts(opts...)
	if err != nil {
		return err
	}
	w.containerMap = map[string]*routableContainer{}
	w.sidecars = map[string]*sidecarContainer{}

	logrus.Trace("Performing initial listing of Docker containers")
	if err := w.monitorContainers(ctx); err != nil {
		return err
	}

	// streamEvents will resync on (re)connect and otherwise apply incremental
	// updates from the Docker event stream — no periodic polling.
	go w.streamEvents(ctx)

	logrus.Info("Monitoring Docker for Minecraft containers")
	return nil
}

// streamEvents subscribes to the Docker event stream and triggers reconciliation
// of routes whenever container or network events relevant to routing occur.
// Reconnects with backoff on stream errors (e.g. daemon restart).
func (w *dockerWatcherImpl) streamEvents(ctx context.Context) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			logrus.Debug("Stopping Docker monitoring")
			return
		}

		eventFilters := filters.NewArgs(
			filters.Arg("type", string(events.ContainerEventType)),
			filters.Arg("type", string(events.NetworkEventType)),
			filters.Arg("event", string(events.ActionStart)),
			filters.Arg("event", string(events.ActionUnPause)),
			filters.Arg("event", string(events.ActionStop)),
			filters.Arg("event", string(events.ActionDie)),
			filters.Arg("event", string(events.ActionPause)),
			filters.Arg("event", string(events.ActionDestroy)),
			filters.Arg("event", string(events.ActionRename)),
			filters.Arg("event", string(events.ActionConnect)),
			filters.Arg("event", string(events.ActionDisconnect)),
		)

		eventCh, errCh := w.client.Events(ctx, events.ListOptions{Filters: eventFilters})

		// Resync after (re)connecting in case we missed events while disconnected
		if err := w.monitorContainers(ctx); err != nil {
			logrus.WithError(err).Error("Docker resync failed")
		} else {
			backoff = time.Second
		}

	loop:
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-eventCh:
				if !ok {
					break loop
				}
				if err := w.applyEvent(ctx, ev); err != nil {
					logrus.WithError(err).Error("Docker event handling failed")
				}
			case err, ok := <-errCh:
				if !ok {
					break loop
				}
				if ctx.Err() != nil {
					return
				}
				logrus.WithError(err).Warn("Docker event stream error, reconnecting")
				break loop
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (w *dockerWatcherImpl) listContainers(ctx context.Context) ([]*routableContainer, []*sidecarContainer, error) {
	containers, err := w.client.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return nil, nil, err
	}

	var result []*routableContainer
	var sidecars []*sidecarContainer
	for _, container := range containers {
		rcs, sidecar, err := w.containersForID(ctx, container.ID)
		if err != nil {
			logrus.WithFields(logrus.Fields{"containerID": container.ID}).WithError(err).Error("Failed to get routable info for Docker container")
			continue
		}
		result = append(result, rcs...)
		if sidecar != nil {
			sidecars = append(sidecars, sidecar)
		}
	}

	return result, sidecars, nil
}

type parsedDockerContainerData struct {
	hosts                []string
	port                 uint64
	def                  *bool
	network              *string
	ip                   string
	autoScaleDown        bool
	autoScaleUp          bool
	autoScaleAsleepMOTD  string
	autoScaleLoadingMOTD string
	autoScaleWith        []string
	notRunning           bool
}

type sidecarContainer struct {
	containerID   string
	autoScaleWith []string
}

func (w *dockerWatcherImpl) parseContainerData(container *container.InspectResponse) (data parsedDockerContainerData, ok bool) {
	data.autoScaleUp = w.config.autoScaleUp
	data.autoScaleDown = w.config.autoScaleDown
	for key, value := range container.Config.Labels {
		if key == DockerRouterLabelHost {
			if data.hosts != nil {
				logrus.WithFields(logrus.Fields{"containerId": container.ID, "containerNames": container.Name}).
					Warnf("ignoring container with duplicate %s label", DockerRouterLabelHost)
				return
			}
			data.hosts = SplitExternalHosts(value)
		}

		if key == DockerRouterLabelPort {
			if data.port != 0 {
				logrus.WithFields(logrus.Fields{"containerId": container.ID, "containerNames": container.Name}).
					Warnf("ignoring container with duplicate %s label", DockerRouterLabelPort)
				return
			}
			var err error
			data.port, err = strconv.ParseUint(value, 10, 32)
			if err != nil {
				logrus.WithFields(logrus.Fields{"containerId": container.ID, "containerNames": container.Name}).
					WithError(err).
					Warnf("ignoring container with invalid %s label", DockerRouterLabelPort)
				return
			}
		}
		if key == DockerRouterLabelDefault {
			if data.def != nil {
				logrus.WithFields(logrus.Fields{"containerId": container.ID, "containerNames": container.Name}).
					Warnf("ignoring container with duplicate %s label", DockerRouterLabelDefault)
				return
			}
			defaultValue, err := strconv.ParseBool(strings.TrimSpace(value))
			if err != nil {
				logrus.WithFields(logrus.Fields{"containerId": container.ID, "containerNames": container.Name}).
					WithError(err).
					Warnf("ignoring container with invalid value for %s label", DockerRouterLabelDefault)
				return
			}
			data.def = &defaultValue
		}
		if key == DockerRouterLabelNetwork {
			if data.network != nil {
				logrus.WithFields(logrus.Fields{"containerId": container.ID, "containerNames": container.Name}).
					Warnf("ignoring container with duplicate %s label", DockerRouterLabelNetwork)
				return
			}
			data.network = new(string)
			*data.network = value
		}
		if key == DockerRouterLabelAutoScaleUp {
			autoScaleUp, err := strconv.ParseBool(strings.TrimSpace(value))
			if err != nil {
				logrus.WithFields(logrus.Fields{"containerId": container.ID, "containerNames": container.Name}).
					WithError(err).
					Warnf("ignoring container with invalid value for %s label", DockerRouterLabelAutoScaleUp)
				return
			}
			data.autoScaleUp = autoScaleUp
		}
		if key == DockerRouterLabelAutoScaleDown {
			autoScaleDown, err := strconv.ParseBool(strings.TrimSpace(value))
			if err != nil {
				logrus.WithFields(logrus.Fields{"containerId": container.ID, "containerNames": container.Name}).
					WithError(err).
					Warnf("ignoring container with invalid value for %s label", DockerRouterLabelAutoScaleDown)
				return
			}
			data.autoScaleDown = autoScaleDown
		}
		if key == DockerRouterLabelAutoScaleAsleepMOTD {
			data.autoScaleAsleepMOTD = value
		}
		if key == DockerRouterLabelAutoScaleLoadingMOTD {
			data.autoScaleLoadingMOTD = value
		}
		if key == DockerRouterLabelAutoScaleWith {
			data.autoScaleWith = SplitExternalHosts(value)
		}
	}

	// probably not minecraft related
	if len(data.hosts) == 0 && len(data.autoScaleWith) == 0 {
		return
	}

	if len(container.NetworkSettings.Networks) == 0 {
		logrus.WithFields(logrus.Fields{"containerId": container.ID, "containerNames": container.Name}).
			Warnf("ignoring container, no networks found")
		return
	}

	if data.port == 0 {
		data.port = 25565
	}

	if data.network != nil {
		// Loop through all the container's networks and attempt to find one whose Network ID, Name, or Aliases match the
		// specified network
		for name, endpoint := range container.NetworkSettings.Networks {
			if name == endpoint.NetworkID {
				data.ip = endpoint.IPAddress
			}

			if name == *data.network {
				data.ip = endpoint.IPAddress
				break
			}

			for _, alias := range endpoint.Aliases {
				if alias == name {
					data.ip = endpoint.IPAddress
					break
				}
			}
		}
	} else {
		// If there's no endpoint specified we can just assume the only one is the network we should use. One caveat is
		// if there's more than one network on this container, we should require that the user specifies a network to avoid
		// weird problems.
		if len(container.NetworkSettings.Networks) > 1 {
			logrus.WithFields(logrus.Fields{"containerId": container.ID, "containerNames": container.Name}).
				Warnf("ignoring container, multiple networks found and none specified using label %s", DockerRouterLabelNetwork)
			return
		}

		for _, endpoint := range container.NetworkSettings.Networks {
			data.ip = endpoint.IPAddress
			break
		}
	}

	if data.ip == "" && container.State != nil && container.State.Running {
		logrus.WithFields(logrus.Fields{"containerId": container.ID, "containerNames": container.Name}).
			Warnf("ignoring container, unable to find accessible ip address")
		return
	}

	if container.State != nil && !container.State.Running {
		if !w.config.autoScaleUp {
			logrus.WithFields(logrus.Fields{"containerId": container.ID, "containerNames": container.Name}).
				Debugf("ignoring container, not running and auto scale up is disabled")
			return
		}
		data.notRunning = true
	}

	ok = true

	return
}

type routableContainer struct {
	externalContainerName string
	containerEndpoint     string
	containerID           string
	autoScaleUp           bool
	autoScaleDown         bool
	autoScaleAsleepMOTD   string
	autoScaleLoadingMOTD  string
}
