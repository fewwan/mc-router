package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDockerWatcherClientOpts(t *testing.T) {
	t.Run("uses Docker host from environment when no socket is configured", func(t *testing.T) {
		t.Setenv("DOCKER_HOST", "tcp://docker.example:2376")

		cli, err := client.NewClientWithOpts((&dockerWatcherConfig{}).clientOpts()...)
		require.NoError(t, err)
		assert.Equal(t, "tcp://docker.example:2376", cli.DaemonHost())
	})

	t.Run("explicit socket overrides environment", func(t *testing.T) {
		t.Setenv("DOCKER_HOST", "tcp://docker.example:2376")

		config := &dockerWatcherConfig{socket: "unix:///var/run/custom.sock"}
		cli, err := client.NewClientWithOpts(config.clientOpts()...)
		require.NoError(t, err)
		assert.Equal(t, "unix:///var/run/custom.sock", cli.DaemonHost())
	})
}

func TestDockerEndpointIP(t *testing.T) {
	t.Run("prefers IPv4 when both exist", func(t *testing.T) {
		endpoint := &network.EndpointSettings{
			IPAddress:         "10.0.0.10",
			GlobalIPv6Address: "fd00::10",
		}
		assert.Equal(t, "10.0.0.10", dockerEndpointIP(endpoint))
	})

	t.Run("falls back to IPv6 when IPv4 absent", func(t *testing.T) {
		endpoint := &network.EndpointSettings{
			GlobalIPv6Address: "fd00::20",
		}
		assert.Equal(t, "fd00::20", dockerEndpointIP(endpoint))
	})

	t.Run("returns empty when endpoint missing", func(t *testing.T) {
		assert.Empty(t, dockerEndpointIP(nil))
	})
}

func TestDockerBackendEndpoint(t *testing.T) {
	assert.Equal(t, "10.0.0.10:25565", dockerBackendEndpoint("10.0.0.10", 25565))
	assert.Equal(t, "[fd00::20]:25565", dockerBackendEndpoint("fd00::20", 25565))
}

func TestDockerWatcherParseContainerData_IPSelection(t *testing.T) {
	makeInspect := func(labels map[string]string, networks map[string]*network.EndpointSettings) *container.InspectResponse {
		return &container.InspectResponse{
			Config: &container.Config{
				Labels: labels,
			},
			NetworkSettings: &container.NetworkSettings{
				Networks: networks,
			},
			ContainerJSONBase: &container.ContainerJSONBase{
				ID:   "abc123",
				Name: "example",
				State: &container.State{
					Running: true,
				},
			},
		}
	}

	w := &dockerWatcherImpl{}

	t.Run("uses ipv4 for single-network container", func(t *testing.T) {
		inspect := makeInspect(
			map[string]string{
				DockerRouterLabelHost: "mc.example.com",
			},
			map[string]*network.EndpointSettings{
				"v6net": {
					IPAddress:         "172.20.0.2",
					GlobalIPv6Address: "fd00::2",
				},
			},
		)

		data, ok := w.parseContainerData(inspect)
		require.True(t, ok)
		assert.Equal(t, "172.20.0.2", data.ip)
		assert.Equal(t, uint64(25565), data.port)
	})

	t.Run("uses ipv6 for single-network container when ipv4 absent", func(t *testing.T) {
		inspect := makeInspect(
			map[string]string{
				DockerRouterLabelHost: "mc.example.com",
			},
			map[string]*network.EndpointSettings{
				"v6net": {
					GlobalIPv6Address: "fd00::2",
				},
			},
		)

		data, ok := w.parseContainerData(inspect)
		require.True(t, ok)
		assert.Equal(t, "fd00::2", data.ip)
	})

	t.Run("uses ipv6 for selected network when network label is set", func(t *testing.T) {
		inspect := makeInspect(
			map[string]string{
				DockerRouterLabelHost:    "mc.example.com",
				DockerRouterLabelNetwork: "v6net",
			},
			map[string]*network.EndpointSettings{
				"other": {
					IPAddress: "172.20.0.5",
				},
				"v6net": {
					GlobalIPv6Address: "fd00::6",
				},
			},
		)

		data, ok := w.parseContainerData(inspect)
		require.True(t, ok)
		assert.Equal(t, "fd00::6", data.ip)
	})

	t.Run("rejects container with no routable ip while running", func(t *testing.T) {
		inspect := makeInspect(
			map[string]string{
				DockerRouterLabelHost: "mc.example.com",
			},
			map[string]*network.EndpointSettings{
				"v6net": {},
			},
		)

		_, ok := w.parseContainerData(inspect)
		assert.False(t, ok)
	})
}

func TestDockerWatcherParseContainerData_AutoScaleWith(t *testing.T) {
	makeInspect := func(labels map[string]string, state *container.State) *container.InspectResponse {
		return &container.InspectResponse{
			Config: &container.Config{
				Labels: labels,
			},
			ContainerJSONBase: &container.ContainerJSONBase{
				ID:    "abc123",
				Name:  "/sidecar",
				State: state,
			},
		}
	}

	w := &dockerWatcherImpl{}

	t.Run("recognizes a running sidecar without a host", func(t *testing.T) {
		inspect := makeInspect(
			map[string]string{DockerRouterLabelAutoScaleWith: "mc.example.com"},
			&container.State{Running: true},
		)

		data, ok := w.parseContainerData(inspect)
		require.True(t, ok)
		assert.Empty(t, data.hosts)
		assert.Equal(t, []string{"mc.example.com"}, data.autoScaleWith)
		assert.True(t, data.running)
	})

	t.Run("recognizes a stopped sidecar", func(t *testing.T) {
		inspect := makeInspect(
			map[string]string{DockerRouterLabelAutoScaleWith: "mc.example.com"},
			&container.State{Running: false},
		)

		data, ok := w.parseContainerData(inspect)
		require.True(t, ok)
		assert.Equal(t, []string{"mc.example.com"}, data.autoScaleWith)
		assert.False(t, data.running)
	})

	t.Run("splits and lowercases multiple hosts", func(t *testing.T) {
		inspect := makeInspect(
			map[string]string{DockerRouterLabelAutoScaleWith: "MC.Example.com,\n Other.Example.com "},
			&container.State{Running: true},
		)

		data, ok := w.parseContainerData(inspect)
		require.True(t, ok)
		assert.Equal(t, []string{"mc.example.com", "other.example.com"}, data.autoScaleWith)
	})

	t.Run("container with both host and auto-scale-with is routable and a sidecar", func(t *testing.T) {
		inspect := &container.InspectResponse{
			Config: &container.Config{
				Labels: map[string]string{
					DockerRouterLabelHost:          "mc.example.com",
					DockerRouterLabelAutoScaleWith: "other.example.com",
				},
			},
			NetworkSettings: &container.NetworkSettings{
				Networks: map[string]*network.EndpointSettings{
					"net": {IPAddress: "172.20.0.2"},
				},
			},
			ContainerJSONBase: &container.ContainerJSONBase{
				ID:    "abc123",
				Name:  "/both",
				State: &container.State{Running: true},
			},
		}

		data, ok := w.parseContainerData(inspect)
		require.True(t, ok)
		assert.Equal(t, []string{"mc.example.com"}, data.hosts)
		assert.Equal(t, []string{"other.example.com"}, data.autoScaleWith)
	})

	t.Run("container without host or auto-scale-with is ignored", func(t *testing.T) {
		inspect := makeInspect(map[string]string{"unrelated": "label"}, &container.State{Running: true})

		_, ok := w.parseContainerData(inspect)
		assert.False(t, ok)
	})
}

func TestDockerWatcherReconcileSidecars(t *testing.T) {
	newWatcher := func(containers map[string]*routableContainer, sidecars map[string]*dockerSidecar) *dockerWatcherImpl {
		return &dockerWatcherImpl{containerMap: containers, sidecars: sidecars}
	}

	t.Run("starts a sidecar when any backend is running", func(t *testing.T) {
		w := newWatcher(
			map[string]*routableContainer{
				"mc.example.com":    {externalName: "mc.example.com", containerId: "backend", running: true},
				"other.example.com": {externalName: "other.example.com", containerId: "backend2", running: false},
			},
			map[string]*dockerSidecar{
				"sc1": {containerId: "sc1", hosts: []string{"mc.example.com", "other.example.com"}, running: false},
			},
		)

		starts, stops := w.reconcileSidecarsLocked()
		assert.Equal(t, []string{"sc1"}, starts)
		assert.Empty(t, stops)
	})

	t.Run("stops a sidecar only once all backends have stopped", func(t *testing.T) {
		w := newWatcher(
			map[string]*routableContainer{
				"mc.example.com":    {externalName: "mc.example.com", containerId: "backend", running: true},
				"other.example.com": {externalName: "other.example.com", containerId: "backend2", running: false},
			},
			map[string]*dockerSidecar{
				"sc1": {containerId: "sc1", hosts: []string{"mc.example.com", "other.example.com"}, running: true},
			},
		)

		starts, stops := w.reconcileSidecarsLocked()
		assert.Empty(t, starts)
		assert.Empty(t, stops)

		w.containerMap["mc.example.com"].running = false
		starts, stops = w.reconcileSidecarsLocked()
		assert.Empty(t, starts)
		assert.Equal(t, []string{"sc1"}, stops)
	})

	t.Run("treats an unknown backend as stopped", func(t *testing.T) {
		w := newWatcher(
			map[string]*routableContainer{},
			map[string]*dockerSidecar{
				"sc1": {containerId: "sc1", hosts: []string{"mc.example.com"}, running: true},
			},
		)

		_, stops := w.reconcileSidecarsLocked()
		assert.Equal(t, []string{"sc1"}, stops)
	})

	t.Run("leaves sidecars already in the desired state alone", func(t *testing.T) {
		w := newWatcher(
			map[string]*routableContainer{
				"mc.example.com": {externalName: "mc.example.com", containerId: "backend", running: true},
			},
			map[string]*dockerSidecar{
				"sc1": {containerId: "sc1", hosts: []string{"mc.example.com"}, running: true},
			},
		)

		starts, stops := w.reconcileSidecarsLocked()
		assert.Empty(t, starts)
		assert.Empty(t, stops)
	})
}

func TestDockerWatcherSidecarRegistration(t *testing.T) {
	w := &dockerWatcherImpl{sidecars: map[string]*dockerSidecar{}}

	w.setContainerSidecarsLocked("sc1", &dockerSidecar{
		containerId: "sc1", name: "/sc1", hosts: []string{"mc.example.com"}, running: true,
	})
	require.Contains(t, w.sidecars, "sc1")
	assert.Equal(t, []string{"mc.example.com"}, w.sidecars["sc1"].hosts)
	assert.True(t, w.sidecars["sc1"].running)

	w.setContainerSidecarsLocked("sc1", &dockerSidecar{
		containerId: "sc1", name: "/sc1", hosts: []string{"other.example.com"}, running: false,
	})
	assert.Equal(t, []string{"other.example.com"}, w.sidecars["sc1"].hosts)
	assert.False(t, w.sidecars["sc1"].running)

	w.setContainerSidecarsLocked("sc1", nil)
	assert.NotContains(t, w.sidecars, "sc1")
}

func TestDockerWatcherSidecarFor(t *testing.T) {
	w := &dockerWatcherImpl{}

	assert.Nil(t, w.sidecarFor("abc123", parsedDockerContainerData{}))

	sidecar := w.sidecarFor("abc123", parsedDockerContainerData{
		name:          "/sidecar",
		autoScaleWith: []string{"mc.example.com"},
		running:       true,
	})
	require.NotNil(t, sidecar)
	assert.Equal(t, "abc123", sidecar.containerId)
	assert.Equal(t, "/sidecar", sidecar.name)
	assert.Equal(t, []string{"mc.example.com"}, sidecar.hosts)
	assert.True(t, sidecar.running)
}

// fakeDockerAPI is a fake Docker daemon used by the lifecycle tests.
type fakeDockerAPI struct {
	mu         sync.Mutex
	calls      []fakeDockerCall
	containers map[string]*fakeDockerContainer
	startFail  map[string]bool
	stopFail   map[string]bool
}

type fakeDockerContainer struct {
	name    string
	labels  map[string]string
	ip      string
	running bool
}

type fakeDockerCall struct {
	method string
	id     string
	action string
}

func newFakeDockerAPI(t *testing.T) (*fakeDockerAPI, *client.Client) {
	t.Helper()
	fake := &fakeDockerAPI{
		containers: map[string]*fakeDockerContainer{},
		startFail:  map[string]bool{},
		stopFail:   map[string]bool{},
	}
	server := httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(server.Close)

	cli, err := client.NewClientWithOpts(client.WithHost(server.URL), client.WithVersion("1.44"))
	require.NoError(t, err)
	return fake, cli
}

func (f *fakeDockerAPI) addContainer(id string, labels map[string]string, ip string, running bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.containers[id] = &fakeDockerContainer{name: "/" + id, labels: labels, ip: ip, running: running}
}

func (f *fakeDockerAPI) setRunning(id string, running bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.containers[id]; ok {
		c.running = running
	}
}

func (f *fakeDockerAPI) failOn(action, id string, fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch action {
	case "start":
		f.startFail[id] = fail
	case "stop":
		f.stopFail[id] = fail
	}
}

func (f *fakeDockerAPI) handle(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	id, action := "", ""
	for i, p := range parts {
		if p == "containers" && i+1 < len(parts) {
			id = parts[i+1]
			if i+2 < len(parts) {
				action = parts[i+2]
			}
			break
		}
	}

	f.mu.Lock()
	f.calls = append(f.calls, fakeDockerCall{method: r.Method, id: id, action: action})
	container := f.containers[id]
	fail := (action == "start" && f.startFail[id]) || (action == "stop" && f.stopFail[id])
	if !fail && container != nil {
		switch action {
		case "start", "unpause":
			container.running = true
		case "stop":
			container.running = false
		}
	}
	var snapshot *fakeDockerContainer
	if container != nil {
		copied := *container
		snapshot = &copied
	}
	f.mu.Unlock()

	if action == "json" {
		if snapshot == nil {
			http.Error(w, "no such container", http.StatusNotFound)
			return
		}
		writeFakeDockerInspect(w, id, snapshot)
		return
	}

	if fail {
		http.Error(w, "boom", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeFakeDockerInspect(w http.ResponseWriter, id string, c *fakeDockerContainer) {
	networks := map[string]any{}
	if c.ip != "" {
		networks["bridge"] = map[string]any{"IPAddress": c.ip}
	}
	status := "exited"
	if c.running {
		status = "running"
	}
	result := map[string]any{
		"Id":   id,
		"Name": c.name,
		"State": map[string]any{
			"Running": c.running,
			"Status":  status,
		},
		"Config":          map[string]any{"Labels": c.labels},
		"NetworkSettings": map[string]any{"Networks": networks},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (f *fakeDockerAPI) actionsOf(action string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []string
	for _, c := range f.calls {
		if c.action == action {
			ids = append(ids, c.id)
		}
	}
	return ids
}

func (f *fakeDockerAPI) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

func TestDockerWatcherSidecarLifecycle(t *testing.T) {
	fake, cli := newFakeDockerAPI(t)

	const (
		sidecarA = "aaaaaaaaaa01"
		sidecarB = "bbbbbbbbbb02"
	)
	fake.addContainer(sidecarA, nil, "", false)
	fake.addContainer(sidecarB, nil, "", false)

	w := &dockerWatcherImpl{client: cli}

	t.Run("starts the sidecars it is given", func(t *testing.T) {
		fake.reset()
		fake.setRunning(sidecarA, false)
		fake.setRunning(sidecarB, false)

		w.performSidecarChanges(context.Background(), []string{sidecarA, sidecarB}, nil)

		assert.ElementsMatch(t, []string{sidecarA, sidecarB}, fake.actionsOf("start"))
	})

	t.Run("is best-effort when a sidecar fails to start", func(t *testing.T) {
		fake.reset()
		fake.setRunning(sidecarA, false)
		fake.setRunning(sidecarB, false)
		fake.failOn("start", sidecarA, true)
		defer fake.failOn("start", sidecarA, false)

		// Must not abort if one sidecar fails; both are still attempted.
		w.performSidecarChanges(context.Background(), []string{sidecarA, sidecarB}, nil)

		assert.ElementsMatch(t, []string{sidecarA, sidecarB}, fake.actionsOf("start"))
	})

	t.Run("stops the sidecars it is given", func(t *testing.T) {
		fake.reset()
		fake.setRunning(sidecarA, true)
		fake.setRunning(sidecarB, true)

		w.performSidecarChanges(context.Background(), nil, []string{sidecarA, sidecarB})

		assert.ElementsMatch(t, []string{sidecarA, sidecarB}, fake.actionsOf("stop"))
	})

	t.Run("is best-effort when a sidecar fails to stop", func(t *testing.T) {
		fake.reset()
		fake.setRunning(sidecarA, true)
		fake.setRunning(sidecarB, true)
		fake.failOn("stop", sidecarA, true)
		defer fake.failOn("stop", sidecarA, false)

		w.performSidecarChanges(context.Background(), nil, []string{sidecarA, sidecarB})

		assert.ElementsMatch(t, []string{sidecarA, sidecarB}, fake.actionsOf("stop"))
	})
}

// TestDockerWatcherSidecarFollowsBackendLifecycle verifies that a sidecar is
// started and stopped in reaction to the backend's observed container state,
// which is how manual `docker start`/`docker stop` of the backend is handled.
func TestDockerWatcherSidecarFollowsBackendLifecycle(t *testing.T) {
	fake, cli := newFakeDockerAPI(t)

	const (
		sidecar = "aaaaaaaaaa01"
		backend = "bbbbbbbbbb02"
	)
	fake.addContainer(backend, map[string]string{
		DockerRouterLabelHost:        "mc.example.com",
		DockerRouterLabelAutoScaleUp: "true",
	}, "172.20.0.2", true)
	fake.addContainer(sidecar, map[string]string{
		DockerRouterLabelAutoScaleWith: "mc.example.com",
	}, "", true)

	w := &dockerWatcherImpl{
		client:       cli,
		config:       dockerWatcherConfig{autoScaleUp: true},
		routes:       NewRoutes(t.Context()),
		containerMap: map[string]*routableContainer{},
		sidecars:     map[string]*dockerSidecar{},
	}
	w.setContainerSidecarsLocked(sidecar, &dockerSidecar{containerId: sidecar, hosts: []string{"mc.example.com"}, running: true})

	wasCalled := func(action, id string) func() bool {
		return func() bool {
			return slices.Contains(fake.actionsOf(action), id)
		}
	}

	// Stopping the backend stops the sidecar.
	fake.setRunning(backend, false)
	require.NoError(t, w.applyEvent(context.Background(), events.Message{
		Type:   events.ContainerEventType,
		Action: events.ActionDie,
		Actor:  events.Actor{ID: backend},
	}))
	assert.Eventually(t, wasCalled("stop", sidecar), time.Second, 10*time.Millisecond)

	// Starting the backend starts the sidecar again.
	fake.reset()
	w.monitorLock.Lock()
	w.sidecars[sidecar].running = false
	w.monitorLock.Unlock()
	fake.setRunning(backend, true)
	require.NoError(t, w.applyEvent(context.Background(), events.Message{
		Type:   events.ContainerEventType,
		Action: events.ActionStart,
		Actor:  events.Actor{ID: backend},
	}))
	assert.Eventually(t, wasCalled("start", sidecar), time.Second, 10*time.Millisecond)
}
