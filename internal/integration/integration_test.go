package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

type E2ESuite struct {
	suite.Suite

	ctx         context.Context
	cancel      context.CancelFunc
	client      *http.Client
	scrapperURL string
	botURL      string
	dbURL       string
}

func (s *E2ESuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 10*time.Minute)
	s.client = &http.Client{Timeout: 5 * time.Second}

	mockPort := s.startTgMock()
	dockerNet, gateway := s.startDockerNetwork()
	s.dbURL = s.startPostgres(dockerNet.Name)
	s.scrapperURL = s.startScrapper(dockerNet.Name)
	s.botURL = s.startBot(dockerNet.Name, gateway, mockPort)
}

func (s *E2ESuite) TearDownSuite() {
	s.cancel()
}

func (s *E2ESuite) TestScrapper_RegisterAndAddLink() {
	req, _ := http.NewRequestWithContext(s.ctx, http.MethodPost, s.scrapperURL+"/tg-chat/1", nil)
	resp, err := s.client.Do(req) //nolint:gosec // testing environment
	s.Require().NoError(err)
	_ = resp.Body.Close()
	s.Equal(http.StatusOK, resp.StatusCode)

	body := `{"link":"https://github.com/golang/go","tags":["go"]}`
	req, _ = http.NewRequestWithContext(s.ctx, http.MethodPost, s.scrapperURL+"/links",
		bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Tg-Chat-Id", "1")
	resp, err = s.client.Do(req) //nolint:gosec // testing environment
	s.Require().NoError(err)
	_ = resp.Body.Close()
	s.Equal(http.StatusOK, resp.StatusCode)
}

func (s *E2ESuite) TestBot_ReceivesUpdate() {
	body := `{"id":1,"url":"https://github.com/golang/go","description":"update","tgChatIds":[1]}`
	req, _ := http.NewRequestWithContext(s.ctx, http.MethodPost, s.botURL+"/updates",
		bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req) //nolint:gosec // testing environment
	s.Require().NoError(err)
	defer func() { _ = resp.Body.Close() }()
	s.Equal(http.StatusOK, resp.StatusCode)
}

func (s *E2ESuite) TestScrapper_DeleteUnknownChat() {
	req, _ := http.NewRequestWithContext(s.ctx, http.MethodDelete, s.scrapperURL+"/tg-chat/999", nil)
	resp, err := s.client.Do(req) //nolint:gosec // testing environment
	s.Require().NoError(err)
	defer func() { _ = resp.Body.Close() }()
	s.Equal(http.StatusNotFound, resp.StatusCode)
}

func (s *E2ESuite) TestScrapper_DeleteLink() {
	req, _ := http.NewRequestWithContext(s.ctx, http.MethodPost, s.scrapperURL+"/tg-chat/2", nil)
	resp, err := s.client.Do(req) //nolint:gosec // testing environment
	s.Require().NoError(err)
	_ = resp.Body.Close()

	body := `{"link":"https://github.com/golang/go","tags":[]}`
	req, _ = http.NewRequestWithContext(s.ctx, http.MethodPost, s.scrapperURL+"/links",
		bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Tg-Chat-Id", "2")
	resp, err = s.client.Do(req) //nolint:gosec // testing environment
	s.Require().NoError(err)
	_ = resp.Body.Close()

	req, _ = http.NewRequestWithContext(s.ctx, http.MethodDelete, s.scrapperURL+"/links",
		bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Tg-Chat-Id", "2")
	resp, err = s.client.Do(req) //nolint:gosec // testing environment
	s.Require().NoError(err)
	defer func() { _ = resp.Body.Close() }()
	s.Equal(http.StatusOK, resp.StatusCode)
}

func (s *E2ESuite) TestScrapper_DuplicateLink() {
	req, _ := http.NewRequestWithContext(s.ctx, http.MethodPost, s.scrapperURL+"/tg-chat/3", nil)
	resp, err := s.client.Do(req) //nolint:gosec // testing environment
	s.Require().NoError(err)
	_ = resp.Body.Close()

	body := `{"link":"https://github.com/golang/go","tags":[]}`

	req, _ = http.NewRequestWithContext(s.ctx, http.MethodPost, s.scrapperURL+"/links",
		bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Tg-Chat-Id", "3")
	resp, err = s.client.Do(req) //nolint:gosec // testing environment
	s.Require().NoError(err)
	_ = resp.Body.Close()

	req, _ = http.NewRequestWithContext(s.ctx, http.MethodPost, s.scrapperURL+"/links",
		bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Tg-Chat-Id", "3")
	resp, err = s.client.Do(req) //nolint:gosec // testing environment
	s.Require().NoError(err)
	defer func() { _ = resp.Body.Close() }()
	s.Equal(http.StatusConflict, resp.StatusCode)
}

func (s *E2ESuite) startTgMock() string {
	l, err := net.Listen("tcp", "0.0.0.0:0") //nolint:gosec // G102: needed for Docker accessibility
	s.Require().NoError(err)

	tgMock := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/getMe"):
			_, _ = fmt.Fprintln(w, `{"ok":true,"result":{"id":1,"is_bot":true,"first_name":"Test","username":"test_bot"}}`)
		case strings.HasSuffix(r.URL.Path, "/getUpdates"):
			_, _ = fmt.Fprintln(w, `{"ok":true,"result":[]}`)
		default:
			_, _ = fmt.Fprintln(w, `{"ok":true,"result":{}}`)
		}
	}))
	tgMock.Listener = l
	tgMock.Start()
	s.T().Cleanup(tgMock.Close)

	_, port, err := net.SplitHostPort(tgMock.Listener.Addr().String())
	s.Require().NoError(err)

	return port
}

func (s *E2ESuite) startDockerNetwork() (*testcontainers.DockerNetwork, string) {
	second := rand.Intn(10) + 20 //nolint:gosec // 172.20-172.30
	third := rand.Intn(200) + 10 //nolint:gosec // insecure random is fine for subnet generation
	subnetStr := fmt.Sprintf("172.%d.%d.0/24", second, third)
	gatewayStr := fmt.Sprintf("172.%d.%d.1", second, third)

	subnet := netip.MustParsePrefix(subnetStr)
	gateway := netip.MustParseAddr(gatewayStr)

	dockerNet, err := network.New(s.ctx,
		network.WithIPAM(&dockernetwork.IPAM{
			Config: []dockernetwork.IPAMConfig{
				{Subnet: subnet, Gateway: gateway},
			},
		}),
	)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = dockerNet.Remove(s.ctx) })

	return dockerNet, "host.docker.internal"
}

func (s *E2ESuite) startScrapper(netName string) string {
	accessType := os.Getenv("ACCESS_TYPE")
	if accessType == "" {
		accessType = "pgx"
	}
	c, err := testcontainers.GenericContainer(s.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    "../../",
				Dockerfile: "build/scrapper/Dockerfile",
			},
			ExposedPorts:   []string{"8081/tcp"},
			Networks:       []string{netName},
			NetworkAliases: map[string][]string{netName: {"scrapper"}},
			Env: map[string]string{
				"BOT_URL":                   "http://bot:8080",
				"SCRAPPER_PORT":             "8081",
				"SCRAPPER_GRPC_PORT":        "50051",
				"SCHEDULE_INTERVAL_SECONDS": "3600",
				"DATABASE_URL":              "postgres://postgres:postgres@postgres:5432/linktracker?sslmode=disable",
				"ACCESS_TYPE":               accessType,
			},
			WaitingFor: wait.ForListeningPort("8081/tcp").WithStartupTimeout(5 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		if c != nil {
			s.printLogs(c)
		}
		s.Require().NoError(err)
	}
	s.T().Cleanup(func() { _ = c.Terminate(s.ctx) })

	host, _ := c.Host(s.ctx)
	port, _ := c.MappedPort(s.ctx, "8081")

	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

func (s *E2ESuite) startBot(netName, gateway, mockPort string) string {
	accessType := os.Getenv("ACCESS_TYPE")
	if accessType == "" {
		accessType = "pgx"
	}
	c, err := testcontainers.GenericContainer(s.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    "../../",
				Dockerfile: "build/bot/Dockerfile",
			},
			ExposedPorts:   []string{"8080/tcp"},
			Networks:       []string{netName},
			NetworkAliases: map[string][]string{netName: {"bot"}},
			ExtraHosts:     []string{"host.docker.internal:host-gateway"},
			Env: map[string]string{
				"TELEGRAM_TOKEN":   "fake-token",
				"TELEGRAM_API_URL": "http://" + gateway + ":" + mockPort,
				"SCRAPPER_URL":     "scrapper:50051",
				"BOT_PORT":         "8080",
				"DATABASE_URL":     "postgres://postgres:postgres@postgres:5432/linktracker?sslmode=disable",
				"ACCESS_TYPE":      accessType,
			},
			WaitingFor: wait.ForListeningPort("8080/tcp").WithStartupTimeout(5 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		if c != nil {
			s.printLogs(c)
		}
		s.Require().NoError(err)
	}
	s.T().Cleanup(func() { _ = c.Terminate(s.ctx) })

	host, _ := c.Host(s.ctx)
	port, _ := c.MappedPort(s.ctx, "8080")

	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

func (s *E2ESuite) startPostgres(netName string) string {
	c, err := testcontainers.GenericContainer(s.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16",
			ExposedPorts: []string{"5432/tcp"},
			Networks:     []string{netName},
			NetworkAliases: map[string][]string{
				netName: {"postgres"},
			},
			Env: map[string]string{
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "postgres",
				"POSTGRES_DB":       "linktracker",
			},
			WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = c.Terminate(s.ctx) })

	host, _ := c.Host(s.ctx)
	port, _ := c.MappedPort(s.ctx, "5432")

	return fmt.Sprintf("postgres://postgres:postgres@%s:%s/linktracker?sslmode=disable", host, port.Port())
}

func (s *E2ESuite) printLogs(c testcontainers.Container) {
	if c == nil {
		return
	}
	logs, err := c.Logs(s.ctx)
	if err != nil {
		s.T().Logf("failed to get logs: %v", err)
		return
	}
	defer func() { _ = logs.Close() }()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(logs)
	s.T().Logf("container logs:\n%s", buf.String())
}

func TestE2E(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(E2ESuite))
}
