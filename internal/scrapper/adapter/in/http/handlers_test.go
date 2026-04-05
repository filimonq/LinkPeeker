package scrapperhttp_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	scrapperhttp "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/in/http"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/adapter/out/repository/memory"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/service"
)

func newTestScrapperServer() *httptest.Server {
	chatRepo := memory.NewChatRepository()
	linkRepo := memory.NewLinkRepository()
	tagRepo := memory.NewTagRepository()
	svc := service.NewScrapperService(chatRepo, linkRepo, memory.NewSubscriptionRepository(linkRepo), tagRepo, nil, nil, 4, 10)
	handler := scrapperhttp.NewHandler(svc, slog.Default())
	server := scrapperhttp.NewServer(":0", handler)
	return httptest.NewServer(server.Handler())
}

func post(t *testing.T, srv *httptest.Server, path string, body string, chatID int64) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if chatID != 0 {
		req.Header.Set("Tg-Chat-Id", strconv.FormatInt(chatID, 10))
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // intentional for local test server
	require.NoError(t, err)
	return resp
}

func del(t *testing.T, srv *httptest.Server, path string, body string, chatID int64) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if chatID != 0 {
		req.Header.Set("Tg-Chat-Id", strconv.FormatInt(chatID, 10))
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // intentional for local test server
	require.NoError(t, err)
	return resp
}

func get(t *testing.T, srv *httptest.Server, path string, chatID int64) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if chatID != 0 {
		req.Header.Set("Tg-Chat-Id", strconv.FormatInt(chatID, 10))
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // intentional for local test server
	require.NoError(t, err)
	return resp
}

func decodeLinks(t *testing.T, resp *http.Response) scrapperhttp.ListLinksResponse {
	t.Helper()
	var result scrapperhttp.ListLinksResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	return result
}

// 3.1 регистрация + добавление + получение
func TestScrapper_AddAndListLink(t *testing.T) {
	srv := newTestScrapperServer()
	defer srv.Close()

	resp := post(t, srv, "/tg-chat/1", "", 0)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = post(t, srv, "/links", `{"link":"https://github.com/foo/bar","tags":["go"]}`, 1)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = get(t, srv, "/links", 1)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	result := decodeLinks(t, resp)
	assert.Equal(t, 1, result.Size)
}

// 3.2 добавление + удаление + список пустой
func TestScrapper_AddAndDeleteLink(t *testing.T) {
	srv := newTestScrapperServer()
	defer srv.Close()

	resp := post(t, srv, "/tg-chat/1", "", 0)
	_ = resp.Body.Close()
	resp = post(t, srv, "/links", `{"link":"https://github.com/foo/bar"}`, 1)
	_ = resp.Body.Close()

	resp = del(t, srv, "/links", `{"link":"https://github.com/foo/bar"}`, 1)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = get(t, srv, "/links", 1)
	defer func() { _ = resp.Body.Close() }()
	result := decodeLinks(t, resp)
	assert.Equal(t, 0, result.Size)
}

// 3.3 удаление из несуществующего чата
func TestScrapper_DeleteLinkFromUnknownChat(t *testing.T) {
	srv := newTestScrapperServer()
	defer srv.Close()

	resp := post(t, srv, "/tg-chat/1", "", 0)
	_ = resp.Body.Close()
	resp = post(t, srv, "/links", `{"link":"https://github.com/foo/bar"}`, 1)
	_ = resp.Body.Close()

	resp = del(t, srv, "/links", `{"link":"https://github.com/foo/bar"}`, 999)
	_ = resp.Body.Close()
	assert.NotEqual(t, http.StatusOK, resp.StatusCode)

	resp = get(t, srv, "/links", 1)
	defer func() { _ = resp.Body.Close() }()
	result := decodeLinks(t, resp)
	assert.Equal(t, 1, result.Size)
}

// 3.4 добавление ссылки в несуществующий чат
func TestScrapper_AddLinkToUnknownChat(t *testing.T) {
	srv := newTestScrapperServer()
	defer srv.Close()
	resp := post(t, srv, "/tg-chat/1", "", 0)
	_ = resp.Body.Close()

	resp = post(t, srv, "/links", `{"link":"https://github.com/foo/bar"}`, 2)
	defer func() { _ = resp.Body.Close() }()

	assert.NotEqual(t, http.StatusOK, resp.StatusCode)
}

// 3.5 удалённый чат не может добавить ссылку
func TestScrapper_AddLinkAfterChatDeleted(t *testing.T) {
	srv := newTestScrapperServer()
	defer srv.Close()

	resp := post(t, srv, "/tg-chat/1", "", 0)
	_ = resp.Body.Close()

	resp = del(t, srv, "/tg-chat/1", "", 0)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = post(t, srv, "/links", `{"link":"https://github.com/foo/bar"}`, 1)
	defer func() { _ = resp.Body.Close() }()

	assert.NotEqual(t, http.StatusOK, resp.StatusCode)
}

// 3.6 удаление несуществующего чата
func TestScrapper_DeleteUnknownChat(t *testing.T) {
	srv := newTestScrapperServer()
	defer srv.Close()

	resp := del(t, srv, "/tg-chat/1", "", 0)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
