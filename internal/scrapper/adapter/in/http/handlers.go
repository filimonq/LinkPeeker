package scrapperhttp

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"log/slog"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/chat"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/service"
)

// Handler handles HTTP requests for scrapper.
type Handler struct {
	svc    service.ScrapperService
	logger *slog.Logger
}

// NewHandler creates a new Handler.
func NewHandler(svc service.ScrapperService, logger *slog.Logger) *Handler {
	return &Handler{svc: svc, logger: logger}
}

// RegisterRoutes registers all routes on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /tg-chat/{id}", h.registerChat)
	mux.HandleFunc("DELETE /tg-chat/{id}", h.deleteChat)
	mux.HandleFunc("GET /links", h.listLinks)
	mux.HandleFunc("POST /links", h.addLink)
	mux.HandleFunc("DELETE /links", h.deleteLink)
}

func (h *Handler) registerChat(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid chat id")
		return
	}

	if err = h.svc.RegisterChat(r.Context(), id); err != nil {
		if errors.Is(err, chat.ErrAlreadyExists) {
			h.writeError(w, http.StatusConflict, "chat already exists")
			return
		}
		h.logger.Error("register chat", slog.String("error", err.Error()))
		h.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteChat(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid chat id")
		return
	}

	if err = h.svc.DeleteChat(r.Context(), id); err != nil {
		if errors.Is(err, chat.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "chat not found")
			return
		}
		h.logger.Error("delete chat", slog.String("error", err.Error()))
		h.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) listLinks(w http.ResponseWriter, r *http.Request) {
	chatID, err := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid Tg-Chat-Id header")
		return
	}

	links, err := h.svc.ListLinks(r.Context(), chatID)
	if err != nil {
		if errors.Is(err, chat.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "chat not found")
			return
		}
		h.logger.Error("list links", slog.String("error", err.Error()))
		h.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	resp := ListLinksResponse{
		Links: make([]LinkResponse, len(links)),
		Size:  len(links),
	}
	for i, l := range links {
		resp.Links[i] = toLinkResponse(l)
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) addLink(w http.ResponseWriter, r *http.Request) {
	chatID, err := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid Tg-Chat-Id header")
		return
	}

	var req AddLinkRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	l, err := h.svc.AddLink(r.Context(), chatID, req.Link, req.Tags)
	if err != nil {
		if errors.Is(err, chat.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "chat not found")
			return
		}
		if errors.Is(err, link.ErrAlreadyExists) {
			h.writeError(w, http.StatusConflict, "link already tracked")
			return
		}
		h.logger.Error("add link", slog.String("error", err.Error()))
		h.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	h.writeJSON(w, http.StatusOK, toLinkResponse(l))
}

func (h *Handler) deleteLink(w http.ResponseWriter, r *http.Request) {
	chatID, err := strconv.ParseInt(r.Header.Get("Tg-Chat-Id"), 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid Tg-Chat-Id header")
		return
	}

	var req RemoveLinkRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	l, err := h.svc.RemoveLink(r.Context(), chatID, req.Link)
	if err != nil {
		if errors.Is(err, chat.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "chat not found")
			return
		}
		if errors.Is(err, link.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "link not found")
			return
		}
		h.logger.Error("delete link", slog.String("error", err.Error()))
		h.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	h.writeJSON(w, http.StatusOK, toLinkResponse(l))
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Error("encode response", slog.String("error", err.Error()))
	}
}

func (h *Handler) writeError(w http.ResponseWriter, status int, description string) {
	h.writeJSON(w, status, APIErrorResponse{
		Description: description,
		Code:        strconv.Itoa(status),
	})
}

func toLinkResponse(l *link.LinkWithTags) LinkResponse {
	return LinkResponse{
		ID:   int64(l.ID),
		URL:  l.URL,
		Tags: l.Tags,
	}
}
