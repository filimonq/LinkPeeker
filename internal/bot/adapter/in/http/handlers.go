package bothttp

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
)

// Handler handles incoming HTTP requests for bot.
type Handler struct {
	updateSvc service.UpdateService
	logger    *slog.Logger
}

// NewHandler creates a new Handler.
func NewHandler(updateSvc service.UpdateService, logger *slog.Logger) *Handler {
	return &Handler{updateSvc: updateSvc, logger: logger}
}

func (h *Handler) handleUpdates(w http.ResponseWriter, r *http.Request) {
	var req LinkUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(APIErrorResponse{Description: "invalid request body"})
		return
	}

	update := pkgdomain.LinkUpdate{
		ID:          req.ID,
		URL:         req.URL,
		Description: req.Description,
		TgChatIDs:   req.TgChatIDs,
	}

	if err := h.updateSvc.SendUpdate(r.Context(), update); err != nil {
		h.logger.Error("send update failed", slog.String("error", err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
