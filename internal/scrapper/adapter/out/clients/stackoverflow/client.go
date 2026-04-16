// Package stackoverflow provides StackOverflow API client.
package stackoverflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	pkgdomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/pkg/domain"
	"golang.org/x/sync/errgroup"
)

// Client is a StackOverflow API client.
type Client struct {
	http    *http.Client
	baseURL string
}

const defaultTimeout = 10 * time.Second

// NewClient creates a new StackOverflow client.
func NewClient() *Client {
	return &Client{
		http:    &http.Client{Timeout: defaultTimeout},
		baseURL: "https://api.stackexchange.com/2.3",
	}
}

// NewClientWithBaseURL creates a client with custom base URL for testing.
func NewClientWithBaseURL(baseURL string) *Client {
	return &Client{
		http:    &http.Client{Timeout: defaultTimeout},
		baseURL: baseURL,
	}
}

type soAnswer struct {
	AnswerID     int64  `json:"answer_id"`
	Score        int    `json:"score"`
	Body         string `json:"body"`
	CreationDate int64  `json:"creation_date"`
	Owner        struct {
		DisplayName string `json:"display_name"`
	} `json:"owner"`
}

type soAnswersResponse struct {
	Items []soAnswer `json:"items"`
}

type soComment struct {
	CommentID    int64  `json:"comment_id"`
	Body         string `json:"body"`
	CreationDate int64  `json:"creation_date"`
	Owner        struct {
		DisplayName string `json:"display_name"`
	} `json:"owner"`
}

type soCommentsResponse struct {
	Items []soComment `json:"items"`
}

type soQuestionResponse struct {
	Items []struct {
		Title            string `json:"title"`
		LastActivityDate int64  `json:"last_activity_date"`
	} `json:"items"`
}

// GetUpdate returns info about the latest activity (answer or comment) for a SO question URL.
func (c *Client) GetUpdate(ctx context.Context, url string) (*pkgdomain.UpdateInfo, error) {
	questionID, err := parseStackOverflowQuestionID(url)
	if err != nil {
		return nil, err
	}

	questionTitle, lastActivity, err := c.fetchQuestionMeta(ctx, questionID)
	if err != nil {
		return nil, err
	}

	var (
		answer  *soAnswer
		comment *soComment
		mu      sync.Mutex
	)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		a, fetchErr := c.fetchLatestAnswer(gCtx, questionID)
		if fetchErr != nil {
			slog.Warn("failed to fetch latest answer", "question_id", questionID, "err", fetchErr)
			return nil
		}
		mu.Lock()
		answer = a
		mu.Unlock()
		return nil
	})

	g.Go(func() error {
		cmt, fetchErr := c.fetchLatestComment(gCtx, questionID)
		if fetchErr != nil {
			slog.Warn("failed to fetch latest comment", "question_id", questionID, "err", fetchErr)
			return nil
		}
		mu.Lock()
		comment = cmt
		mu.Unlock()
		return nil
	})

	if waitErr := g.Wait(); waitErr != nil {
		return nil, fmt.Errorf("parallel fetch failed: %w", waitErr)
	}

	if comment != nil {
		commentTime := time.Unix(comment.CreationDate, 0)
		if answer == nil || commentTime.After(time.Unix(answer.CreationDate, 0)) {
			return &pkgdomain.UpdateInfo{
				UpdatedAt: lastActivity,
				Title:     questionTitle,
				Author:    comment.Owner.DisplayName,
				CreatedAt: commentTime,
				Preview:   comment.Body,
			}, nil
		}
	}

	if answer != nil {
		return &pkgdomain.UpdateInfo{
			UpdatedAt: lastActivity,
			Title:     questionTitle,
			Author:    answer.Owner.DisplayName,
			CreatedAt: time.Unix(answer.CreationDate, 0),
			Preview:   answer.Body,
		}, nil
	}

	return &pkgdomain.UpdateInfo{
		UpdatedAt: lastActivity,
		Title:     questionTitle,
	}, nil
}

// GetLastUpdated returns last activity time — kept for interface compatibility.
func (c *Client) GetLastUpdated(ctx context.Context, url string) (time.Time, error) {
	info, err := c.GetUpdate(ctx, url)
	if err != nil {
		return time.Time{}, err
	}
	return info.UpdatedAt, nil
}

func (c *Client) Supports(url string) bool {
	return strings.HasPrefix(url, "https://stackoverflow.com/questions/")
}

func (c *Client) fetchQuestionMeta(ctx context.Context, questionID string) (string, time.Time, error) {
	apiURL := fmt.Sprintf("%s/questions/%s?site=stackoverflow", c.baseURL, questionID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("so api error: %d", resp.StatusCode)
	}

	var result soQuestionResponse
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", time.Time{}, fmt.Errorf("decode response: %w", err)
	}

	if len(result.Items) == 0 {
		return "", time.Time{}, errors.New("question not found")
	}

	return result.Items[0].Title, time.Unix(result.Items[0].LastActivityDate, 0), nil
}

func (c *Client) fetchLatestAnswer(ctx context.Context, questionID string) (*soAnswer, error) {
	apiURL := fmt.Sprintf("%s/questions/%s/answers?site=stackoverflow&sort=creation&order=desc&per_page=1&filter=withbody", c.baseURL, questionID)
	var result soAnswersResponse
	if err := c.doSORequest(ctx, apiURL, &result); err != nil {
		return nil, err
	}
	if len(result.Items) == 0 {
		//nolint:nilnil // no answers/comments is a valid case handled by caller
		return nil, nil
	}
	return &result.Items[0], nil
}

func (c *Client) fetchLatestComment(ctx context.Context, questionID string) (*soComment, error) {
	apiURL := fmt.Sprintf("%s/questions/%s/comments?site=stackoverflow&sort=creation&order=desc&per_page=1&filter=withbody", c.baseURL, questionID)
	var result soCommentsResponse
	if err := c.doSORequest(ctx, apiURL, &result); err != nil {
		return nil, err
	}
	if len(result.Items) == 0 {
		//nolint:nilnil // no answers/comments is a valid case handled by caller
		return nil, nil
	}
	return &result.Items[0], nil
}

func parseStackOverflowQuestionID(url string) (string, error) {
	trimmed := strings.TrimPrefix(url, "https://stackoverflow.com/questions/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 1 || parts[0] == "" {
		return "", errors.New("invalid stackoverflow url")

	}
	return parts[0], nil
}

func (c *Client) doSORequest(ctx context.Context, url string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SO API error: %d", resp.StatusCode)
	}
	if err = json.NewDecoder(resp.Body).Decode(target); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
