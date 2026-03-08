// Package scrapper provides gRPC client for scrapper service.
package scrapper

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot/service"
	scrapperpb "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/proto"
)

// Client is a gRPC client for scrapper service.
type Client struct {
	pb scrapperpb.ScrapperServiceClient
}

// NewClient creates a new scrapper gRPC Client.
func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to scrapper: %w", err)
	}
	return &Client{pb: scrapperpb.NewScrapperServiceClient(conn)}, nil
}

// RegisterChat registers a chat in scrapper.
func (c *Client) RegisterChat(ctx context.Context, chatID int64) error {
	_, err := c.pb.RegisterChat(ctx, &scrapperpb.RegisterChatRequest{ChatId: chatID})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
		return fmt.Errorf("register chat: %w", err)
	}
	return nil
}

// DeleteChat deletes a chat from scrapper.
func (c *Client) DeleteChat(ctx context.Context, chatID int64) error {
	_, err := c.pb.DeleteChat(ctx, &scrapperpb.DeleteChatRequest{ChatId: chatID})
	if err != nil {
		return fmt.Errorf("delete chat: %w", err)
	}
	return nil
}

// AddLink adds a link to scrapper.
func (c *Client) AddLink(ctx context.Context, chatID int64, rawURL string, tags []string) (*service.LinkInfo, error) {
	resp, err := c.pb.AddLink(ctx, &scrapperpb.AddLinkRequest{
		ChatId: chatID,
		Url:    rawURL,
		Tags:   tags,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil, service.ErrLinkAlreadyExists
		}
		if status.Code(err) == codes.NotFound {
			return nil, service.ErrLinkNotFound
		}
		return nil, fmt.Errorf("add link: %w", err)
	}
	return &service.LinkInfo{ID: resp.Id, URL: resp.Url, Tags: resp.Tags}, nil
}

// RemoveLink removes a link from scrapper.
func (c *Client) RemoveLink(ctx context.Context, chatID int64, rawURL string) (*service.LinkInfo, error) {
	resp, err := c.pb.RemoveLink(ctx, &scrapperpb.RemoveLinkRequest{
		ChatId: chatID,
		Url:    rawURL,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, service.ErrLinkNotFound
		}
		return nil, fmt.Errorf("remove link: %w", err)
	}
	return &service.LinkInfo{ID: resp.Id, URL: resp.Url, Tags: resp.Tags}, nil
}

// ListLinks returns all tracked links for a chat.
func (c *Client) ListLinks(ctx context.Context, chatID int64) ([]*service.LinkInfo, error) {
	resp, err := c.pb.ListLinks(ctx, &scrapperpb.ListLinksRequest{ChatId: chatID})
	if err != nil {
		return nil, fmt.Errorf("list links: %w", err)
	}
	links := make([]*service.LinkInfo, len(resp.Links))
	for i, l := range resp.Links {
		links[i] = &service.LinkInfo{ID: l.Id, URL: l.Url, Tags: l.Tags}
	}
	return links, nil
}
