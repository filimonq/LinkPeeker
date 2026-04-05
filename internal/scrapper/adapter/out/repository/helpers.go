package repository

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain/link"
)

// ScanLinks scans rows into a slice of links.
func ScanLinks(rows pgx.Rows) ([]*link.Link, error) {
	var links []*link.Link
	for rows.Next() {
		l := &link.Link{}
		if err := rows.Scan(&l.ID, &l.URL, &l.ChatID, &l.CreatedAt, &l.UpdatedAt, &l.Tags); err != nil {
			return nil, fmt.Errorf("scan link: %w", err)
		}
		links = append(links, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return links, nil
}

// IsUniqueViolation checks if err is a PostgreSQL unique constraint violation.
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
