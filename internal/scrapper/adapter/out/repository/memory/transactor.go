package memory

import "context"

type Transactor struct{}

func NewTransactor() *Transactor {
	return &Transactor{}
}

// WithinTransaction runs fn inside a transaction.
// Since this is an in-memory implementation, it just runs the function without actual transaction management.
func (t *Transactor) WithinTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}
