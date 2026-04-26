package integration

import (
	"context"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

var (
	kafkaBrokers []string
	once         sync.Once
)

func GetTestKafka(_ *testing.T) []string {
	once.Do(func() {
		ctx := context.Background()
		kc, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
		if err != nil {
			panic(err)
		}

		brokers, err := kc.Brokers(ctx)
		if err != nil {
			panic(err)
		}
		kafkaBrokers = brokers
	})

	return kafkaBrokers
}
