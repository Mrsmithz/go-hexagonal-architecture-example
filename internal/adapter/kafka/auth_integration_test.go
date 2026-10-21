//go:build integration

package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"example.com/go-backend-example/internal/consumer"
)

func TestKafkaPasswordAuthenticationIntegration(t *testing.T) {
	ctx := testContext(t)
	container, err := kafkamodule.Run(ctx, kafkaTestImage,
		kafkamodule.WithClusterID("go-backend-auth-tests"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":                 "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_SASL_ENABLED_MECHANISMS":                        "PLAIN",
			"KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG": `org.apache.kafka.common.security.plain.PlainLoginModule required username="worker" password="test-password" user_worker="test-password";`,
		}),
	)
	if err != nil {
		t.Fatalf("start SASL Kafka: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate SASL Kafka: %v", err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const topic = "integration-auth"
	createTopic(t, ctx, container, topic, 1)
	cfg := integrationConfig(brokers, topic, "integration-auth-group")
	cfg.Kafka.Auth = consumer.AuthPlain
	cfg.Kafka.Username = "worker"
	cfg.Kafka.Password = "test-password"
	factory, err := NewClientFactory(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	mechanism, err := authMechanism(ctx, cfg.Kafka)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.SASL(mechanism))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("authenticated")}).FirstErr(); err != nil {
		t.Fatalf("authenticated production failed: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	received := make(chan consumer.Message, 1)
	commits := make(chan int, 1)
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(runCtx, cfg, func(_ context.Context, message consumer.Message) error {
			received <- message
			return nil
		}, func(config consumer.Config, topic consumer.TopicConfig) (*observedClient, error) {
			client, err := factory(config, topic)
			if err != nil {
				return nil, err
			}
			return &observedClient{Client: client, onCommit: func(messages []consumer.Message) error {
				commits <- len(messages)
				return nil
			}}, nil
		})
	}()
	waitForMessages(t, done, received, 1)
	waitForCommits(t, done, commits, 1, cfg.Kafka.MaxPollRecords)
	cancel()
	waitForRun(t, done)

	cfg.Kafka.Password = "incorrect-password"
	badFactory, err := NewClientFactory(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	badClient, err := badFactory(cfg, cfg.Topics[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(badClient.CloseAllowingRebalance)
	authCtx, cancelAuth := context.WithTimeout(ctx, 10*time.Second)
	defer cancelAuth()
	if err := badClient.client.Ping(authCtx); !errors.Is(err, kerr.SaslAuthenticationFailed) {
		t.Fatalf("invalid password returned %v, want SASL authentication failure", err)
	}
}
