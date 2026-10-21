package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	awsSASL "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"example.com/go-backend-example/internal/consumer"
)

type Client struct {
	client       *kgo.Client
	closeTimeout time.Duration
}

func NewClientFactory(ctx context.Context, cfg consumer.Config) (func(consumer.Config, consumer.TopicConfig) (*Client, error), error) {
	if ctx == nil {
		return nil, fmt.Errorf("Kafka client factory context must not be nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Kafka config: %w", err)
	}
	mechanism, err := authMechanism(ctx, cfg.Kafka)
	if err != nil {
		return nil, err
	}
	return func(clientConfig consumer.Config, topic consumer.TopicConfig) (*Client, error) {
		return newClient(clientConfig, topic, mechanism)
	}, nil
}

func newClient(cfg consumer.Config, topic consumer.TopicConfig, mechanism sasl.Mechanism) (*Client, error) {
	tlsConfig, err := buildTLSConfig(cfg.Kafka.TLS)
	if err != nil {
		return nil, err
	}
	sessionTimeout := time.Duration(cfg.Kafka.SessionTimeoutMS) * time.Millisecond
	heartbeatInterval := min(3*time.Second, sessionTimeout/3)
	brokerMaxReadBytes := max(100*1024*1024, cfg.Kafka.FetchMaxBytes+1024*1024)
	resetOffset := kgo.NewOffset().AtStart()
	switch cfg.Kafka.AutoOffsetReset {
	case "latest":
		resetOffset = kgo.NewOffset().AtEnd()
	case "none":
		resetOffset = kgo.NoResetOffset()
	}
	startOffset := resetOffset
	if cfg.Kafka.AutoOffsetReset == "none" {
		startOffset = kgo.NewOffset().AtCommitted()
	}
	compression := compressionCodec(cfg.Kafka.CompressionType)
	options := []kgo.Opt{
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ConsumerGroup(cfg.Kafka.GroupID + "." + topic.Name),
		kgo.ConsumeTopics(topic.Name),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeStartOffset(startOffset),
		kgo.ConsumeResetOffset(resetOffset),
		kgo.FetchMinBytes(int32(cfg.Kafka.FetchMinBytes)),
		kgo.FetchMaxBytes(int32(cfg.Kafka.FetchMaxBytes)),
		kgo.BrokerMaxReadBytes(int32(brokerMaxReadBytes)),
		kgo.RebalanceTimeout(time.Duration(cfg.Kafka.MaxPollIntervalMS) * time.Millisecond),
		kgo.SessionTimeout(sessionTimeout),
		kgo.HeartbeatInterval(heartbeatInterval),
		kgo.ProducerBatchCompression(compression),
	}
	if tlsConfig != nil {
		options = append(options, kgo.DialTLSConfig(tlsConfig))
	}
	if mechanism != nil {
		options = append(options, kgo.SASL(mechanism))
	}
	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka client for topic %q: %w", topic.Name, err)
	}
	return &Client{client: client, closeTimeout: topic.CommitTimeout}, nil
}

func (c *Client) Poll(ctx context.Context, maxRecords int) ([]consumer.Message, error) {
	fetches := c.client.PollRecords(ctx, maxRecords)
	if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
		fetchErr := fetchErrors[0]
		return nil, fmt.Errorf("fetch %s partition %d: %w", fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
	}
	records := fetches.Records()
	messages := make([]consumer.Message, len(records))
	for i, record := range records {
		messages[i] = messageFromRecord(record)
	}
	return messages, nil
}

func (c *Client) Commit(ctx context.Context, messages ...consumer.Message) error {
	if len(messages) == 0 {
		return nil
	}
	return c.client.CommitRecords(ctx, recordsForCommit(messages)...)
}

func (c *Client) AllowRebalance() {
	c.client.AllowRebalance()
}

func (c *Client) CloseAllowingRebalance() {
	c.client.AllowRebalance()
	ctx, cancel := context.WithTimeout(context.Background(), c.closeTimeout)
	defer cancel()
	_ = c.client.LeaveGroupContext(ctx)
	c.client.Close()
}

func compressionCodec(compressionType string) kgo.CompressionCodec {
	switch compressionType {
	case "none":
		return kgo.NoCompression()
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "zstd":
		return kgo.ZstdCompression()
	default:
		return kgo.Lz4Compression()
	}
}

func authMechanism(ctx context.Context, cfg consumer.KafkaConfig) (sasl.Mechanism, error) {
	switch cfg.Auth {
	case consumer.AuthNone:
		return nil, nil
	case consumer.AuthAWSIAM:
		awsConfig, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("load AWS default credential chain: %w", err)
		}
		return managedIAM(awsConfig.Credentials), nil
	case consumer.AuthPlain:
		return plain.Plain(func(context.Context) (plain.Auth, error) {
			return plain.Auth{User: cfg.Username, Pass: cfg.Password}, nil
		}), nil
	case consumer.AuthSCRAMSHA256:
		return scram.Sha256(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: cfg.Username, Pass: cfg.Password}, nil
		}), nil
	case consumer.AuthSCRAMSHA512:
		return scram.Sha512(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: cfg.Username, Pass: cfg.Password}, nil
		}), nil
	default:
		return nil, fmt.Errorf("unsupported Kafka authentication mode %q", cfg.Auth)
	}
}

func managedIAM(provider awsSDK.CredentialsProvider) sasl.Mechanism {
	return awsSASL.ManagedStreamingIAM(func(ctx context.Context) (awsSASL.Auth, error) {
		credentials, err := provider.Retrieve(ctx)
		if err != nil {
			return awsSASL.Auth{}, fmt.Errorf("retrieve AWS credentials: %w", err)
		}
		if credentials.AccessKeyID == "" || credentials.SecretAccessKey == "" {
			return awsSASL.Auth{}, fmt.Errorf("AWS credentials are missing an access key or secret key")
		}
		return awsSASL.Auth{
			AccessKey:    credentials.AccessKeyID,
			SecretKey:    credentials.SecretAccessKey,
			SessionToken: credentials.SessionToken,
		}, nil
	})
}

func buildTLSConfig(cfg consumer.TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.ServerName}
	if cfg.CAFile == "" {
		return tlsConfig, nil
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	caPEM, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read Kafka CA file: %w", err)
	}
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("Kafka CA file contains no valid certificates")
	}
	tlsConfig.RootCAs = roots
	return tlsConfig, nil
}

func messageFromRecord(record *kgo.Record) consumer.Message {
	headers := make([]consumer.Header, len(record.Headers))
	for i, header := range record.Headers {
		headers[i] = consumer.Header{Key: header.Key, Value: cloneBytes(header.Value)}
	}
	return consumer.Message{
		Topic:       record.Topic,
		Key:         cloneBytes(record.Key),
		Value:       cloneBytes(record.Value),
		Headers:     headers,
		Partition:   record.Partition,
		LeaderEpoch: record.LeaderEpoch,
		Offset:      record.Offset,
		Time:        record.Timestamp,
	}
}

func recordsForCommit(messages []consumer.Message) []*kgo.Record {
	records := make([]*kgo.Record, len(messages))
	for i, message := range messages {
		records[i] = &kgo.Record{Topic: message.Topic, Partition: message.Partition, LeaderEpoch: message.LeaderEpoch, Offset: message.Offset}
	}
	return records
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}
