package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/twmb/franz-go/pkg/kgo"

	"example.com/go-backend-example/internal/consumer"
)

func TestCommitEmptyBatchIsNoop(t *testing.T) {
	if err := (&Client{}).Commit(context.Background()); err != nil {
		t.Fatalf("Commit() error = %v for an empty batch", err)
	}
}

func TestCloneBytesPreservesNil(t *testing.T) {
	if got := cloneBytes(nil); got != nil {
		t.Fatalf("cloneBytes(nil) = %v, want nil", got)
	}
}

func TestMessageFromRecordPreservesHeadersAndCopiesBytes(t *testing.T) {
	key := []byte("key")
	value := []byte("value")
	headerValue := []byte("first")
	createdAt := time.Unix(123, 456)
	record := &kgo.Record{
		Topic:       "events",
		Partition:   2,
		LeaderEpoch: 7,
		Offset:      19,
		Key:         key,
		Value:       value,
		Timestamp:   createdAt,
		Headers: []kgo.RecordHeader{
			{Key: "trace", Value: headerValue},
			{Key: "trace", Value: []byte("second")},
		},
	}
	message := messageFromRecord(record)
	if message.Topic != record.Topic || message.Partition != record.Partition || message.LeaderEpoch != record.LeaderEpoch || message.Offset != record.Offset || !message.Time.Equal(createdAt) {
		t.Fatalf("message metadata = %+v, want Kafka record metadata", message)
	}
	if string(message.Key) != "key" || string(message.Value) != "value" || len(message.Headers) != 2 || string(message.Headers[0].Value) != "first" || string(message.Headers[1].Value) != "second" {
		t.Fatalf("message payload or duplicate headers = %+v", message)
	}
	key[0], value[0], headerValue[0] = 'X', 'X', 'X'
	if string(message.Key) != "key" || string(message.Value) != "value" || string(message.Headers[0].Value) != "first" {
		t.Fatal("message data aliases Kafka record bytes")
	}
}

func TestCommitMetadataRoundTripPreservesLeaderEpoch(t *testing.T) {
	for _, epoch := range []int32{7, -1} {
		original := &kgo.Record{Topic: "events", Partition: 2, LeaderEpoch: epoch, Offset: 100}
		records := recordsForCommit([]consumer.Message{messageFromRecord(original)})
		if len(records) != 1 {
			t.Fatalf("commit metadata returned %d records, want 1", len(records))
		}
		if got := *records[0]; got.Topic != original.Topic || got.Partition != original.Partition || got.Offset != original.Offset || got.LeaderEpoch != original.LeaderEpoch {
			t.Errorf("commit metadata = %+v, want %+v", got, *original)
		}
	}
}

func TestBuildTLSConfig(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		got, err := buildTLSConfig(consumer.TLSConfig{})
		if err != nil || got != nil {
			t.Fatalf("buildTLSConfig() = %v, %v; want nil, nil", got, err)
		}
	})
	t.Run("system roots and server name", func(t *testing.T) {
		got, err := buildTLSConfig(consumer.TLSConfig{Enabled: true, ServerName: "kafka.example"})
		if err != nil {
			t.Fatal(err)
		}
		if got.MinVersion != tls.VersionTLS12 || got.ServerName != "kafka.example" {
			t.Fatalf("TLS config = %+v, want TLS 1.2 and server name", got)
		}
	})
	t.Run("missing CA file", func(t *testing.T) {
		_, err := buildTLSConfig(consumer.TLSConfig{Enabled: true, CAFile: filepath.Join(t.TempDir(), "missing.pem")})
		if err == nil {
			t.Fatal("buildTLSConfig() error = nil, want missing CA file error")
		}
	})
	t.Run("invalid CA PEM", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ca.pem")
		if err := os.WriteFile(path, []byte("not a certificate"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := buildTLSConfig(consumer.TLSConfig{Enabled: true, CAFile: path})
		if err == nil {
			t.Fatal("buildTLSConfig() error = nil, want invalid certificate error")
		}
	})
}

func TestNewClientAppliesKafkaNativeOptions(t *testing.T) {
	tests := []struct {
		name            string
		offsetReset     string
		compressionType string
		fetchMaxBytes   int
		sessionTimeout  int
		pollInterval    int
	}{
		{name: "earliest lz4 defaults", offsetReset: "earliest", compressionType: "lz4", fetchMaxBytes: 50 * 1024 * 1024, sessionTimeout: 45_000, pollInterval: 300_000},
		{name: "latest gzip", offsetReset: "latest", compressionType: "gzip", fetchMaxBytes: 1_048_576, sessionTimeout: 9_000, pollInterval: 120_000},
		{name: "none snappy", offsetReset: "none", compressionType: "snappy", fetchMaxBytes: 1<<30 - 1<<20, sessionTimeout: 100, pollInterval: 100},
		{name: "zstd", offsetReset: "earliest", compressionType: "zstd", fetchMaxBytes: 50 * 1024 * 1024, sessionTimeout: 45_000, pollInterval: 300_000},
		{name: "no compression", offsetReset: "earliest", compressionType: "none", fetchMaxBytes: 50 * 1024 * 1024, sessionTimeout: 45_000, pollInterval: 300_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := consumer.DefaultConfig()
			cfg.Kafka.AutoOffsetReset = tt.offsetReset
			cfg.Kafka.CompressionType = tt.compressionType
			cfg.Kafka.FetchMinBytes = 0
			cfg.Kafka.FetchMaxBytes = tt.fetchMaxBytes
			cfg.Kafka.SessionTimeoutMS = tt.sessionTimeout
			cfg.Kafka.MaxPollIntervalMS = tt.pollInterval
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			client, err := newClient(cfg, cfg.Topics[0], nil)
			if err != nil {
				t.Fatalf("newClient() error = %v", err)
			}
			defer client.client.Close()

			wantOffset := kgo.NewOffset().AtStart()
			switch tt.offsetReset {
			case "latest":
				wantOffset = kgo.NewOffset().AtEnd()
			case "none":
				wantOffset = kgo.NoResetOffset()
			}
			if got := client.client.OptValue(kgo.ConsumeResetOffset); !reflect.DeepEqual(got, wantOffset) {
				t.Errorf("ConsumeResetOffset = %#v, want %#v", got, wantOffset)
			}
			if tt.offsetReset == "none" {
				wantOffset = kgo.NewOffset().AtCommitted()
			}
			if got := client.client.OptValue(kgo.ConsumeStartOffset); !reflect.DeepEqual(got, wantOffset) {
				t.Errorf("ConsumeStartOffset = %#v, want %#v", got, wantOffset)
			}
			if got := client.client.OptValue(kgo.FetchMinBytes); got != int32(0) {
				t.Errorf("FetchMinBytes = %v, want 0", got)
			}
			if got := client.client.OptValue(kgo.FetchMaxBytes); got != int32(tt.fetchMaxBytes) {
				t.Errorf("FetchMaxBytes = %v, want %d", got, tt.fetchMaxBytes)
			}
			wantBrokerMaxReadBytes := max(100*1024*1024, tt.fetchMaxBytes+1024*1024)
			if got := client.client.OptValue(kgo.BrokerMaxReadBytes); got != int32(wantBrokerMaxReadBytes) {
				t.Errorf("BrokerMaxReadBytes = %v, want %d", got, wantBrokerMaxReadBytes)
			}
			if got := client.client.OptValue(kgo.RebalanceTimeout); got != time.Duration(tt.pollInterval)*time.Millisecond {
				t.Errorf("RebalanceTimeout = %v, want %v", got, time.Duration(tt.pollInterval)*time.Millisecond)
			}
			if got := client.client.OptValue(kgo.SessionTimeout); got != time.Duration(tt.sessionTimeout)*time.Millisecond {
				t.Errorf("SessionTimeout = %v, want %v", got, time.Duration(tt.sessionTimeout)*time.Millisecond)
			}
			wantHeartbeat := min(3*time.Second, time.Duration(tt.sessionTimeout)*time.Millisecond/3)
			if got := client.client.OptValue(kgo.HeartbeatInterval); got != wantHeartbeat {
				t.Errorf("HeartbeatInterval = %v, want %v", got, wantHeartbeat)
			}
			wantCompression := []kgo.CompressionCodec{compressionCodec(tt.compressionType)}
			if got := client.client.OptValues(kgo.ProducerBatchCompression); len(got) != 1 || !reflect.DeepEqual(got[0], wantCompression) {
				t.Errorf("ProducerBatchCompression = %#v, want %#v", got, wantCompression)
			}
			if got := client.client.OptValue(kgo.DisableAutoCommit); got != true {
				t.Errorf("DisableAutoCommit = %v, want true", got)
			}
			if got := client.client.OptValue(kgo.BlockRebalanceOnPoll); got != true {
				t.Errorf("BlockRebalanceOnPoll = %v, want true", got)
			}
		})
	}
}

func TestNewClientFactoryValidatesContextAndConfig(t *testing.T) {
	if _, err := NewClientFactory(nil, consumer.DefaultConfig()); err == nil {
		t.Fatal("NewClientFactory() error = nil for nil context")
	}
	cfg := consumer.DefaultConfig()
	cfg.Topics = nil
	if _, err := NewClientFactory(context.Background(), cfg); err == nil {
		t.Fatal("NewClientFactory() error = nil for invalid config")
	}
}

func TestAuthMechanism(t *testing.T) {
	for _, test := range []struct {
		name     string
		config   consumer.KafkaConfig
		wantName string
	}{
		{name: "none", config: consumer.KafkaConfig{Auth: consumer.AuthNone}},
		{name: "plain", config: consumer.KafkaConfig{Auth: consumer.AuthPlain, Username: "user", Password: "pass"}, wantName: "PLAIN"},
		{name: "scram sha 256", config: consumer.KafkaConfig{Auth: consumer.AuthSCRAMSHA256, Username: "user", Password: "pass"}, wantName: "SCRAM-SHA-256"},
		{name: "scram sha 512", config: consumer.KafkaConfig{Auth: consumer.AuthSCRAMSHA512, Username: "user", Password: "pass"}, wantName: "SCRAM-SHA-512"},
		{name: "unknown", config: consumer.KafkaConfig{Auth: "unknown"}, wantName: "error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mechanism, err := authMechanism(context.Background(), test.config)
			if test.wantName == "error" {
				if err == nil {
					t.Fatal("authMechanism() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.wantName == "" {
				if mechanism != nil {
					t.Fatalf("authMechanism() = %v, want nil", mechanism)
				}
				return
			}
			if mechanism == nil || mechanism.Name() != test.wantName {
				t.Fatalf("auth mechanism = %v, want name %q", mechanism, test.wantName)
			}
		})
	}
}

func TestAuthMechanismAWSIAMLoadsDefaultConfig(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret-key")
	mechanism, err := authMechanism(context.Background(), consumer.KafkaConfig{Auth: consumer.AuthAWSIAM})
	if err != nil {
		t.Fatal(err)
	}
	if mechanism == nil {
		t.Fatal("authMechanism() returned nil AWS IAM mechanism")
	}
}

func TestManagedIAMRetrievesCredentialsForEachSession(t *testing.T) {
	provider := &rotatingCredentials{}
	mechanism := managedIAM(provider)
	for range 2 {
		_, _, err := mechanism.Authenticate(context.Background(), "b-1.example.kafka.us-east-1.amazonaws.com:9098")
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := provider.calls.Load(); got != 2 {
		t.Fatalf("credential provider called %d times, want once per SASL session", got)
	}
}

type rotatingCredentials struct {
	calls atomic.Int32
}

func (p *rotatingCredentials) Retrieve(context.Context) (awsSDK.Credentials, error) {
	n := p.calls.Add(1)
	return awsSDK.Credentials{
		AccessKeyID:     "access-key",
		SecretAccessKey: "secret-key",
		SessionToken:    "session-token",
		CanExpire:       true,
		Expires:         time.Now().Add(time.Hour).Add(-time.Duration(n) * time.Second),
	}, nil
}

func TestManagedIAMRejectsEmptyCredentials(t *testing.T) {
	mechanism := managedIAM(emptyCredentials{})
	_, _, err := mechanism.Authenticate(context.Background(), "b-1.example.kafka.us-east-1.amazonaws.com:9098")
	if err == nil {
		t.Fatal("Authenticate() error = nil, want empty credentials error")
	}
}

func TestManagedIAMReturnsCredentialErrors(t *testing.T) {
	credentialError := errors.New("credential retrieval failed")
	mechanism := managedIAM(failingCredentials{err: credentialError})
	_, _, err := mechanism.Authenticate(context.Background(), "b-1.example.kafka.us-east-1.amazonaws.com:9098")
	if !errors.Is(err, credentialError) {
		t.Fatalf("Authenticate() error = %v, want credential retrieval error", err)
	}
}

type emptyCredentials struct{}

func (emptyCredentials) Retrieve(context.Context) (awsSDK.Credentials, error) {
	return awsSDK.Credentials{}, nil
}

type failingCredentials struct {
	err error
}

func (p failingCredentials) Retrieve(context.Context) (awsSDK.Credentials, error) {
	return awsSDK.Credentials{}, p.err
}
