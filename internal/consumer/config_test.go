package consumer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		env      map[string]string
		check    func(Config) bool
		wantErr  string
	}{
		{
			name: "defaults",
			check: func(cfg Config) bool {
				return !cfg.Kafka.Enabled && cfg.Kafka.Brokers[0] == "localhost:9092" &&
					cfg.Kafka.AutoOffsetReset == "earliest" && cfg.Kafka.FetchMinBytes == 1 &&
					cfg.Kafka.FetchMaxBytes == 50*1024*1024 && cfg.Kafka.MaxPollRecords == 50 &&
					cfg.Kafka.MaxPollIntervalMS == 300_000 && cfg.Kafka.SessionTimeoutMS == 45_000 &&
					cfg.Kafka.CompressionType == "lz4" && cfg.Topics[0] == (TopicConfig{Name: "greetings", Consumers: 1})
			},
		},
		{
			name: "all dotted Kafka-native keys",
			contents: "kafka:\n  enabled: true\n  auto.offset.reset: latest\n  fetch.min.bytes: 0\n" +
				"  fetch.max.bytes: 1048576\n  max.poll.records: 7\n  max.poll.interval.ms: 120000\n" +
				"  session.timeout.ms: 9000\n  compression.type: zstd\n",
			check: func(cfg Config) bool {
				return cfg.Kafka.AutoOffsetReset == "latest" && cfg.Kafka.FetchMinBytes == 0 &&
					cfg.Kafka.FetchMaxBytes == 1_048_576 && cfg.Kafka.MaxPollRecords == 7 &&
					cfg.Kafka.MaxPollIntervalMS == 120_000 && cfg.Kafka.SessionTimeoutMS == 9_000 &&
					cfg.Kafka.CompressionType == "zstd"
			},
		},
		{
			name: "durations and existing nested settings still decode",
			contents: "kafka:\n  enabled: true\n  brokers: [broker.example:9093]\n  group_id: app\n" +
				"  auth: plain\n  username: alice\n  password: secret\n  max.poll.records: 8\n" +
				"  tls:\n    enabled: true\n    ca_file: /tmp/ca.pem\n    server_name: kafka.example\n" +
				"topics:\n  - name: events\n    consumers: 2\nprocessing_concurrency: 3\nbatch_timeout: 12s\ncommit_timeout: 2s\n",
			check: func(cfg Config) bool {
				return cfg.Kafka.Brokers[0] == "broker.example:9093" && cfg.Kafka.GroupID == "app" &&
					cfg.Kafka.Auth == AuthPlain && cfg.Kafka.TLS.CAFile == "/tmp/ca.pem" &&
					cfg.Kafka.TLS.ServerName == "kafka.example" && cfg.Kafka.MaxPollRecords == 8 &&
					cfg.Topics[0] == (TopicConfig{Name: "events", Consumers: 2}) &&
					cfg.ProcessingConcurrency == 3 && cfg.BatchTimeout == 12*time.Second && cfg.CommitTimeout == 2*time.Second
			},
		},
		{
			name:     "nested spelling of literal dotted key rejected",
			contents: "kafka:\n  max:\n    poll:\n      records: 7\n",
			wantErr:  "invalid keys",
		},
		{
			name:     "environment secrets override file values",
			contents: "kafka:\n  auth: scram-sha-512\n  username: file-user\n  password: file-pass\n",
			env:      map[string]string{"KAFKA_USERNAME": "env-user", "KAFKA_PASSWORD": "env-pass"},
			check:    func(cfg Config) bool { return cfg.Kafka.Username == "env-user" && cfg.Kafka.Password == "env-pass" },
		},
		{
			name:     "unknown key rejected",
			contents: "unknown: true\n",
			wantErr:  "invalid keys",
		},
		{
			name:     "unknown nested key rejected",
			contents: "kafka:\n  passwordd: secret\n",
			wantErr:  "invalid keys",
		},
		{
			name:     "removed batch size key rejected",
			contents: "batch_size: 8\n",
			wantErr:  "invalid keys",
		},
		{
			name:     "malformed YAML rejected",
			contents: "topics: [\n",
			wantErr:  "load config",
		},
		{
			name:     "invalid scalar type rejected",
			contents: "kafka:\n  fetch.max.bytes: not-a-number\n",
			wantErr:  "fetch.max.bytes",
		},
		{
			name: "disabled skips operational validation",
			contents: "kafka:\n  enabled: false\n  auth: aws-iam\n  brokers: []\n" +
				"  tls:\n    enabled: false\n  fetch.min.bytes: -1\n  fetch.max.bytes: 0\n" +
				"  max.poll.records: 0\n  compression.type: invalid\n",
			check: func(cfg Config) bool {
				return !cfg.Kafka.Enabled && cfg.Kafka.Auth == AuthAWSIAM && cfg.Kafka.FetchMinBytes == -1
			},
		},
		{
			name:     "invalid enabled flag rejected",
			contents: "kafka:\n  enabled: not-a-bool\n",
			wantErr:  "enabled",
		},
		{
			name:     "invalid auth rejected when enabled",
			contents: "kafka:\n  enabled: true\n  auth: bearer\n",
			wantErr:  "unsupported value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tt.contents), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := loadConfig(path, func(key string) (string, bool) {
				value, ok := tt.env[key]
				return value, ok
			})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("loadConfig() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadConfig() error = %v", err)
			}
			if !tt.check(cfg) {
				t.Fatalf("loaded config does not match expected behavior: %+v", cfg)
			}
		})
	}
}

func TestLoadConfigPublicEntryPoints(t *testing.T) {
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("topics:\n  - name: public-api\n    consumers: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("LoadConfig", func(t *testing.T) {
		cfg, err := LoadConfig(configPath)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Topics[0].Name != "public-api" {
			t.Fatalf("topic = %q, want public-api", cfg.Topics[0].Name)
		}
	})

	t.Run("default config.yaml", func(t *testing.T) {
		t.Chdir(configDir)
		cfg, err := LoadConfig("")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Topics[0].Name != "public-api" {
			t.Fatalf("topic = %q, want public-api", cfg.Topics[0].Name)
		}
	})

	t.Run("missing default config", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := LoadConfig("")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Kafka.Enabled || cfg.Kafka.FetchMaxBytes != DefaultConfig().Kafka.FetchMaxBytes {
			t.Fatalf("missing default config = %+v", cfg)
		}
	})
}

func TestLoadConfigMissingFile(t *testing.T) {
	_, err := loadConfig(filepath.Join(t.TempDir(), "missing.yaml"), func(string) (string, bool) { return "", false })
	if err == nil || !strings.Contains(err.Error(), "load config") {
		t.Fatalf("loadConfig() error = %v, want missing-file error", err)
	}
}

func TestConfigAllowsExplicitLocalPlaintext(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Kafka.TLS.Enabled = false
	cfg.Kafka.AllowInsecure = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected explicitly enabled local plaintext: %v", err)
	}
}

func TestConfigValidateKafkaNativeOptions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{name: "unknown offset reset", mutate: func(cfg *Config) { cfg.Kafka.AutoOffsetReset = "beginning" }, want: "auto.offset.reset"},
		{name: "negative minimum fetch bytes", mutate: func(cfg *Config) { cfg.Kafka.FetchMinBytes = -1 }, want: "fetch.min.bytes"},
		{name: "minimum exceeds maximum fetch bytes", mutate: func(cfg *Config) { cfg.Kafka.FetchMinBytes = cfg.Kafka.FetchMaxBytes + 1 }, want: "fetch.min.bytes"},
		{name: "zero maximum fetch bytes", mutate: func(cfg *Config) { cfg.Kafka.FetchMaxBytes = 0 }, want: "fetch.max.bytes"},
		{name: "maximum fetch bytes over cap", mutate: func(cfg *Config) { cfg.Kafka.FetchMaxBytes = 1<<30 - 1<<20 + 1 }, want: "fetch.max.bytes"},
		{name: "zero max poll records", mutate: func(cfg *Config) { cfg.Kafka.MaxPollRecords = 0 }, want: "max.poll.records"},
		{name: "max poll interval below minimum", mutate: func(cfg *Config) { cfg.Kafka.MaxPollIntervalMS = 99 }, want: "max.poll.interval.ms"},
		{name: "max poll interval over int32", mutate: func(cfg *Config) { cfg.Kafka.MaxPollIntervalMS = 1<<31 - 1 + 1 }, want: "max.poll.interval.ms"},
		{name: "session timeout below minimum", mutate: func(cfg *Config) { cfg.Kafka.SessionTimeoutMS = 99 }, want: "session.timeout.ms"},
		{name: "session timeout over int32", mutate: func(cfg *Config) { cfg.Kafka.SessionTimeoutMS = 1<<31 - 1 + 1 }, want: "session.timeout.ms"},
		{name: "unknown compression type", mutate: func(cfg *Config) { cfg.Kafka.CompressionType = "brotli" }, want: "compression.type"},
		{name: "zero batch timeout", mutate: func(cfg *Config) { cfg.BatchTimeout = 0 }, want: "batch_timeout must be positive"},
		{name: "zero commit timeout", mutate: func(cfg *Config) { cfg.CommitTimeout = 0 }, want: "commit_timeout must be positive"},
		{name: "long batch and commit timeouts allowed", mutate: func(cfg *Config) {
			cfg.BatchTimeout = 30 * 24 * time.Hour
			cfg.CommitTimeout = 30 * 24 * time.Hour
		}, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestConfigValidateExistingRules(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{name: "empty brokers", mutate: func(cfg *Config) { cfg.Kafka.Brokers = nil }, want: "at least one broker"},
		{name: "bad broker address", mutate: func(cfg *Config) { cfg.Kafka.Brokers = []string{"broker"} }, want: "invalid host:port"},
		{name: "bad broker port", mutate: func(cfg *Config) { cfg.Kafka.Brokers = []string{"broker:70000"} }, want: "invalid port"},
		{name: "empty group", mutate: func(cfg *Config) { cfg.Kafka.GroupID = " " }, want: "group_id must not be empty"},
		{name: "zero concurrency", mutate: func(cfg *Config) { cfg.ProcessingConcurrency = 0 }, want: "processing_concurrency must be positive"},
		{name: "no topics", mutate: func(cfg *Config) { cfg.Topics = nil }, want: "at least one topic"},
		{name: "bad topic", mutate: func(cfg *Config) { cfg.Topics[0].Name = "bad topic" }, want: "invalid Kafka topic name"},
		{name: "duplicate topic", mutate: func(cfg *Config) { cfg.Topics = append(cfg.Topics, cfg.Topics[0]) }, want: "duplicate topic"},
		{name: "zero topic clients", mutate: func(cfg *Config) { cfg.Topics[0].Consumers = 0 }, want: "consumers must be positive"},
		{name: "derived group too long", mutate: func(cfg *Config) { cfg.Kafka.GroupID = strings.Repeat("g", 249) }, want: "derived consumer group ID"},
		{name: "tls required unless local testing allowed", mutate: func(cfg *Config) { cfg.Kafka.TLS.Enabled = false }, want: "only when kafka.allow_insecure is true"},
		{name: "IAM requires TLS even in local mode", mutate: func(cfg *Config) {
			cfg.Kafka.Auth = AuthAWSIAM
			cfg.Kafka.TLS.Enabled = false
			cfg.Kafka.AllowInsecure = true
		}, want: "aws-iam requires TLS"},
		{name: "IAM rejects password fields", mutate: func(cfg *Config) { cfg.Kafka.Auth = AuthAWSIAM; cfg.Kafka.Password = "secret" }, want: "must be empty for aws-iam"},
		{name: "plain requires password", mutate: func(cfg *Config) { cfg.Kafka.Auth = AuthPlain; cfg.Kafka.Username = "user" }, want: "required for plain"},
		{name: "SCRAM SHA-256 requires credentials", mutate: func(cfg *Config) { cfg.Kafka.Auth = AuthSCRAMSHA256 }, want: "required for scram-sha-256"},
		{name: "SCRAM SHA-512 requires credentials", mutate: func(cfg *Config) { cfg.Kafka.Auth = AuthSCRAMSHA512 }, want: "required for scram-sha-512"},
		{name: "unknown auth", mutate: func(cfg *Config) { cfg.Kafka.Auth = "other" }, want: "unsupported value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(&cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}
