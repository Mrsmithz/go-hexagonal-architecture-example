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
		want     func(Config) bool
		wantErr  string
	}{
		{
			name:     "defaults with topic override",
			contents: "topics:\n  - name: orders\n    consumers: 3\n",
			want: func(cfg Config) bool {
				return !cfg.Kafka.Enabled && cfg.Kafka.Brokers[0] == "localhost:9092" && cfg.Kafka.TLS.Enabled && cfg.Topics[0] == (TopicConfig{Name: "orders", Consumers: 3}) && cfg.BatchSize == 50 && cfg.ProcessingConcurrency == 10
			},
		},
		{
			name:     "environment secrets override file values",
			contents: "kafka:\n  enabled: true\n  auth: scram-sha-512\n  username: file-user\n  password: file-pass\ntopics:\n  - name: orders\n    consumers: 1\n",
			env:      map[string]string{"KAFKA_USERNAME": "env-user", "KAFKA_PASSWORD": "env-pass"},
			want: func(cfg Config) bool {
				return cfg.Kafka.Username == "env-user" && cfg.Kafka.Password == "env-pass"
			},
		},
		{
			name:     "durations and kafka options",
			contents: "kafka:\n  enabled: true\n  brokers: [broker.example:9093]\n  group_id: app\n  auth: plain\n  tls:\n    enabled: true\n    ca_file: /tmp/ca.pem\n    server_name: kafka.example\n  username: alice\n  password: secret\ntopics:\n  - name: events\n    consumers: 2\nbatch_size: 8\nprocessing_concurrency: 3\nbatch_timeout: 12s\ncommit_timeout: 2s\n",
			want: func(cfg Config) bool {
				return cfg.Kafka.Enabled && cfg.Kafka.Brokers[0] == "broker.example:9093" && cfg.Kafka.Auth == AuthPlain && cfg.Kafka.TLS.CAFile == "/tmp/ca.pem" && cfg.Kafka.TLS.ServerName == "kafka.example" && cfg.BatchSize == 8 && cfg.ProcessingConcurrency == 3 && cfg.BatchTimeout == 12*time.Second && cfg.CommitTimeout == 2*time.Second
			},
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
			name:     "malformed yaml rejected",
			contents: "topics: [\n",
			wantErr:  "load config",
		},
		{
			name:     "invalid scalar type rejected",
			contents: "batch_size: not-a-number\n",
			wantErr:  "batch_size",
		},
		{
			name:     "disabled skips operational validation",
			contents: "kafka:\n  enabled: false\n  auth: aws-iam\n  brokers: []\n  tls:\n    enabled: false\n    ca_file: /not-readable\nbatch_size: 0\n",
			want:     func(cfg Config) bool { return !cfg.Kafka.Enabled && cfg.Kafka.Auth == AuthAWSIAM && cfg.BatchSize == 0 },
		},
		{
			name:     "omitted flag disables Kafka",
			contents: "kafka:\n  auth: plain\n  username: incomplete\n",
			want:     func(cfg Config) bool { return !cfg.Kafka.Enabled && cfg.Kafka.Password == "" },
		},
		{
			name:     "invalid enabled flag rejected",
			contents: "kafka:\n  enabled: not-a-bool\n",
			wantErr:  "enabled",
		},
		{
			name:     "invalid auth rejected",
			contents: "kafka:\n  enabled: true\n  auth: bearer\n",
			wantErr:  "unsupported value",
		},
		{
			name:     "incomplete credentials rejected",
			contents: "kafka:\n  enabled: true\n  auth: plain\n  username: only-user\n",
			wantErr:  "required for plain",
		},
		{
			name:     "credentials with none rejected",
			contents: "kafka:\n  enabled: true\n  username: user\n  password: pass\n",
			wantErr:  "require a username/password auth mode",
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
			if !tt.want(cfg) {
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

	t.Run("missing default disables Kafka", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := LoadConfig("")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Kafka.Enabled || cfg.BatchSize != DefaultConfig().BatchSize {
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

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{name: "empty brokers", mutate: func(cfg *Config) { cfg.Kafka.Brokers = nil }, want: "at least one broker"},
		{name: "bad broker address", mutate: func(cfg *Config) { cfg.Kafka.Brokers = []string{"broker"} }, want: "invalid host:port"},
		{name: "bad broker port", mutate: func(cfg *Config) { cfg.Kafka.Brokers = []string{"broker:70000"} }, want: "invalid port"},
		{name: "empty group", mutate: func(cfg *Config) { cfg.Kafka.GroupID = " " }, want: "group_id must not be empty"},
		{name: "zero batch size", mutate: func(cfg *Config) { cfg.BatchSize = 0 }, want: "batch_size must be positive"},
		{name: "zero concurrency", mutate: func(cfg *Config) { cfg.ProcessingConcurrency = 0 }, want: "processing_concurrency must be positive"},
		{name: "zero batch timeout", mutate: func(cfg *Config) { cfg.BatchTimeout = 0 }, want: "batch_timeout must be positive"},
		{name: "zero commit timeout", mutate: func(cfg *Config) { cfg.CommitTimeout = 0 }, want: "commit_timeout must be positive"},
		{name: "duration overflow", mutate: func(cfg *Config) { cfg.BatchTimeout = time.Duration(1<<63 - 1); cfg.CommitTimeout = time.Second }, want: "exceed the supported duration"},
		{name: "rebalance timeout exceeds Kafka limit", mutate: func(cfg *Config) { cfg.BatchTimeout = 30 * 24 * time.Hour }, want: "Kafka's rebalance timeout limit"},
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
