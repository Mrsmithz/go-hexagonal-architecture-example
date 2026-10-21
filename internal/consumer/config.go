package consumer

import (
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const (
	AuthNone        = "none"
	AuthAWSIAM      = "aws-iam"
	AuthPlain       = "plain"
	AuthSCRAMSHA256 = "scram-sha-256"
	AuthSCRAMSHA512 = "scram-sha-512"
)

type Config struct {
	Kafka  KafkaConfig   `koanf:"kafka"`
	Topics []TopicConfig `koanf:"topics"`
}

type KafkaConfig struct {
	Enabled           bool      `koanf:"enabled"`
	Brokers           []string  `koanf:"brokers"`
	GroupID           string    `koanf:"group_id"`
	Auth              string    `koanf:"auth"`
	Username          string    `koanf:"username"`
	Password          string    `koanf:"password"`
	TLS               TLSConfig `koanf:"tls"`
	AllowInsecure     bool      `koanf:"allow_insecure"`
	AutoOffsetReset   string    `koanf:"auto.offset.reset"`
	FetchMinBytes     int       `koanf:"fetch.min.bytes"`
	FetchMaxBytes     int       `koanf:"fetch.max.bytes"`
	MaxPollRecords    int       `koanf:"max.poll.records"`
	MaxPollIntervalMS int       `koanf:"max.poll.interval.ms"`
	SessionTimeoutMS  int       `koanf:"session.timeout.ms"`
	// CompressionType configures producer batch compression; it does not affect consumer decompression.
	CompressionType string `koanf:"compression.type"`
}

type TLSConfig struct {
	Enabled    bool   `koanf:"enabled"`
	CAFile     string `koanf:"ca_file"`
	ServerName string `koanf:"server_name"`
}

type TopicConfig struct {
	Name                  string        `koanf:"name"`
	Consumers             int           `koanf:"consumers"`
	ProcessingConcurrency int           `koanf:"processing_concurrency"`
	BatchTimeout          time.Duration `koanf:"batch_timeout"`
	CommitTimeout         time.Duration `koanf:"commit_timeout"`
}

func DefaultConfig() Config {
	return Config{
		Kafka: KafkaConfig{
			Brokers:           []string{"localhost:9092"},
			GroupID:           "go-backend-consumer",
			Auth:              AuthNone,
			TLS:               TLSConfig{Enabled: true},
			AutoOffsetReset:   "earliest",
			FetchMinBytes:     1,
			FetchMaxBytes:     50 * 1024 * 1024,
			MaxPollRecords:    50,
			MaxPollIntervalMS: 300_000,
			SessionTimeoutMS:  45_000,
			CompressionType:   "lz4",
		},
		Topics: []TopicConfig{{
			Name:                  "greetings",
			Consumers:             1,
			ProcessingConcurrency: 10,
			BatchTimeout:          30 * time.Second,
			CommitTimeout:         10 * time.Second,
		}},
	}
}

func LoadConfig(path string) (Config, error) {
	return loadConfig(path, os.LookupEnv)
}

func loadConfig(path string, lookupEnv func(string) (string, bool)) (Config, error) {
	defaults := DefaultConfig()
	if path == "" {
		path = "configs/config.yaml"
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return defaults, nil
		}
	}
	values := map[string]any{
		"kafka": map[string]any{
			"enabled":              defaults.Kafka.Enabled,
			"brokers":              defaults.Kafka.Brokers,
			"group_id":             defaults.Kafka.GroupID,
			"auth":                 defaults.Kafka.Auth,
			"username":             defaults.Kafka.Username,
			"password":             defaults.Kafka.Password,
			"allow_insecure":       defaults.Kafka.AllowInsecure,
			"auto.offset.reset":    defaults.Kafka.AutoOffsetReset,
			"fetch.min.bytes":      defaults.Kafka.FetchMinBytes,
			"fetch.max.bytes":      defaults.Kafka.FetchMaxBytes,
			"max.poll.records":     defaults.Kafka.MaxPollRecords,
			"max.poll.interval.ms": defaults.Kafka.MaxPollIntervalMS,
			"session.timeout.ms":   defaults.Kafka.SessionTimeoutMS,
			"compression.type":     defaults.Kafka.CompressionType,
			"tls": map[string]any{
				"enabled":     defaults.Kafka.TLS.Enabled,
				"ca_file":     defaults.Kafka.TLS.CAFile,
				"server_name": defaults.Kafka.TLS.ServerName,
			},
		},
		"topics": []map[string]any{{
			"name":                   defaults.Topics[0].Name,
			"consumers":              defaults.Topics[0].Consumers,
			"processing_concurrency": defaults.Topics[0].ProcessingConcurrency,
			"batch_timeout":          defaults.Topics[0].BatchTimeout.String(),
			"commit_timeout":         defaults.Topics[0].CommitTimeout.String(),
		}},
	}
	k := koanf.NewWithConf(koanf.Conf{Delim: "::"})
	if err := k.Load(confmap.Provider(values, "::"), nil); err != nil {
		return Config{}, fmt.Errorf("load defaults: %w", err)
	}
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return Config{}, fmt.Errorf("load config %q: %w", path, err)
	}
	secrets := map[string]any{}
	if username, ok := lookupEnv("KAFKA_USERNAME"); ok {
		secrets["kafka::username"] = username
	}
	if password, ok := lookupEnv("KAFKA_PASSWORD"); ok {
		secrets["kafka::password"] = password
	}
	if len(secrets) > 0 {
		if err := k.Load(confmap.Provider(secrets, "::"), nil); err != nil {
			return Config{}, fmt.Errorf("load environment overrides: %w", err)
		}
	}

	var cfg Config
	decode := &mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			defaultTopicConfigDecodeHook,
		),
		ErrorUnused: true,
	}
	if err := k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{Tag: "koanf", DecoderConfig: decode}); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if cfg.Kafka.Enabled {
		if err := cfg.Validate(); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}

var topicNamePattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

func defaultTopicConfigDecodeHook(from, to reflect.Type, data any) (any, error) {
	if to != reflect.TypeFor[TopicConfig]() || from.Kind() != reflect.Map {
		return data, nil
	}
	values, ok := data.(map[string]any)
	if !ok {
		return data, nil
	}
	values = maps.Clone(values)
	if _, ok := values["processing_concurrency"]; !ok {
		values["processing_concurrency"] = 10
	}
	if _, ok := values["batch_timeout"]; !ok {
		values["batch_timeout"] = "30s"
	}
	if _, ok := values["commit_timeout"]; !ok {
		values["commit_timeout"] = "10s"
	}
	return values, nil
}

func (cfg Config) Validate() error {
	if len(cfg.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers must contain at least one broker")
	}
	for _, broker := range cfg.Kafka.Brokers {
		if err := validateBroker(broker); err != nil {
			return err
		}
	}
	if strings.TrimSpace(cfg.Kafka.GroupID) == "" {
		return fmt.Errorf("kafka.group_id must not be empty")
	}
	switch cfg.Kafka.AutoOffsetReset {
	case "earliest", "latest", "none":
	default:
		return fmt.Errorf("kafka.auto.offset.reset has unsupported value %q", cfg.Kafka.AutoOffsetReset)
	}
	if cfg.Kafka.FetchMinBytes < 0 || cfg.Kafka.FetchMinBytes > cfg.Kafka.FetchMaxBytes {
		return fmt.Errorf("kafka.fetch.min.bytes must be between 0 and kafka.fetch.max.bytes")
	}
	const maxFetchBytes = 1<<30 - 1<<20
	if cfg.Kafka.FetchMaxBytes <= 0 || cfg.Kafka.FetchMaxBytes > maxFetchBytes {
		return fmt.Errorf("kafka.fetch.max.bytes must be positive and at most %d", maxFetchBytes)
	}
	if cfg.Kafka.MaxPollRecords <= 0 {
		return fmt.Errorf("kafka.max.poll.records must be positive")
	}
	const maxKafkaMillis = int64(1<<31 - 1)
	if cfg.Kafka.MaxPollIntervalMS < 100 || int64(cfg.Kafka.MaxPollIntervalMS) > maxKafkaMillis {
		return fmt.Errorf("kafka.max.poll.interval.ms must be between 100 and %d", maxKafkaMillis)
	}
	if cfg.Kafka.SessionTimeoutMS < 100 || int64(cfg.Kafka.SessionTimeoutMS) > maxKafkaMillis {
		return fmt.Errorf("kafka.session.timeout.ms must be between 100 and %d", maxKafkaMillis)
	}
	switch cfg.Kafka.CompressionType {
	case "none", "gzip", "snappy", "lz4", "zstd":
	default:
		return fmt.Errorf("kafka.compression.type has unsupported value %q", cfg.Kafka.CompressionType)
	}
	if len(cfg.Topics) == 0 {
		return fmt.Errorf("topics must contain at least one topic")
	}
	seenTopics := make(map[string]struct{}, len(cfg.Topics))
	for _, topic := range cfg.Topics {
		if len(topic.Name) == 0 || len(topic.Name) > 249 || !topicNamePattern.MatchString(topic.Name) || topic.Name == "." || topic.Name == ".." {
			return fmt.Errorf("topics contains invalid Kafka topic name %q", topic.Name)
		}
		if _, exists := seenTopics[topic.Name]; exists {
			return fmt.Errorf("topics contains duplicate topic %q", topic.Name)
		}
		seenTopics[topic.Name] = struct{}{}
		if topic.Consumers <= 0 {
			return fmt.Errorf("topics.%s.consumers must be positive", topic.Name)
		}
		if topic.ProcessingConcurrency <= 0 {
			return fmt.Errorf("topics.%s.processing_concurrency must be positive", topic.Name)
		}
		if topic.BatchTimeout <= 0 {
			return fmt.Errorf("topics.%s.batch_timeout must be positive", topic.Name)
		}
		if topic.CommitTimeout <= 0 {
			return fmt.Errorf("topics.%s.commit_timeout must be positive", topic.Name)
		}
		if len(cfg.Kafka.GroupID)+1+len(topic.Name) > 249 {
			return fmt.Errorf("derived consumer group ID for topic %q exceeds 249 characters", topic.Name)
		}
	}
	if !cfg.Kafka.TLS.Enabled && !cfg.Kafka.AllowInsecure {
		return fmt.Errorf("kafka.tls.enabled may be false only when kafka.allow_insecure is true")
	}
	if cfg.Kafka.Auth == AuthAWSIAM {
		if !cfg.Kafka.TLS.Enabled {
			return fmt.Errorf("kafka.auth aws-iam requires TLS")
		}
		if cfg.Kafka.Username != "" || cfg.Kafka.Password != "" {
			return fmt.Errorf("kafka.username and kafka.password must be empty for aws-iam")
		}
	} else {
		switch cfg.Kafka.Auth {
		case AuthNone:
			if cfg.Kafka.Username != "" || cfg.Kafka.Password != "" {
				return fmt.Errorf("kafka.username and kafka.password require a username/password auth mode")
			}
		case AuthPlain, AuthSCRAMSHA256, AuthSCRAMSHA512:
			if cfg.Kafka.Username == "" || cfg.Kafka.Password == "" {
				return fmt.Errorf("kafka.username and kafka.password are required for %s", cfg.Kafka.Auth)
			}
		default:
			return fmt.Errorf("kafka.auth has unsupported value %q", cfg.Kafka.Auth)
		}
	}
	return nil
}

func validateBroker(broker string) error {
	host, port, err := net.SplitHostPort(broker)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf("kafka.brokers contains invalid host:port %q", broker)
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return fmt.Errorf("kafka.brokers contains invalid port in %q", broker)
		}
	}
	var portNumber int
	if _, err := fmt.Sscanf(port, "%d", &portNumber); err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("kafka.brokers contains invalid port in %q", broker)
	}
	return nil
}
