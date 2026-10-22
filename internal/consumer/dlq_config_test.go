package consumer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeadLetterTopic(t *testing.T) {
	if got := (TopicConfig{Name: "events"}).DeadLetterTopic(); got != "events.dlq" {
		t.Fatalf("default DLQ = %q", got)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	contents := "kafka:\n  enabled: true\ntopics:\n  - name: events\n    consumers: 1\n    dlq_topic: rejected-events\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path, func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Topics[0].DeadLetterTopic(); got != "rejected-events" {
		t.Fatalf("configured DLQ = %q", got)
	}
}

func TestValidateDeadLetterTopics(t *testing.T) {
	for _, name := range []string{"invalid name", ".", "..", strings.Repeat("a", 250), "greetings"} {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Topics[0].DLQTopic = name
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "dlq_topic") {
				t.Fatalf("invalid DLQ %q accepted: %v", name, err)
			}
		})
	}
	t.Run("default suffix must fit", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Kafka.GroupID = "g"
		cfg.Topics[0].Name = strings.Repeat("a", 246)
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "dlq_topic") {
			t.Fatalf("oversized derived DLQ accepted: %v", err)
		}
		cfg.Topics[0].DLQTopic = "short-dlq"
		if err := cfg.Validate(); err != nil {
			t.Fatalf("valid override rejected: %v", err)
		}
	})
	t.Run("cannot route into another source", func(t *testing.T) {
		cfg := DefaultConfig()
		other := cfg.Topics[0]
		other.Name = cfg.Topics[0].DeadLetterTopic()
		cfg.Topics = append(cfg.Topics, other)
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "must not be a consumed topic") {
			t.Fatalf("recursive DLQ route accepted: %v", err)
		}
	})
}
