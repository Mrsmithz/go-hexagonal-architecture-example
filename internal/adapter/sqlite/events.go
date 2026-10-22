package sqlite

import (
	"context"
	"fmt"

	"example.com/go-backend-example/internal/message"
	"gorm.io/gorm/clause"
)

type eventRow struct {
	SourceTopic     string `gorm:"primaryKey;autoIncrement:false"`
	SourcePartition int32  `gorm:"primaryKey;autoIncrement:false"`
	SourceOffset    int64  `gorm:"primaryKey;autoIncrement:false"`
	UserID          int64  `gorm:"not null"`
	Event           string `gorm:"not null"`
	Timestamp       int64  `gorm:"not null"`
}

func (eventRow) TableName() string { return "user_events" }

var _ message.Store = (*Store)(nil)

func (s *Store) SaveEvent(ctx context.Context, source message.Source, event message.Event) error {
	row := eventRow{
		SourceTopic:     source.Topic,
		SourcePartition: source.Partition,
		SourceOffset:    source.Offset,
		UserID:          event.UserID,
		Event:           event.Event,
		Timestamp:       event.Timestamp,
	}
	conflict := clause.OnConflict{
		Columns: []clause.Column{
			{Name: "source_topic"},
			{Name: "source_partition"},
			{Name: "source_offset"},
		},
		DoNothing: true,
	}
	if err := s.db.WithContext(ctx).Clauses(conflict).Create(&row).Error; err != nil {
		return fmt.Errorf("save user event: %w", err)
	}
	return nil
}
