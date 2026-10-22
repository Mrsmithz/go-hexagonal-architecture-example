package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"

	"example.com/go-backend-example/internal/greeting"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Store struct {
	dsn   string
	db    *gorm.DB
	sqlDB *sql.DB
}

type greetingRow struct {
	ID      int64  `gorm:"primaryKey;autoIncrement"`
	Message string `gorm:"not null"`
}

func (greetingRow) TableName() string { return "greetings" }

var _ greeting.Store = (*Store)(nil)

func New(dsn string) *Store { return &Store{dsn: dsn} }

func (s *Store) Start(ctx context.Context) (startErr error) {
	if s.db != nil {
		return errors.New("sqlite store already started")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(s.dsn) == "" {
		return errors.New("sqlite dsn is empty")
	}
	db, err := gorm.Open(sqlite.Open(s.dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("get sqlite pool: %w", err)
	}
	defer func() {
		if startErr != nil {
			startErr = errors.Join(startErr, sqlDB.Close())
		}
	}()
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	sqlDB.SetConnMaxLifetime(0)
	if _, err := sqlDB.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
		return fmt.Errorf("set sqlite busy timeout: %w", err)
	}
	if s.dsn != ":memory:" && s.dsn != "file::memory:?cache=shared" {
		if _, err := sqlDB.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
			return fmt.Errorf("enable sqlite WAL: %w", err)
		}
	}
	if err := db.WithContext(ctx).AutoMigrate(&greetingRow{}, &eventRow{}); err != nil {
		return fmt.Errorf("migrate sqlite schema: %w", err)
	}
	s.db, s.sqlDB = db, sqlDB
	return nil
}

func (s *Store) Close() error {
	if s.sqlDB == nil {
		return nil
	}
	return s.sqlDB.Close()
}

func (s *Store) Insert(ctx context.Context, message string) (greeting.Greeting, error) {
	row := greetingRow{Message: message}
	if err := s.db.WithContext(ctx).Create(&row).Error; err != nil {
		return greeting.Greeting{}, fmt.Errorf("insert greeting: %w", err)
	}
	return greeting.Greeting{ID: greeting.ID(row.ID), Message: row.Message}, nil
}

func (s *Store) Find(ctx context.Context, id greeting.ID) (greeting.Greeting, error) {
	if err := ctx.Err(); err != nil {
		return greeting.Greeting{}, err
	}
	if uint64(id) > math.MaxInt64 {
		return greeting.Greeting{}, greeting.ErrNotFound
	}
	var row greetingRow
	err := s.db.WithContext(ctx).First(&row, int64(id)).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return greeting.Greeting{}, greeting.ErrNotFound
	}
	if err != nil {
		return greeting.Greeting{}, fmt.Errorf("find greeting: %w", err)
	}
	return greeting.Greeting{ID: greeting.ID(row.ID), Message: row.Message}, nil
}

func (s *Store) All(ctx context.Context) ([]greeting.Greeting, error) {
	var rows []greetingRow
	if err := s.db.WithContext(ctx).Order("id ASC").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list greetings: %w", err)
	}
	items := make([]greeting.Greeting, 0, len(rows))
	for _, row := range rows {
		items = append(items, greeting.Greeting{ID: greeting.ID(row.ID), Message: row.Message})
	}
	return items, nil
}

func (s *Store) Replace(ctx context.Context, id greeting.ID, message string) (greeting.Greeting, error) {
	if err := ctx.Err(); err != nil {
		return greeting.Greeting{}, err
	}
	if uint64(id) > math.MaxInt64 {
		return greeting.Greeting{}, greeting.ErrNotFound
	}
	result := s.db.WithContext(ctx).Model(&greetingRow{}).Where("id = ?", int64(id)).Update("message", message)
	if result.Error != nil {
		return greeting.Greeting{}, fmt.Errorf("replace greeting: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return greeting.Greeting{}, greeting.ErrNotFound
	}
	return greeting.Greeting{ID: id, Message: message}, nil
}

func (s *Store) Remove(ctx context.Context, id greeting.ID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if uint64(id) > math.MaxInt64 {
		return greeting.ErrNotFound
	}
	result := s.db.WithContext(ctx).Delete(&greetingRow{}, int64(id))
	if result.Error != nil {
		return fmt.Errorf("remove greeting: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return greeting.ErrNotFound
	}
	return nil
}
