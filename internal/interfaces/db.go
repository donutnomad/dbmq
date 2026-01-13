package interfaces

import (
	"context"
	"database/sql"

	"gorm.io/gorm"
)

type DB interface {
	Model(value any) (tx *gorm.DB)
	Where(query any, args ...any) (tx *gorm.DB)
	Table(name string, args ...any) (tx *gorm.DB)
	AutoMigrate(table ...any) error
	Raw(sql string, values ...any) (tx *gorm.DB)
	Exec(sql string, values ...any) (tx *gorm.DB)
	WithContext(ctx context.Context) *gorm.DB
	DB() (*sql.DB, error)
}
