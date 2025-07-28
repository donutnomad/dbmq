package interfaces

import (
	"context"
	"gorm.io/gorm"
)

type DB interface {
	WithContext(ctx context.Context) *gorm.DB
	Exec(sql string, values ...any) (tx *gorm.DB)
	Model(value any) *gorm.DB
	Raw(sql string, values ...interface{}) (tx *gorm.DB)
	AutoMigrate(dst ...interface{}) error
}
