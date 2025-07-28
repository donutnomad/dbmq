package interfaces

import (
	"context"
	"database/sql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type DB interface {
	Model(value any) (tx *gorm.DB)
	Create(value any) (tx *gorm.DB)
	Save(value any) (tx *gorm.DB)
	Where(query any, args ...any) (tx *gorm.DB)
	Table(name string, args ...any) (tx *gorm.DB)
	Delete(value any, conds ...any) (tx *gorm.DB)
	Transaction(fn func(tx *gorm.DB) error, opts ...*sql.TxOptions) error
	AutoMigrate(table ...any) error
	Unscoped() *gorm.DB
	FirstOrCreate(dest any, conds ...any) (tx *gorm.DB)
	First(dest any, conds ...any) (tx *gorm.DB)
	Scopes(funcs ...func(*gorm.DB) *gorm.DB) (tx *gorm.DB)
	Clauses(conds ...clause.Expression) (tx *gorm.DB)
	Raw(sql string, values ...any) (tx *gorm.DB)
	Exec(sql string, values ...any) (tx *gorm.DB)
	WithContext(ctx context.Context) *gorm.DB
	DB() (*sql.DB, error)
}
