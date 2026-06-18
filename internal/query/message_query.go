package query

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
)

// MessageQuery 消息查询接口
type MessageQuery interface {
	// Search 搜索消息
	Search(ctx context.Context, req MessageSearchRequest) (*MessageSearchResult, error)
	// GetByID 按消息主键 ID 查询单条消息，未找到返回 (nil, nil)
	GetByID(ctx context.Context, id int64) (*MessageRecord, error)
}

// messageQueryMySQL 消息查询 MySQL 实现
type messageQueryMySQL struct {
	db interfaces.DB
}

// NewMessageQuery 创建消息查询实例
func NewMessageQuery(db interfaces.DB) MessageQuery {
	return &messageQueryMySQL{db: db}
}

// Search 搜索消息
func (q *messageQueryMySQL) Search(ctx context.Context, req MessageSearchRequest) (*MessageSearchResult, error) {
	limit := req.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	query := q.db.WithContext(ctx).Where("topic = ?", req.Topic)

	if req.Partition != nil {
		query = query.Where("`partition` = ?", *req.Partition)
	}

	if req.FromTime != nil {
		query = query.Where("created_at >= ?", *req.FromTime)
	}
	if req.ToTime != nil {
		query = query.Where("created_at <= ?", *req.ToTime)
	}

	if req.Search != "" {
		query = query.Where("(message_key LIKE ? OR body LIKE ?)", "%"+req.Search+"%", "%"+req.Search+"%")
	}

	// 统计总数
	var total int64
	if err := query.Model(&messagerepo.MessagePO{}).Count(&total).Error; err != nil {
		return nil, err
	}

	// 查询消息
	var messages []messagerepo.MessagePO
	if err := query.Order("created_at DESC").Offset(int(req.Offset)).Limit(limit).Find(&messages).Error; err != nil {
		return nil, err
	}

	// 转换为查询层 DTO
	records := make([]MessageRecord, len(messages))
	for i, msg := range messages {
		records[i] = MessageRecord{
			ID:         msg.ID,
			Topic:      msg.Topic,
			Partition:  msg.Partition,
			MessageKey: msg.MessageKey,
			Body:       msg.Body,
			Headers:    msg.Headers.Data(),
			CreatedAt:  msg.CreatedAt,
		}
	}

	return &MessageSearchResult{
		Messages: records,
		Total:    total,
	}, nil
}

// GetByID 按消息主键 ID 查询单条消息，未找到返回 (nil, nil)
func (q *messageQueryMySQL) GetByID(ctx context.Context, id int64) (*MessageRecord, error) {
	var msg messagerepo.MessagePO
	err := q.db.WithContext(ctx).Where("id = ?", id).First(&msg).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return &MessageRecord{
		ID:         msg.ID,
		Topic:      msg.Topic,
		Partition:  msg.Partition,
		MessageKey: msg.MessageKey,
		Body:       msg.Body,
		Headers:    msg.Headers.Data(),
		CreatedAt:  msg.CreatedAt,
	}, nil
}

// ParseTime 解析 RFC3339 时间字符串
func ParseTime(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return &t
}
