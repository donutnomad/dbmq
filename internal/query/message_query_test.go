package query

import (
	"testing"
	"time"

	"gorm.io/datatypes"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// testMessagePO 与 mq_messages 同表，但使用 sqlite 兼容的列类型，
// 仅用于测试建表/插入（messagerepo.MessagePO 的 gorm tag 是 MySQL 专有语法）。
type testMessagePO struct {
	ID         int64 `gorm:"primaryKey;column:id;autoIncrement"`
	CreatedAt  time.Time
	Topic      string
	Partition  uint
	MessageKey string                                `gorm:"column:message_key"`
	Headers    datatypes.JSONType[map[string]string] `gorm:"column:headers"`
	Body       datatypes.JSON                        `gorm:"column:body"`
}

func (testMessagePO) TableName() string { return "mq_messages" }

// setupMessageQueryTestDB 创建 sqlite 内存库并迁移 mq_messages 表
func setupMessageQueryTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("打开 sqlite 失败: %v", err)
	}
	if err := db.AutoMigrate(&testMessagePO{}); err != nil {
		t.Fatalf("迁移 mq_messages 失败: %v", err)
	}
	return db
}

// insertTestMessage 插入一条测试消息并返回其 ID
func insertTestMessage(t *testing.T, db *gorm.DB, topic, key string, headers map[string]string, body string) int64 {
	t.Helper()
	po := testMessagePO{
		CreatedAt:  time.Now(),
		Topic:      topic,
		MessageKey: key,
		Headers:    datatypes.NewJSONType(headers),
		Body:       datatypes.JSON([]byte(body)),
	}
	if err := db.Create(&po).Error; err != nil {
		t.Fatalf("插入消息失败: %v", err)
	}
	return po.ID
}

func TestGetByID_Found(t *testing.T) {
	db := setupMessageQueryTestDB(t)
	id := insertTestMessage(t, db, "business_user:log", "order-123",
		map[string]string{"trace": "abc"}, `{"hello":"world"}`)

	q := NewMessageQuery(db)
	got, err := q.GetByID(t.Context(), id)
	if err != nil {
		t.Fatalf("GetByID 返回错误: %v", err)
	}
	if got == nil {
		t.Fatal("期望找到消息，得到 nil")
	}
	if got.ID != id {
		t.Errorf("ID 不匹配: 期望 %d, 得到 %d", id, got.ID)
	}
	if got.Topic != "business_user:log" {
		t.Errorf("Topic 不匹配: 得到 %q", got.Topic)
	}
	if got.MessageKey != "order-123" {
		t.Errorf("MessageKey 不匹配: 得到 %q", got.MessageKey)
	}
	if got.Headers["trace"] != "abc" {
		t.Errorf("Headers 不匹配: 得到 %v", got.Headers)
	}
	if string(got.Body) != `{"hello":"world"}` {
		t.Errorf("Body 不匹配: 得到 %s", string(got.Body))
	}
}

func TestGetByID_NotFound(t *testing.T) {
	db := setupMessageQueryTestDB(t)
	q := NewMessageQuery(db)

	got, err := q.GetByID(t.Context(), 999999)
	if err != nil {
		t.Fatalf("未找到时应返回 nil 错误, 得到: %v", err)
	}
	if got != nil {
		t.Errorf("未找到时应返回 nil 记录, 得到: %+v", got)
	}
}
