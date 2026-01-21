package repo

import (
	"context"
	"strings"

	"github.com/donutnomad/dbmq/internal/db"
)

// CreateManualAssignment 创建手动分区分配配置
func (d *MqRepo) CreateManualAssignment(ctx context.Context, assignment *db.ManualPartitionAssignment) error {
	return d.db.WithContext(ctx).Create(assignment).Error
}

// GetManualAssignmentsByGroup 获取消费组的所有手动分配配置
func (d *MqRepo) GetManualAssignmentsByGroup(ctx context.Context, groupID string) ([]db.ManualPartitionAssignment, error) {
	var assignments []db.ManualPartitionAssignment
	err := d.db.WithContext(ctx).
		Model(&db.ManualPartitionAssignment{}).
		Where("`group_id` = ?", groupID).
		Order("id ASC").
		Find(&assignments).Error
	if err != nil {
		return nil, err
	}
	return assignments, nil
}

// DeleteManualAssignment 删除手动分区分配配置
func (d *MqRepo) DeleteManualAssignment(ctx context.Context, id int64) error {
	result := d.db.WithContext(ctx).Delete(&db.ManualPartitionAssignment{}, id)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// GetManualAssignments 获取消费组的手动分配配置
// 根据 pattern 匹配 consumerIDs，返回 map[consumerID][]PartitionInfo
// 匹配规则：
//   - 如果 pattern 以 `*` 结尾 -> 前缀匹配（去掉 `*` 后用 strings.HasPrefix）
//   - 否则 -> 精确匹配
func (d *MqRepo) GetManualAssignments(ctx context.Context, groupID string, consumerIDs []string) (map[string][]db.PartitionInfo, error) {
	if len(consumerIDs) == 0 {
		return nil, nil
	}

	// 从数据库查询该 groupID 对应的所有手动分配配置
	var assignments []db.ManualPartitionAssignment
	err := d.db.WithContext(ctx).
		Model(&db.ManualPartitionAssignment{}).
		Where("`group_id` = ?", groupID).
		Find(&assignments).Error
	if err != nil {
		return nil, err
	}

	if len(assignments) == 0 {
		return nil, nil
	}

	// 构建结果 map
	result := make(map[string][]db.PartitionInfo)

	// 遍历每个配置，对每个 consumerID 进行匹配
	for _, assignment := range assignments {
		pattern := assignment.ConsumerIDPattern
		partition := db.PartitionInfo{
			Topic:     assignment.Topic,
			Partition: assignment.Partition,
		}

		// 判断是前缀匹配还是精确匹配
		if strings.HasSuffix(pattern, "*") {
			// 前缀匹配：去掉 `*` 后用 strings.HasPrefix
			prefix := strings.TrimSuffix(pattern, "*")
			for _, consumerID := range consumerIDs {
				if strings.HasPrefix(consumerID, prefix) {
					result[consumerID] = append(result[consumerID], partition)
				}
			}
		} else {
			// 精确匹配
			for _, consumerID := range consumerIDs {
				if consumerID == pattern {
					result[consumerID] = append(result[consumerID], partition)
				}
			}
		}
	}

	if len(result) == 0 {
		return nil, nil
	}

	return result, nil
}
