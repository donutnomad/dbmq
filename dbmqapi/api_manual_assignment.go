package dbmqapi

import (
	"context"

	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
)

// ManualAssignmentAPI 手动分区分配 API
// @TAG(Manual-Assignment)
type ManualAssignmentAPI interface {
	// Create 创建手动分区分配
	// @POST(/dbmq/api/v1/manual-assignments)
	Create(ctx context.Context, req CreateManualAssignmentReq) (ManualAssignmentResp, error)
	// List 查询手动分区分配列表
	// @GET(/dbmq/api/v1/manual-assignments)
	List(ctx context.Context, req ListManualAssignmentsReq) ([]ManualAssignmentResp, error)
	// Delete 删除手动分区分配
	// @DELETE(/dbmq/api/v1/manual-assignments/{id})
	Delete(ctx context.Context, id int64) (MessageResp, error)
}

type manualAssignmentAPI struct {
	repo manualassignment.Repo
}

func NewManualAssignmentAPI(deps *Deps) ManualAssignmentAPI {
	return &manualAssignmentAPI{repo: deps.ManualAssignmentRepo}
}

func (a *manualAssignmentAPI) Create(ctx context.Context, req CreateManualAssignmentReq) (ManualAssignmentResp, error) {
	assignment := &manualassignment.Assignment{
		GroupID:           req.GroupID,
		ConsumerIDPattern: req.ConsumerIDPattern,
		Topic:             req.Topic,
		Partition:         req.Partition,
	}

	if err := a.repo.Create(ctx, assignment); err != nil {
		return ManualAssignmentResp{}, err
	}

	return ManualAssignmentResp{
		ID:                assignment.ID,
		GroupID:           assignment.GroupID,
		ConsumerIDPattern: assignment.ConsumerIDPattern,
		Topic:             assignment.Topic,
		Partition:         assignment.Partition,
		CreatedAt:         assignment.CreatedAt,
		UpdatedAt:         assignment.UpdatedAt,
	}, nil
}

func (a *manualAssignmentAPI) List(ctx context.Context, req ListManualAssignmentsReq) ([]ManualAssignmentResp, error) {
	assignments, err := a.repo.GetByGroup(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}

	result := make([]ManualAssignmentResp, len(assignments))
	for i, assign := range assignments {
		result[i] = ManualAssignmentResp{
			ID:                assign.ID,
			GroupID:           assign.GroupID,
			ConsumerIDPattern: assign.ConsumerIDPattern,
			Topic:             assign.Topic,
			Partition:         assign.Partition,
			CreatedAt:         assign.CreatedAt,
			UpdatedAt:         assign.UpdatedAt,
		}
	}

	return result, nil
}

func (a *manualAssignmentAPI) Delete(ctx context.Context, id int64) (MessageResp, error) {
	if err := a.repo.Delete(ctx, id); err != nil {
		return MessageResp{}, err
	}

	return MessageResp{
		Message: "Manual assignment deleted successfully",
	}, nil
}
