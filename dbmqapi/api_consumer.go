package dbmqapi

import (
	"context"
	"time"

	"github.com/donutnomad/dbmq/internal/query"
)

// ConsumerAPI 消费者 API
// @TAG(Consumer)
// @PREFIX(/dbmq/api/v1/consumers)
type ConsumerAPI interface {
	// List 获取所有消费者
	// @GET(/)
	List(ctx context.Context) ([]ConsumerResp, error)
}

type consumerAPI struct {
	deps *Deps
}

func NewConsumerAPI(deps *Deps) ConsumerAPI {
	return &consumerAPI{deps: deps}
}

func buildConsumerResponses(consumers []query.ConsumerMetrics) []ConsumerResp {
	result := make([]ConsumerResp, len(consumers))
	for i, consumer := range consumers {
		assignment := make(map[string][]int)
		for _, part := range consumer.Assignment {
			assignment[part.Topic] = append(assignment[part.Topic], int(part.Partition))
		}

		resp := ConsumerResp{
			GroupID: consumer.GroupID,
			ConsumerMemberDTO: ConsumerMemberDTO{
				MemberID:         consumer.ConsumerID,
				ClientID:         consumer.ClientID,
				Host:             consumer.Host,
				GenerationID:     consumer.GenerationID,
				Offline:          consumer.Offline,
				LastHeartbeat:    consumer.LastHeartbeat.Format(time.RFC3339),
				SubscribedTopics: consumer.SubscribedTopics,
				Assignment:       assignment,
				Status:           consumer.Status,
			},
		}

		if consumer.OfflineAt != nil {
			offlineAt := consumer.OfflineAt.Format(time.RFC3339)
			resp.OfflineAt = &offlineAt
		}

		result[i] = resp
	}

	return result
}

func (a *consumerAPI) List(ctx context.Context) ([]ConsumerResp, error) {
	consumers, err := a.deps.ConsumerQuery.GetAllConsumers(ctx)
	if err != nil {
		return nil, err
	}

	return buildConsumerResponses(consumers), nil
}
