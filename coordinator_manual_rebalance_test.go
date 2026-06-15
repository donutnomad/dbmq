package dbmq

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/domain/consumergroup"
	"github.com/donutnomad/dbmq/internal/domain/consumerprogress"
	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
	"github.com/donutnomad/dbmq/internal/domain/message"
	"github.com/donutnomad/dbmq/internal/domain/topic"
	"github.com/donutnomad/dbmq/internal/types"
)

// mockManualAssignmentRepo 用于测试的手动分配仓储 mock
type mockManualAssignmentRepo struct {
	// matchResult 预设的 GetMatching 返回值
	matchResult map[string][]types.PartitionInfo
}

func (m *mockManualAssignmentRepo) Create(_ context.Context, _ *manualassignment.Assignment) error {
	return nil
}
func (m *mockManualAssignmentRepo) GetByGroup(_ context.Context, _ string) ([]*manualassignment.Assignment, error) {
	return nil, nil
}
func (m *mockManualAssignmentRepo) GetAll(_ context.Context) ([]*manualassignment.Assignment, error) {
	return nil, nil
}
func (m *mockManualAssignmentRepo) Delete(_ context.Context, _ int64) error {
	return nil
}
func (m *mockManualAssignmentRepo) GetMatching(_ context.Context, _ string, _ []string) (map[string][]types.PartitionInfo, error) {
	return m.matchResult, nil
}
func (m *mockManualAssignmentRepo) DeleteByGroup(_ context.Context, _ string) error {
	return nil
}

// makeTestConsumersWithTopics 创建带有订阅 Topic 的消费者列表
func makeTestConsumersWithTopics(topics []string, ids ...string) []*heartbeat.Heartbeat {
	consumers := make([]*heartbeat.Heartbeat, len(ids))
	for i, id := range ids {
		consumers[i] = &heartbeat.Heartbeat{
			ConsumerID:       id,
			SubscribedTopics: topics,
		}
	}
	return consumers
}

// setConsumerGeneration 将消费者心跳行的 generation 设为指定值
// （模拟真实 IncrementAndUpdateAssignments 在 rebalance 后写库的结果）
func setConsumerGeneration(consumers []*heartbeat.Heartbeat, gen uint) {
	for _, c := range consumers {
		c.GenerationID = gen
	}
}

// newTestSnapshotInfo 创建带 mock repo 的测试快照状态
func newTestSnapshotInfo(matchResult map[string][]types.PartitionInfo) (*snapshotInfo, *mockManualAssignmentRepo) {
	repo := &mockManualAssignmentRepo{matchResult: matchResult}
	return newSnapshotInfo(), repo
}

func testCanRebalance(ctx context.Context, snapshots *snapshotInfo, repo *mockManualAssignmentRepo, groupID string, consumers []*heartbeat.Heartbeat, partitionHash string, currentGenerationID uint) bool {
	snapshots.mu.Lock()
	snapshot := snapshots.groupSnapshots[groupID]
	snapshots.mu.Unlock()

	return canRebalance(ctx, snapshot, repo, groupID, consumers, partitionHash, currentGenerationID)
}

type rebalanceHeartbeatRepo struct {
	active []*heartbeat.Heartbeat
}

func (m *rebalanceHeartbeatRepo) Get(_ context.Context, _, _ string) (*heartbeat.Heartbeat, error) {
	return nil, nil
}

func (m *rebalanceHeartbeatRepo) Upsert(_ context.Context, _, _ string, _ []string) error {
	return nil
}

func (m *rebalanceHeartbeatRepo) MarkOffline(_ context.Context, _, _ string) error {
	return nil
}

func (m *rebalanceHeartbeatRepo) Delete(_ context.Context, _, _ string) error {
	return nil
}

func (m *rebalanceHeartbeatRepo) FindActive(_ context.Context, _ string, _ time.Duration) ([]*heartbeat.Heartbeat, error) {
	return m.active, nil
}

func (m *rebalanceHeartbeatRepo) FindAll(_ context.Context, _ string, _ time.Duration) ([]*heartbeat.Heartbeat, error) {
	return m.active, nil
}

func (m *rebalanceHeartbeatRepo) DeleteByGroup(_ context.Context, _ string) error {
	return nil
}

func (m *rebalanceHeartbeatRepo) DeleteExpired(_ context.Context, _ time.Time, _ int) (int64, error) {
	return 0, nil
}

type rebalanceTopicRepo struct {
	topics []*topic.Topic
}

func (m *rebalanceTopicRepo) Get(_ context.Context, topicName string) (*topic.Topic, error) {
	for _, topicInfo := range m.topics {
		if topicInfo.Name == topicName {
			return topicInfo, nil
		}
	}
	return nil, nil
}

func (m *rebalanceTopicRepo) GetAll(_ context.Context) ([]*topic.Topic, error) {
	return m.topics, nil
}

func (m *rebalanceTopicRepo) FindByNames(_ context.Context, topicNames []string) ([]*topic.Topic, error) {
	nameSet := make(map[string]struct{}, len(topicNames))
	for _, name := range topicNames {
		nameSet[name] = struct{}{}
	}
	var result []*topic.Topic
	for _, topicInfo := range m.topics {
		if _, ok := nameSet[topicInfo.Name]; ok {
			result = append(result, topicInfo)
		}
	}
	return result, nil
}

type rebalanceGroupRepo struct {
	generation uint
	increments int
	// heartbeats 可选：模拟真实 IncrementAndUpdateAssignments 会把所有心跳行写到新 generation
	heartbeats []*heartbeat.Heartbeat
}

func (m *rebalanceGroupRepo) GetGeneration(_ context.Context, groupID string) (*consumergroup.Generation, error) {
	return &consumergroup.Generation{GroupID: groupID, GenerationID: m.generation}, nil
}

func (m *rebalanceGroupRepo) IncrementGenerationID(_ context.Context, _ string) (uint, error) {
	m.generation++
	m.increments++
	return m.generation, nil
}

func (m *rebalanceGroupRepo) IncrementAndUpdateAssignments(_ context.Context, _ string, _ map[string][]types.PartitionInfo) (uint, error) {
	m.generation++
	m.increments++
	for _, hb := range m.heartbeats {
		hb.GenerationID = m.generation
	}
	return m.generation, nil
}

func (m *rebalanceGroupRepo) FindAllActiveGroups(_ context.Context, _ time.Duration) ([]string, error) {
	return []string{"test-group"}, nil
}

func (m *rebalanceGroupRepo) FindAllGroups(_ context.Context) ([]string, error) {
	return []string{"test-group"}, nil
}

func (m *rebalanceGroupRepo) Delete(_ context.Context, _ string) error {
	return nil
}

type rebalanceProgressRepo struct{}

func (rebalanceProgressRepo) GetCommittedOffsets(context.Context, string, []types.PartitionInfo) ([]*consumerprogress.Progress, error) {
	return nil, nil
}
func (rebalanceProgressRepo) CommitOffset(context.Context, string, uint, types.PartitionInfo, int64) error {
	return nil
}
func (rebalanceProgressRepo) BatchCommitOffsets(context.Context, string, uint, map[types.PartitionInfo]int64) error {
	return nil
}
func (rebalanceProgressRepo) BatchCommitOffsetsWithWatermark(context.Context, string, uint, map[types.PartitionInfo]consumerprogress.ProgressWithWatermark) error {
	return nil
}
func (rebalanceProgressRepo) CommitWithSubscriptionRegistration(context.Context, string, uint, types.PartitionInfo, int64, int64) error {
	return nil
}
func (rebalanceProgressRepo) GetLowWatermarks(context.Context) (map[types.PartitionInfo]int64, error) {
	return nil, nil
}
func (rebalanceProgressRepo) DeleteByGroup(context.Context, string) error {
	return nil
}
func (rebalanceProgressRepo) DeleteByGroupTopicPartition(context.Context, string, string, uint) error {
	return nil
}

type rebalanceMessageRepo struct{}

func (rebalanceMessageRepo) CreateBatch(context.Context, []*message.Message) error { return nil }
func (rebalanceMessageRepo) Fetch(context.Context, string, uint, int64, int) ([]*message.Message, error) {
	return nil, nil
}
func (rebalanceMessageRepo) FetchBatch(context.Context, []message.FetchRequest) ([]*message.Message, error) {
	return nil, nil
}
func (rebalanceMessageRepo) GetLatestID(context.Context, string, uint) (int64, error) { return 0, nil }
func (rebalanceMessageRepo) GetLatestIDs(context.Context, []types.PartitionInfo) (map[types.PartitionInfo]int64, error) {
	return nil, nil
}
func (rebalanceMessageRepo) DeleteConsumed(context.Context, string, uint, int64, time.Time, int) (int64, error) {
	return 0, nil
}
func (rebalanceMessageRepo) DeleteExpired(context.Context, string, uint, time.Time, int) (int64, error) {
	return 0, nil
}

// TestManualAssignment_ConsumerRestart_PartitionShouldReturn 模拟场景：
// 1. A 和 B 在线，手动分配 p0 给 A
// 2. A 下线，B 拿到 p0
// 3. A'（新 UUID）上线，p0 应该回到 A'
func TestManualAssignment_ConsumerRestart_PartitionShouldReturn(t *testing.T) {
	p := func(topic string, partition uint) types.PartitionInfo {
		return types.PartitionInfo{Topic: topic, Partition: partition}
	}

	topic := "approvalflow:transferpolicy"
	partitions := []types.PartitionInfo{p(topic, 0)}

	consumerA := "ubuntudeMacBook-Pro.local:da:cc:09:69:e9:bd:uuid-1111"
	consumerB := "ubuntu:00:0c:29:3d:f7:5d:uuid-2222"
	consumerAPrime := "ubuntudeMacBook-Pro.local:da:cc:09:69:e9:bd:uuid-3333"

	// ===== 阶段 1: A 和 B 都在线，手动分配 p0 给 A =====
	manualForAB := map[string][]types.PartitionInfo{
		consumerA: {p(topic, 0)},
	}
	result1 := calculateAssignments(
		makeTestConsumers(consumerA, consumerB),
		partitions,
		manualForAB,
	)
	if !reflect.DeepEqual(result1[consumerA], []types.PartitionInfo{p(topic, 0)}) {
		t.Fatalf("阶段1: A 应该得到 p0，实际: %v", result1[consumerA])
	}
	if len(result1[consumerB]) != 0 {
		t.Fatalf("阶段1: B 不应该有分区，实际: %v", result1[consumerB])
	}
	t.Logf("阶段1 通过: A=%v, B=%v", result1[consumerA], result1[consumerB])

	// ===== 阶段 2: A 下线，只有 B 在线 =====
	result2 := calculateAssignments(
		makeTestConsumers(consumerB),
		partitions,
		nil,
	)
	if !reflect.DeepEqual(result2[consumerB], []types.PartitionInfo{p(topic, 0)}) {
		t.Fatalf("阶段2: B 应该通过自动分配得到 p0，实际: %v", result2[consumerB])
	}
	t.Logf("阶段2 通过: B=%v", result2[consumerB])

	// ===== 阶段 3: A'（新 UUID）上线 =====
	manualForAPrimeB := map[string][]types.PartitionInfo{
		consumerAPrime: {p(topic, 0)},
	}
	result3 := calculateAssignments(
		makeTestConsumers(consumerAPrime, consumerB),
		partitions,
		manualForAPrimeB,
	)
	if !reflect.DeepEqual(result3[consumerAPrime], []types.PartitionInfo{p(topic, 0)}) {
		t.Fatalf("阶段3: A' 应该得到 p0（手动分配），实际: %v", result3[consumerAPrime])
	}
	if len(result3[consumerB]) != 0 {
		t.Fatalf("阶段3: B 不应该有分区，实际: %v", result3[consumerB])
	}
	t.Logf("阶段3 通过: A'=%v, B=%v", result3[consumerAPrime], result3[consumerB])
}

// TestIsRebalanceNeeded_NewMemberTriggers 新成员上线时应该触发 rebalance
func TestIsRebalanceNeeded_NewMemberTriggers(t *testing.T) {
	snapshots, repo := newTestSnapshotInfo(nil)
	groupID := "test-group"
	topics := []string{"test-topic"}
	var gen uint = 10

	snapshots.groupSnapshots[groupID] = &groupSnapshot{
		generationID: gen,
		memberTopics: map[string]string{
			"consumer-b": hashSubscribedTopics(topics),
		},
		partitionHash:        "test-topic:1",
		manualAssignmentHash: "",
	}

	consumersWithNew := makeTestConsumersWithTopics(topics, "consumer-a-new", "consumer-b")

	if !testCanRebalance(context.Background(), snapshots, repo, groupID, consumersWithNew, "test-topic:1", gen) {
		t.Fatal("新成员上线后 isRebalanceNeeded 应该返回 true（成员数量变化 1→2）")
	}
}

// TestIsRebalanceNeeded_SameMemberCount_DifferentID 成员数相同但 ID 不同时应触发
func TestIsRebalanceNeeded_SameMemberCount_DifferentID(t *testing.T) {
	snapshots, repo := newTestSnapshotInfo(nil)
	groupID := "test-group"
	topics := []string{"test-topic"}
	var gen uint = 10

	snapshots.groupSnapshots[groupID] = &groupSnapshot{
		generationID: gen,
		memberTopics: map[string]string{
			"consumer-a-old": hashSubscribedTopics(topics),
			"consumer-b":     hashSubscribedTopics(topics),
		},
		partitionHash:        "test-topic:1",
		manualAssignmentHash: "",
	}

	consumersReplaced := makeTestConsumersWithTopics(topics, "consumer-a-new", "consumer-b")

	if !testCanRebalance(context.Background(), snapshots, repo, groupID, consumersReplaced, "test-topic:1", gen) {
		t.Fatal("成员 ID 变化后 isRebalanceNeeded 应该返回 true")
	}
}

// TestIsRebalanceNeeded_NoChange_ReturnsFalse 无变化时不应触发
func TestIsRebalanceNeeded_NoChange_ReturnsFalse(t *testing.T) {
	snapshots, repo := newTestSnapshotInfo(nil)
	groupID := "test-group"
	topics := []string{"test-topic"}
	var gen uint = 10

	consumerA := "consumer-a"
	consumerB := "consumer-b"

	snapshots.groupSnapshots[groupID] = &groupSnapshot{
		generationID: gen,
		memberTopics: map[string]string{
			consumerA: hashSubscribedTopics(topics),
			consumerB: hashSubscribedTopics(topics),
		},
		partitionHash:        "test-topic:1",
		manualAssignmentHash: "",
	}

	consumers := makeTestConsumersWithTopics(topics, consumerA, consumerB)
	setConsumerGeneration(consumers, gen)

	if testCanRebalance(context.Background(), snapshots, repo, groupID, consumers, "test-topic:1", gen) {
		t.Fatal("没有任何变化时 isRebalanceNeeded 应该返回 false")
	}
}

func TestCoordinatorTask_TryRebalance_StableSnapshotSkipsGenerationIncrement(t *testing.T) {
	groupID := "test-group"
	topicName := "test-topic"
	consumers := makeTestConsumersWithTopics([]string{topicName}, "consumer-a")
	groupRepo := &rebalanceGroupRepo{heartbeats: consumers}

	task := &coordinatorTask{
		cfg: &CoordinatorConfig{
			HeartbeatTimeout: 30 * time.Second,
			RebalanceTimeout: time.Second,
		},
		rebalancingLocks: newGroupLocks(),
		snapshotInfo:     newSnapshotInfo(),
		repos: repos{
			topicRepo:            &rebalanceTopicRepo{topics: []*topic.Topic{{Name: topicName, PartitionCount: 1}}},
			messageRepo:          rebalanceMessageRepo{},
			heartbeatRepo:        &rebalanceHeartbeatRepo{active: consumers},
			groupRepo:            groupRepo,
			progressRepo:         rebalanceProgressRepo{},
			manualAssignmentRepo: &mockManualAssignmentRepo{},
		},
	}

	if err := task.tryRebalance(context.Background(), groupID); err != nil {
		t.Fatalf("第一次 rebalance 失败: %v", err)
	}
	if groupRepo.increments != 1 {
		t.Fatalf("第一次 rebalance 应该递增一次 generation，实际: %d", groupRepo.increments)
	}

	if err := task.tryRebalance(context.Background(), groupID); err != nil {
		t.Fatalf("稳定状态 rebalance 检查失败: %v", err)
	}
	if groupRepo.increments != 1 {
		t.Fatalf("稳定状态不应该再次递增 generation，实际: %d", groupRepo.increments)
	}
}

// TestIsRebalanceNeeded_ManualHashChange_Triggers 手动分配规则展开结果变化时应触发
func TestIsRebalanceNeeded_ManualHashChange_Triggers(t *testing.T) {
	topic := "test-topic"
	p0 := types.PartitionInfo{Topic: topic, Partition: 0}
	var gen uint = 10

	oldManual := map[string][]types.PartitionInfo{
		"host:mac:uuid-old": {p0},
	}
	oldHash := hashManualAssignments(oldManual)

	newManual := map[string][]types.PartitionInfo{
		"host:mac:uuid-new": {p0},
	}

	snapshots, repo := newTestSnapshotInfo(newManual)
	groupID := "test-group"
	topics := []string{topic}

	snapshots.groupSnapshots[groupID] = &groupSnapshot{
		generationID: gen,
		memberTopics: map[string]string{
			"host:mac:uuid-new": hashSubscribedTopics(topics),
			"consumer-b":        hashSubscribedTopics(topics),
		},
		partitionHash:        topic + ":1",
		manualAssignmentHash: oldHash,
	}

	consumers := makeTestConsumersWithTopics(topics, "host:mac:uuid-new", "consumer-b")

	if !testCanRebalance(context.Background(), snapshots, repo, groupID, consumers, topic+":1", gen) {
		t.Fatal("手动分配展开结果变化时 isRebalanceNeeded 应该返回 true")
	}
}

// TestHashManualAssignments_DifferentConsumerID_DifferentHash 不同 ConsumerID 产生不同 hash
func TestHashManualAssignments_DifferentConsumerID_DifferentHash(t *testing.T) {
	topic := "approvalflow:transferpolicy"
	p0 := types.PartitionInfo{Topic: topic, Partition: 0}

	hash1 := hashManualAssignments(map[string][]types.PartitionInfo{
		"host:aa:bb:cc:dd:ee:ff:uuid-1111": {p0},
	})
	hash2 := hashManualAssignments(map[string][]types.PartitionInfo{
		"host:aa:bb:cc:dd:ee:ff:uuid-2222": {p0},
	})
	hash3 := hashManualAssignments(nil)

	if hash1 == hash2 {
		t.Fatal("不同 ConsumerID 的手动分配 hash 应该不同")
	}
	if hash1 == hash3 {
		t.Fatal("有匹配 vs 无匹配的 hash 应该不同")
	}
	if hash3 != "" {
		t.Fatalf("空匹配的 hash 应该为空字符串，实际: %q", hash3)
	}
	t.Logf("hash1=%q, hash2=%q, hash3=%q", hash1, hash2, hash3)
}

// TestSnapshotTransition_FullRebalanceCycle 模拟完整 rebalance 周期
func TestSnapshotTransition_FullRebalanceCycle(t *testing.T) {
	groupID := "Custodian_ApprovalFlow_TransferPolicy"
	topic := "approvalflow:transferpolicy"
	topics := []string{topic}
	partitions := []types.PartitionInfo{{Topic: topic, Partition: 0}}
	partitionHash := topic + ":1"

	consumerA := "ubuntudeMacBook-Pro.local:da:cc:09:69:e9:bd:uuid-old"
	consumerB := "ubuntu:00:0c:29:3d:f7:5d:uuid-other"
	consumerAPrime := "ubuntudeMacBook-Pro.local:da:cc:09:69:e9:bd:uuid-new"

	p := func(topic string, partition uint) types.PartitionInfo {
		return types.PartitionInfo{Topic: topic, Partition: partition}
	}

	// ===== 轮次 1: A 和 B 都在线，手动分配 p0 给 A =====
	var gen uint = 1
	manual1 := map[string][]types.PartitionInfo{consumerA: {p(topic, 0)}}
	snapshots, repo := newTestSnapshotInfo(manual1)
	consumersRound1 := makeTestConsumersWithTopics(topics, consumerA, consumerB)

	if !testCanRebalance(context.Background(), snapshots, repo, groupID, consumersRound1, partitionHash, gen) {
		t.Fatal("轮次1: snapshot 为空时应该需要 rebalance")
	}

	assign1 := calculateAssignments(consumersRound1, partitions, manual1)
	snapshots.updateGroupSnapshot(groupID, gen, consumersRound1, partitionHash, manual1)

	if !reflect.DeepEqual(assign1[consumerA], []types.PartitionInfo{p(topic, 0)}) {
		t.Fatalf("轮次1: A 应该得到 p0，实际: %v", assign1)
	}
	t.Logf("轮次1: A=%v, B=%v", assign1[consumerA], assign1[consumerB])

	// ===== 轮次 2: A 下线，只有 B =====
	gen = 2
	repo.matchResult = nil
	consumersRound2 := makeTestConsumersWithTopics(topics, consumerB)

	if !testCanRebalance(context.Background(), snapshots, repo, groupID, consumersRound2, partitionHash, gen) {
		t.Fatal("轮次2: A 下线后应该需要 rebalance")
	}

	assign2 := calculateAssignments(consumersRound2, partitions, nil)
	snapshots.updateGroupSnapshot(groupID, gen, consumersRound2, partitionHash, nil)

	if !reflect.DeepEqual(assign2[consumerB], []types.PartitionInfo{p(topic, 0)}) {
		t.Fatalf("轮次2: B 应该自动分配得到 p0，实际: %v", assign2)
	}
	t.Logf("轮次2: B=%v", assign2[consumerB])

	// ===== 轮次 3: A'（新 UUID）上线 =====
	gen = 3
	manual3 := map[string][]types.PartitionInfo{consumerAPrime: {p(topic, 0)}}
	repo.matchResult = manual3
	consumersRound3 := makeTestConsumersWithTopics(topics, consumerAPrime, consumerB)

	if !testCanRebalance(context.Background(), snapshots, repo, groupID, consumersRound3, partitionHash, gen) {
		t.Fatal("轮次3: A' 上线后应该需要 rebalance")
	}

	assign3 := calculateAssignments(consumersRound3, partitions, manual3)
	snapshots.updateGroupSnapshot(groupID, gen, consumersRound3, partitionHash, manual3)

	if !reflect.DeepEqual(assign3[consumerAPrime], []types.PartitionInfo{p(topic, 0)}) {
		t.Fatalf("轮次3: A' 应该得到 p0，实际: %v", assign3[consumerAPrime])
	}
	if len(assign3[consumerB]) != 0 {
		t.Fatalf("轮次3: B 不应该有分区，实际: %v", assign3[consumerB])
	}
	t.Logf("轮次3: A'=%v, B=%v", assign3[consumerAPrime], assign3[consumerB])

	// ===== 轮次 4: 稳定状态（rebalance 后心跳行已被写到当前 generation） =====
	setConsumerGeneration(consumersRound3, gen)
	if testCanRebalance(context.Background(), snapshots, repo, groupID, consumersRound3, partitionHash, gen) {
		t.Fatal("轮次4: 稳定状态不应该需要 rebalance")
	}
	t.Log("轮次4: 稳定状态确认")
}

// TestRaceCondition_NewConsumerDuringRebalance 竞态条件测试
func TestRaceCondition_NewConsumerDuringRebalance(t *testing.T) {
	groupID := "test-race"
	topic := "test-topic"
	topics := []string{topic}
	partitions := []types.PartitionInfo{{Topic: topic, Partition: 0}}
	partitionHash := topic + ":1"

	consumerB := "consumer-b"
	consumerAPrime := "consumer-a-new"

	p := func(topic string, partition uint) types.PartitionInfo {
		return types.PartitionInfo{Topic: topic, Partition: partition}
	}

	// 第一次 rebalance：只有 B
	var gen uint = 1
	snapshots, repo := newTestSnapshotInfo(nil)
	consumersOnlyB := makeTestConsumersWithTopics(topics, consumerB)
	assign1 := calculateAssignments(consumersOnlyB, partitions, nil)
	snapshots.updateGroupSnapshot(groupID, gen, consumersOnlyB, partitionHash, nil)
	t.Logf("第一次 rebalance: B=%v", assign1[consumerB])

	// 下一轮 scan: A' 上线
	gen = 2
	manual := map[string][]types.PartitionInfo{consumerAPrime: {p(topic, 0)}}
	repo.matchResult = manual
	consumersWithAPrime := makeTestConsumersWithTopics(topics, consumerAPrime, consumerB)

	if !testCanRebalance(context.Background(), snapshots, repo, groupID, consumersWithAPrime, partitionHash, gen) {
		t.Fatal("A' 上线后 isRebalanceNeeded 必须返回 true")
	}

	assign2 := calculateAssignments(consumersWithAPrime, partitions, manual)
	snapshots.updateGroupSnapshot(groupID, gen, consumersWithAPrime, partitionHash, manual)

	if !reflect.DeepEqual(assign2[consumerAPrime], []types.PartitionInfo{p(topic, 0)}) {
		t.Fatalf("A' 应该得到 p0，实际: %v", assign2[consumerAPrime])
	}
	t.Logf("第二次 rebalance: A'=%v, B=%v", assign2[consumerAPrime], assign2[consumerB])
}

// TestEdgeCase_ManualHashChange_AfterConsumerIDChange hash 变化边界测试
func TestEdgeCase_ManualHashChange_AfterConsumerIDChange(t *testing.T) {
	topic := "test-topic"
	p0 := types.PartitionInfo{Topic: topic, Partition: 0}

	oldManual := map[string][]types.PartitionInfo{"host:mac:uuid-old": {p0}}
	newManual := map[string][]types.PartitionInfo{"host:mac:uuid-new": {p0}}

	oldHash := hashManualAssignments(oldManual)
	newHash := hashManualAssignments(newManual)
	emptyHash := hashManualAssignments(nil)

	if oldHash == newHash {
		t.Fatalf("旧 ID 和新 ID 的 manualHash 不应该相同: %q", oldHash)
	}
	if oldHash == emptyHash {
		t.Fatal("有匹配和无匹配的 manualHash 不应该相同")
	}
	if newHash == emptyHash {
		t.Fatal("新 ID 匹配和无匹配的 manualHash 不应该相同")
	}
	t.Logf("oldHash=%q, newHash=%q, emptyHash=%q", oldHash, newHash, emptyHash)
}

// TestBug_TriggerRebalanceAPI_GenerationMismatch 复现并验证 Bug 修复：
// 旧版 TriggerRebalance API 只修改心跳表 generation_id，协调器无法感知。
// 新版 API 修改 mq_consumer_group_generations 表的 generation_id，
// 协调器通过 snapshot.generationID 与数据库中的 generation_id 对比来检测。
func TestBug_TriggerRebalanceAPI_GenerationMismatch(t *testing.T) {
	groupID := "test-trigger-api"
	topic := "approvalflow:transferpolicy"
	topics := []string{topic}
	partitions := []types.PartitionInfo{{Topic: topic, Partition: 0}}
	partitionHash := topic + ":1"

	consumerAPrime := "host:mac:uuid-A-prime"
	consumerB := "host2:mac2:uuid-B"

	p := func(topic string, partition uint) types.PartitionInfo {
		return types.PartitionInfo{Topic: topic, Partition: partition}
	}

	// === 阶段 1: 正常 rebalance 后进入稳定状态 ===
	var gen uint = 5
	manual := map[string][]types.PartitionInfo{consumerAPrime: {p(topic, 0)}}
	snapshots, repo := newTestSnapshotInfo(manual)
	consumers := makeTestConsumersWithTopics(topics, consumerAPrime, consumerB)

	calculateAssignments(consumers, partitions, manual)
	snapshots.updateGroupSnapshot(groupID, gen, consumers, partitionHash, manual)
	setConsumerGeneration(consumers, gen)

	// 稳定状态：snapshot.generationID == 5，传入 currentGenerationID == 5
	if testCanRebalance(context.Background(), snapshots, repo, groupID, consumers, partitionHash, gen) {
		t.Fatal("稳定状态不应该需要 rebalance")
	}

	// === 阶段 2: 模拟 TriggerRebalance API 递增了 generations 表 ===
	// API 执行了: UPDATE mq_consumer_group_generations SET generation_id = generation_id + 1
	// 数据库中 generation_id 变成了 6，但 snapshot.generationID 仍然是 5
	apiIncrementedGen := gen + 1

	// 🔥 核心验证：协调器检测到 generation_id 不匹配，应该触发 rebalance
	if !testCanRebalance(context.Background(), snapshots, repo, groupID, consumers, partitionHash, apiIncrementedGen) {
		t.Fatal("TriggerRebalance API 递增 generation_id 后，isRebalanceNeeded 应该返回 true")
	}
	t.Log("✓ 验证通过: API 递增 generation_id 后，协调器能检测到并触发 rebalance")

	// === 阶段 3: 协调器执行 rebalance 后恢复稳定 ===
	snapshots.updateGroupSnapshot(groupID, apiIncrementedGen, consumers, partitionHash, manual)
	setConsumerGeneration(consumers, apiIncrementedGen)

	if testCanRebalance(context.Background(), snapshots, repo, groupID, consumers, partitionHash, apiIncrementedGen) {
		t.Fatal("rebalance 完成后应该恢复稳定状态")
	}
	t.Log("✓ 验证通过: rebalance 完成后恢复稳定")
}

// TestScenario_CoordinatorRestart_SnapshotLost 协调器重启场景
func TestScenario_CoordinatorRestart_SnapshotLost(t *testing.T) {
	topic := "approvalflow:transferpolicy"
	topics := []string{topic}
	partitions := []types.PartitionInfo{{Topic: topic, Partition: 0}}
	partitionHash := topic + ":1"
	groupID := "test-restart"
	var gen uint = 10

	consumerAPrime := "host:mac:uuid-new"
	consumerB := "other-host:mac:uuid-b"

	p := func(topic string, partition uint) types.PartitionInfo {
		return types.PartitionInfo{Topic: topic, Partition: partition}
	}

	manual := map[string][]types.PartitionInfo{consumerAPrime: {p(topic, 0)}}
	snapshots, repo := newTestSnapshotInfo(manual)
	consumers := makeTestConsumersWithTopics(topics, consumerAPrime, consumerB)

	if !testCanRebalance(context.Background(), snapshots, repo, groupID, consumers, partitionHash, gen) {
		t.Fatal("协调器重启后 snapshot 为空，应该需要 rebalance")
	}

	assign := calculateAssignments(consumers, partitions, manual)
	snapshots.updateGroupSnapshot(groupID, gen, consumers, partitionHash, manual)
	setConsumerGeneration(consumers, gen)

	if !reflect.DeepEqual(assign[consumerAPrime], []types.PartitionInfo{p(topic, 0)}) {
		t.Fatalf("A' 应该得到 p0，实际: %v", assign[consumerAPrime])
	}
	if len(assign[consumerB]) != 0 {
		t.Fatalf("B 不应该有分区，实际: %v", assign[consumerB])
	}

	if testCanRebalance(context.Background(), snapshots, repo, groupID, consumers, partitionHash, gen) {
		t.Fatal("rebalance 后稳定状态不应该再需要 rebalance")
	}
	t.Logf("协调器重启场景通过: A'=%v, B=%v", assign[consumerAPrime], assign[consumerB])
}

// TestCanRebalance_ConsumerGenerationLag_Triggers 死锁场景回归：
// 心跳行被删除后由消费者 Upsert 以 generation_id=0 重建，此时成员集合、订阅、
// 分区 hash、组 generation 全部"看似无变化"，若不检测行 generation 落后，
// 协调器与消费者会互相认为"无变化"而永久死锁（消费者空 assignment，需重启才恢复）
func TestCanRebalance_ConsumerGenerationLag_Triggers(t *testing.T) {
	snapshots, repo := newTestSnapshotInfo(nil)
	groupID := "lag-group"
	topics := []string{"test-topic"}
	var gen uint = 10

	consumers := makeTestConsumersWithTopics(topics, "consumer-a", "consumer-b")
	setConsumerGeneration(consumers, gen)
	snapshots.updateGroupSnapshot(groupID, gen, consumers, "test-topic:1", nil)

	// consumer-b 的心跳行被重建为 generation_id=0
	consumers[1].GenerationID = 0

	if !testCanRebalance(context.Background(), snapshots, repo, groupID, consumers, "test-topic:1", gen) {
		t.Fatal("消费者心跳行 generation 落后于组 generation 时必须触发 rebalance")
	}
}

// TestCanRebalance_ConsumerGenerationMatch_NoTrigger 防过度触发：
// 全部行 generation 与组 generation 一致的稳定态不应触发 rebalance
func TestCanRebalance_ConsumerGenerationMatch_NoTrigger(t *testing.T) {
	snapshots, repo := newTestSnapshotInfo(nil)
	groupID := "match-group"
	topics := []string{"test-topic"}
	var gen uint = 10

	consumers := makeTestConsumersWithTopics(topics, "consumer-a", "consumer-b")
	setConsumerGeneration(consumers, gen)
	snapshots.updateGroupSnapshot(groupID, gen, consumers, "test-topic:1", nil)

	if testCanRebalance(context.Background(), snapshots, repo, groupID, consumers, "test-topic:1", gen) {
		t.Fatal("全部 generation 匹配的稳定态不应触发 rebalance")
	}
}

// TestCoordinatorTask_TryRebalance_HeartbeatRowRecreated_ForcesRebalance 端到端回归：
// 第一次 rebalance 后某消费者心跳行被重建为 generation_id=0（行删除后 Upsert 重建），
// 第二次扫描必须强制再次 rebalance 修复该行
func TestCoordinatorTask_TryRebalance_HeartbeatRowRecreated_ForcesRebalance(t *testing.T) {
	groupID := "test-group"
	topicName := "test-topic"
	consumers := makeTestConsumersWithTopics([]string{topicName}, "consumer-a")
	groupRepo := &rebalanceGroupRepo{heartbeats: consumers}

	task := &coordinatorTask{
		cfg: &CoordinatorConfig{
			HeartbeatTimeout: 30 * time.Second,
			RebalanceTimeout: time.Second,
		},
		rebalancingLocks: newGroupLocks(),
		snapshotInfo:     newSnapshotInfo(),
		repos: repos{
			topicRepo:            &rebalanceTopicRepo{topics: []*topic.Topic{{Name: topicName, PartitionCount: 1}}},
			messageRepo:          rebalanceMessageRepo{},
			heartbeatRepo:        &rebalanceHeartbeatRepo{active: consumers},
			groupRepo:            groupRepo,
			progressRepo:         rebalanceProgressRepo{},
			manualAssignmentRepo: &mockManualAssignmentRepo{},
		},
	}

	if err := task.tryRebalance(context.Background(), groupID); err != nil {
		t.Fatalf("第一次 rebalance 失败: %v", err)
	}
	if groupRepo.increments != 1 {
		t.Fatalf("第一次 rebalance 应该递增一次 generation，实际: %d", groupRepo.increments)
	}

	// 模拟心跳行被删除后由消费者 Upsert 以 generation_id=0 重建
	consumers[0].GenerationID = 0

	if err := task.tryRebalance(context.Background(), groupID); err != nil {
		t.Fatalf("第二次 rebalance 检查失败: %v", err)
	}
	if groupRepo.increments != 2 {
		t.Fatalf("心跳行 generation 落后时应该强制再次 rebalance，increments 实际: %d", groupRepo.increments)
	}
	if consumers[0].GenerationID != groupRepo.generation {
		t.Fatalf("rebalance 后心跳行 generation 应被修复为 %d，实际: %d", groupRepo.generation, consumers[0].GenerationID)
	}
}
