package join

import (
	"log/slog"
	"sort"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	aggregationAmount int
	topSize           int

	stateMu sync.Mutex
	tasks   map[string]*JoinTask
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		_ = inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
		tasks:             map[string]*JoinTask{},
	}, nil
}

func (join *Join) Run() {
	err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
	if err != nil {
		slog.Error("While consuming input queue", "err", err)
	}
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	queryID, fruitRecords, isEOF, messageType, _, err := inner.DeserializeMessageWithMetadata(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		_ = nack
		return
	}

	if messageType == inner.MessageTypeSumEOF {
		return
	}

	if isEOF || messageType == inner.MessageTypeEOF {
		join.handleEOF(queryID)
		return
	}

	join.handleData(queryID, fruitRecords)
}

func (join *Join) handleData(queryID string, fruitRecords []fruititem.FruitItem) {
	join.stateMu.Lock()
	defer join.stateMu.Unlock()

	task := join.getOrCreateTaskLocked(queryID)
	if task.Completed {
		return
	}
	fruititem.AccumulateByFruit(task.Fruits, fruitRecords)
}

func (join *Join) handleEOF(queryID string) {
	join.stateMu.Lock()
	task := join.getOrCreateTaskLocked(queryID)
	if task.Completed {
		join.stateMu.Unlock()
		return
	}

	task.EOFCount++
	received := task.EOFCount
	expected := join.aggregationAmount
	if received < expected {
		join.stateMu.Unlock()
		slog.Info("Join EOF received", "queryID", queryID, "count", received, "expected", expected)
		return
	}

	task.Completed = true
	top := buildTopK(task.Fruits, join.topSize)
	task.Fruits = nil
	join.stateMu.Unlock()

	slog.Info("Join EOF received", "queryID", queryID, "count", received, "expected", expected)

	resultMsg, err := inner.SerializeResultMessage(queryID, top)
	if err != nil {
		slog.Error("While serializing final result", "queryID", queryID, "err", err)
		return
	}

	if err := join.outputQueue.Send(*resultMsg); err != nil {
		slog.Error("While sending final result", "queryID", queryID, "err", err)
	}
}

func (join *Join) getOrCreateTaskLocked(queryID string) *JoinTask {
	task, ok := join.tasks[queryID]
	if !ok {
		task = NewJoinTask()
		join.tasks[queryID] = task
	}
	return task
}

func buildTopK(fruitMap fruititem.ByFruit, k int) []fruititem.FruitItem {
	if len(fruitMap) == 0 {
		return nil
	}

	fruitItems := make([]fruititem.FruitItem, 0, len(fruitMap))
	for _, item := range fruitMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := minInt(k, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
