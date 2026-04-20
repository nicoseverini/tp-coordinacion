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

	stateMu   sync.Mutex
	partialBy map[string]map[string]fruititem.FruitItem
	eofCount  map[string]int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
		partialBy:         map[string]map[string]fruititem.FruitItem{},
		eofCount:          map[string]int{},
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

	queryID, fruitRecords, isEof, messageType, _, err := inner.DeserializeMessageWithMetadata(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		_ = nack
		return
	}

	if messageType == inner.MessageTypeSumEOF {
		return
	}

	if isEof || messageType == inner.MessageTypeEOF {
		join.handleEOF(queryID)
		return
	}

	join.handleData(queryID, fruitRecords)
}

func (join *Join) handleData(queryID string, fruitRecords []fruititem.FruitItem) {
	join.stateMu.Lock()
	defer join.stateMu.Unlock()

	if _, ok := join.partialBy[queryID]; !ok {
		join.partialBy[queryID] = map[string]fruititem.FruitItem{}
	}
	for _, fruitRecord := range fruitRecords {
		current, exists := join.partialBy[queryID][fruitRecord.Fruit]
		if exists {
			join.partialBy[queryID][fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			join.partialBy[queryID][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (join *Join) handleEOF(queryID string) {
	join.stateMu.Lock()

	join.eofCount[queryID]++
	received := join.eofCount[queryID]
	expected := join.aggregationAmount

	join.stateMu.Unlock()

	slog.Info("Join EOF recibido", "queryID", queryID, "count", received, "expected", expected)
	if received < expected {
		return
	}

	top := join.finishTask(queryID)
	resultMsg, err := inner.SerializeResultMessage(queryID, top)
	if err != nil {
		slog.Error("While serializing final result", "queryID", queryID, "err", err)
		return
	}

	if err := join.outputQueue.Send(*resultMsg); err != nil {
		slog.Error("While sending final result", "queryID", queryID, "err", err)
	}
}

func (join *Join) finishTask(queryID string) []fruititem.FruitItem {
	join.stateMu.Lock()
	defer join.stateMu.Unlock()

	fruitMap := join.partialBy[queryID]

	delete(join.partialBy, queryID)
	delete(join.eofCount, queryID)

	return buildTopK(fruitMap, join.topSize)
}

func buildTopK(fruitMap map[string]fruititem.FruitItem, k int) []fruititem.FruitItem {
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
	finalTopSize := min(k, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
