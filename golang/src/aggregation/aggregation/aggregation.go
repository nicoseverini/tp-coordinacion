package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitItemMap  map[string]map[string]fruititem.FruitItem
	topSize       int
	sumAmount     int
	eofCount      map[string]int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		errC := outputQueue.Close()
		if errC != nil {
			return nil, errC
		}
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitItemMap:  map[string]map[string]fruititem.FruitItem{},
		topSize:       config.TopSize,
		sumAmount:     config.SumAmount,
		eofCount:      map[string]int{},
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	taskId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		aggregation.eofCount[taskId]++
		slog.Info("EOF recibido",
			"taskId", taskId,
			"count", aggregation.eofCount[taskId],
			"expected", aggregation.sumAmount,
		)
		if aggregation.eofCount[taskId] == aggregation.sumAmount {
			if err := aggregation.handleEndOfRecordsMessage(taskId); err != nil {
				slog.Error("While handling end of record message", "err", err)
			}
			delete(aggregation.eofCount, taskId)
		}
		return
	}

	aggregation.handleDataMessage(taskId, fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(taskId string) error {
	slog.Info("Received End Of Records message", "taskId", taskId)

	fruitMap, ok := aggregation.fruitItemMap[taskId]
	if !ok {
		slog.Debug("While getting fruitItemMap")
		return nil
	}

	fruitTopRecords := aggregation.buildFruitTop(fruitMap)
	message, err := inner.SerializeResultMessage(taskId, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	message, err = inner.SerializeEOFMessage(taskId)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	delete(aggregation.fruitItemMap, taskId)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(taskId string, fruitRecords []fruititem.FruitItem) {
	if _, ok := aggregation.fruitItemMap[taskId]; !ok {
		aggregation.fruitItemMap[taskId] = map[string]fruititem.FruitItem{}
	}
	for _, fruitRecord := range fruitRecords {
		current, exists := aggregation.fruitItemMap[taskId][fruitRecord.Fruit]
		if exists {
			aggregation.fruitItemMap[taskId][fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			aggregation.fruitItemMap[taskId][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(fruitMap map[string]fruititem.FruitItem) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(fruitMap))
	for _, item := range fruitMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
