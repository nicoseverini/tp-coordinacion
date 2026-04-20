package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const (
	sumControlExchangeName = "SUM_CONTROL_EXCHANGE"
	sumControlRoutingKey   = "SUM_CONTROL_EXCHANGE_ALL"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	id               int
	aggregationCount int
	inputQueue       middleware.Middleware
	outputExchanges  []middleware.Middleware
	controlPublisher middleware.Middleware
	controlConsumer  middleware.Middleware

	stateMu      sync.Mutex
	fruitItemMap map[string]map[string]fruititem.FruitItem
	eofProcessed map[string]bool
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchanges := make([]middleware.Middleware, config.AggregationAmount)
	for i := 0; i < config.AggregationAmount; i++ {
		routeKey := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
		exchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{routeKey}, connSettings)
		if err != nil {
			for _, created := range outputExchanges {
				if created != nil {
					_ = created.Close()
				}
			}
			errC := inputQueue.Close()
			if errC != nil {
				return nil, errC
			}
			return nil, err
		}
		outputExchanges[i] = exchange
	}

	controlPublisher, err := middleware.CreateExchangeMiddleware(sumControlExchangeName, []string{sumControlRoutingKey}, connSettings)
	if err != nil {
		for _, exchange := range outputExchanges {
			_ = exchange.Close()
		}
		_ = inputQueue.Close()
		return nil, err
	}

	controlConsumer, err := middleware.CreateExchangeMiddleware(sumControlExchangeName, []string{sumControlRoutingKey}, connSettings)
	if err != nil {
		_ = controlPublisher.Close()
		for _, exchange := range outputExchanges {
			_ = exchange.Close()
		}
		_ = inputQueue.Close()
		return nil, err
	}

	return &Sum{
		id:               config.Id,
		aggregationCount: config.AggregationAmount,
		inputQueue:       inputQueue,
		outputExchanges:  outputExchanges,
		controlPublisher: controlPublisher,
		controlConsumer:  controlConsumer,
		fruitItemMap:     map[string]map[string]fruititem.FruitItem{},
		eofProcessed:     map[string]bool{},
	}, nil
}

func (sum *Sum) Run() {
	go func() {
		err := sum.controlConsumer.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleControlMessage(msg, ack, nack)
		})
		if err != nil {
			slog.Error("While consuming control exchange", "err", err)
		}
	}()

	err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
	if err != nil {
		slog.Error("While consuming input queue", "err", err)
	}
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	taskId, fruitRecords, isEof, messageType, senderID, err := inner.DeserializeMessageWithMetadata(&msg)
	if err != nil {
		slog.Error("While deserializing input message", "err", err)
		return
	}

	if messageType == inner.MessageTypeSumEOF {
		sum.handleControlEOF(taskId, senderID)
		_ = nack
		return
	}

	if isEof {
		if err := sum.handleEOF(taskId, true); err != nil {
			slog.Error("While handling EOF from input", "taskId", taskId, "err", err)
		}
		_ = nack
		return
	}

	if err := sum.handleDataMessage(taskId, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
	_ = nack
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	taskId, _, _, messageType, senderID, err := inner.DeserializeMessageWithMetadata(&msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		return
	}

	if messageType != inner.MessageTypeSumEOF {
		slog.Debug("Ignoring unsupported control message", "messageType", messageType)
		_ = nack
		return
	}

	sum.handleControlEOF(taskId, senderID)
	_ = nack
}

func (sum *Sum) handleControlEOF(taskId string, senderID *int) {
	if senderID == nil {
		slog.Error("Control EOF without sender", "taskId", taskId)
		return
	}
	if *senderID == sum.id {
		return
	}
	if err := sum.handleEOF(taskId, false); err != nil {
		slog.Error("While handling EOF from control", "taskId", taskId, "senderId", *senderID, "err", err)
	}
}

func (sum *Sum) handleEOF(taskId string, publishControl bool) error {
	fruitMap, shouldProcess := sum.startTaskEOF(taskId)
	if !shouldProcess {
		return nil
	}

	slog.Info("Received End Of Records message", "taskId", taskId)

	for _, fruitRecord := range fruitMap {
		partition := sum.partitionForFruit(fruitRecord.Fruit)
		message, err := inner.SerializeMessage(taskId, []fruititem.FruitItem{fruitRecord})
		if err != nil {
			slog.Debug("While serializing partial sum message", "err", err)
			return err
		}
		if err := sum.outputExchanges[partition].Send(*message); err != nil {
			slog.Debug("While sending partial sum message", "err", err)
			return err
		}
	}

	eofMessage, err := inner.SerializeMessage(taskId, nil)
	if err != nil {
		slog.Debug("While serializing EOF to aggregation", "err", err)
		return err
	}
	for _, exchange := range sum.outputExchanges {
		if err := exchange.Send(*eofMessage); err != nil {
			slog.Debug("While sending EOF to aggregation", "err", err)
			return err
		}
	}

	if publishControl {
		controlMessage, err := inner.SerializeControlEOFMessage(taskId, sum.id)
		if err != nil {
			slog.Debug("While serializing control EOF message", "err", err)
			return err
		}
		if err := sum.controlPublisher.Send(*controlMessage); err != nil {
			slog.Debug("While publishing control EOF message", "err", err)
			return err
		}
	}

	return nil
}

func (sum *Sum) startTaskEOF(taskId string) (map[string]fruititem.FruitItem, bool) {
	sum.stateMu.Lock()
	defer sum.stateMu.Unlock()

	if sum.eofProcessed[taskId] {
		return nil, false
	}
	sum.eofProcessed[taskId] = true

	fruitMap := sum.fruitItemMap[taskId]
	delete(sum.fruitItemMap, taskId)
	return fruitMap, true
}

func (sum *Sum) partitionForFruit(fruit string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(fruit))
	return int(hasher.Sum32() % uint32(sum.aggregationCount))
}

func (sum *Sum) handleDataMessage(taskId string, fruitRecords []fruititem.FruitItem) error {
	sum.stateMu.Lock()
	defer sum.stateMu.Unlock()

	if sum.eofProcessed[taskId] {
		return nil
	}
	if _, ok := sum.fruitItemMap[taskId]; !ok {
		sum.fruitItemMap[taskId] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitRecords {
		current, exists := sum.fruitItemMap[taskId][fruitRecord.Fruit]
		if exists {
			sum.fruitItemMap[taskId][fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			sum.fruitItemMap[taskId][fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
