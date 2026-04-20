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

	stateMu   sync.Mutex
	tasks     map[string]*TaskState
	completed map[string]bool
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
		tasks:            map[string]*TaskState{},
		completed:        map[string]bool{},
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

	taskID, fruitRecords, isEOF, messageType, senderID, err := inner.DeserializeMessageWithMetadata(&msg)
	if err != nil {
		slog.Error("While deserializing input message", "err", err)
		return
	}

	if messageType == inner.MessageTypeSumEOF {
		sum.handleControlEOF(taskID, senderID)
		_ = nack
		return
	}

	if isEOF {
		if err := sum.processEOF(taskID, true); err != nil {
			slog.Error("While handling EOF from input", "taskId", taskID, "err", err)
		}
		_ = nack
		return
	}

	if err := sum.handleDataMessage(taskID, fruitRecords); err != nil {
		slog.Error("While handling data message", "taskId", taskID, "err", err)
	}
	_ = nack
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	taskID, _, _, messageType, senderID, err := inner.DeserializeMessageWithMetadata(&msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		return
	}
	if messageType != inner.MessageTypeSumEOF {
		return
	}

	sum.handleControlEOF(taskID, senderID)
	_ = nack
}

func (sum *Sum) handleControlEOF(taskID string, senderID *int) {
	if senderID == nil {
		slog.Error("Control EOF without sender", "taskId", taskID)
		return
	}
	if *senderID == sum.id {
		return
	}
	if err := sum.processEOF(taskID, false); err != nil {
		slog.Error("While handling EOF from control", "taskId", taskID, "senderId", *senderID, "err", err)
	}
}

func (sum *Sum) handleDataMessage(taskID string, fruitRecords []fruititem.FruitItem) error {
	shouldFlush := false

	sum.stateMu.Lock()
	task, exists := sum.getOrCreateTaskLocked(taskID)
	if !exists {
		sum.stateMu.Unlock()
		return nil
	}
	if task.FlushDone {
		sum.stateMu.Unlock()
		return nil
	}

	task.AddData(fruitRecords)
	if task.CanFlush() {
		task.MarkFlushed()
		shouldFlush = true
	}
	sum.stateMu.Unlock()

	if shouldFlush {
		return sum.flush(taskID)
	}
	return nil
}

func (sum *Sum) processEOF(taskID string, publishControl bool) error {
	shouldFlush := false
	pending := 0
	eofReceived := false

	sum.stateMu.Lock()
	task, exists := sum.getOrCreateTaskLocked(taskID)
	if exists {
		task.MarkEOF()
		pending = task.PendingMessages
		eofReceived = task.EOFReceived
		if task.CanFlush() {
			task.MarkFlushed()
			shouldFlush = true
		}
	}
	sum.stateMu.Unlock()

	if !exists {
		return nil
	}

	if publishControl {
		controlMessage, err := inner.SerializeControlEOFMessage(taskID, sum.id)
		if err != nil {
			slog.Debug("While serializing control EOF message", "err", err)
			return err
		}
		if err := sum.controlPublisher.Send(*controlMessage); err != nil {
			slog.Debug("While publishing control EOF message", "err", err)
			return err
		}
	}

	if shouldFlush {
		return sum.flush(taskID)
	}

	slog.Info("EOF registered, waiting pending",
		"taskId", taskID,
		"pending", pending,
		"eofReceived", eofReceived,
	)
	return nil
}

func (sum *Sum) getOrCreateTaskLocked(taskID string) (*TaskState, bool) {
	if sum.completed[taskID] {
		return nil, false
	}
	task, ok := sum.tasks[taskID]
	if !ok {
		task = NewTaskState()
		sum.tasks[taskID] = task
	}
	return task, true
}

func (sum *Sum) flush(taskID string) error {
	sum.stateMu.Lock()
	task, ok := sum.tasks[taskID]
	if !ok {
		sum.stateMu.Unlock()
		return nil
	}
	fruitMap := task.Fruits
	pending := task.PendingMessages
	eofReceived := task.EOFReceived
	delete(sum.tasks, taskID)
	sum.completed[taskID] = true
	sum.stateMu.Unlock()

	slog.Info("Flushing task",
		"taskId", taskID,
		"pending", pending,
		"eofReceived", eofReceived,
	)

	for _, fruitRecord := range fruitMap {
		partition := sum.partitionForFruit(fruitRecord.Fruit)
		message, err := inner.SerializeMessage(taskID, []fruititem.FruitItem{fruitRecord})
		if err != nil {
			slog.Debug("While serializing partial sum message", "err", err)
			return err
		}
		if err := sum.outputExchanges[partition].Send(*message); err != nil {
			slog.Debug("While sending partial sum message", "err", err)
			return err
		}
	}

	eofMessage, err := inner.SerializeEOFMessage(taskID)
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
	return nil
}

func (sum *Sum) partitionForFruit(fruit string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(fruit))
	return int(hasher.Sum32() % uint32(sum.aggregationCount))
}
