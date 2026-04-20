package messagehandler

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	id string
}

func NewMessageHandler() MessageHandler {
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	slog.Info("new handler created", "id", id)

	return MessageHandler{
		id: id,
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	slog.Info("sending DATA",
		"handlerId", messageHandler.id,
		"fruit", fruitRecord.Fruit,
		"amount", fruitRecord.Amount,
	)
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(messageHandler.id, data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	slog.Info("sending EOF", "handlerId", messageHandler.id)
	var data []fruititem.FruitItem
	return inner.SerializeMessage(messageHandler.id, data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	taskId, fruitRecords, _, messageType, _, err := inner.DeserializeMessageWithMetadata(message)
	if err != nil {
		return nil, err
	}
	if messageType != inner.MessageTypeResult {
		return nil, nil
	}
	//slog.Info("Received message", "taskId", taskId, "handlerId", messageHandler.id)

	if taskId != messageHandler.id {
		slog.Debug("message ignored (not for this handler)",
			"taskId", taskId,
			"handlerId", messageHandler.id,
		)
		return nil, nil
	}
	slog.Info("message accepted",
		"taskId", taskId,
		"records", fruitRecords,
	)

	return fruitRecords, nil
}
