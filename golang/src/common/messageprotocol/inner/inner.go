package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type QueryResult struct {
	Type     string          `json:"type,omitempty"`
	TaskID   string          `json:"query_id"`
	Data     [][]interface{} `json:"data"`
	EOF      *bool           `json:"eof,omitempty"`
	SenderID *int            `json:"sender_id,omitempty"`
}

const (
	MessageTypeData   = "data"
	MessageTypeEOF    = "eof"
	MessageTypeResult = "result"
	MessageTypeSumEOF = "sum_eof"
)

func SerializeMessage(taskId string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	return serializeMessage(taskId, fruitRecords, MessageTypeData, nil, nil)
}

func SerializeResultMessage(taskId string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	return serializeMessage(taskId, fruitRecords, MessageTypeResult, nil, nil)
}

func SerializeEOFMessage(taskId string) (*middleware.Message, error) {
	isEOF := true
	return serializeMessage(taskId, nil, MessageTypeEOF, &isEOF, nil)
}

func SerializeControlEOFMessage(taskId string, senderID int) (*middleware.Message, error) {
	isEOF := true
	return serializeMessage(taskId, nil, MessageTypeSumEOF, &isEOF, &senderID)
}

func serializeMessage(taskId string, fruitRecords []fruititem.FruitItem, messageType string, eof *bool, senderID *int) (*middleware.Message, error) {
	var data [][]interface{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	queryResult := QueryResult{
		Type:     messageType,
		TaskID:   taskId,
		Data:     data,
		EOF:      eof,
		SenderID: senderID,
	}

	body, err := json.Marshal(queryResult)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (string, []fruititem.FruitItem, bool, error) {
	taskID, fruitRecords, isEOF, _, _, err := DeserializeMessageWithMetadata(message)
	return taskID, fruitRecords, isEOF, err
}

func DeserializeMessageWithMetadata(message *middleware.Message) (string, []fruititem.FruitItem, bool, string, *int, error) {
	var queryResult QueryResult

	if err := json.Unmarshal([]byte(message.Body), &queryResult); err != nil {
		return "", nil, false, "", nil, err
	}

	var fruitRecords []fruititem.FruitItem
	for _, datum := range queryResult.Data {
		if len(datum) != 2 {
			return "", nil, false, "", nil, errors.New("datum is not an array")
		}

		fruit, ok := datum[0].(string)
		if !ok {
			return "", nil, false, "", nil, errors.New("datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := datum[1].(float64)
		if !ok {
			return "", nil, false, "", nil, errors.New("datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	isEOF := len(fruitRecords) == 0
	if queryResult.EOF != nil {
		isEOF = *queryResult.EOF
	}
	if queryResult.Type == MessageTypeEOF {
		isEOF = true
	}

	messageType := queryResult.Type
	if messageType == "" {
		messageType = MessageTypeData
	}

	return queryResult.TaskID, fruitRecords, isEOF, messageType, queryResult.SenderID, nil
}
