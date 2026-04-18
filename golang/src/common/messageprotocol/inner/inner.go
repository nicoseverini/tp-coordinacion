package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type QueryResult struct {
	TaskID string          `json:"query_id"`
	Data   [][]interface{} `json:"data"`
}

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(taskId string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	var data [][]interface{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	queryResult := QueryResult{
		TaskID: taskId,
		Data:   data,
	}

	body, err := json.Marshal(queryResult)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (string, []fruititem.FruitItem, bool, error) {
	var queryResult QueryResult

	if err := json.Unmarshal([]byte(message.Body), &queryResult); err != nil {
		return "", nil, false, err
	}

	var fruitRecords []fruititem.FruitItem
	for _, datum := range queryResult.Data {
		if len(datum) != 2 {
			return "", nil, false, errors.New("Datum is not an array")
		}

		fruit, ok := datum[0].(string)
		if !ok {
			return "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := datum[1].(float64)
		if !ok {
			return "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return queryResult.TaskID, fruitRecords, len(fruitRecords) == 0, nil
}
