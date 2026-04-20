package fruititem

func AccumulateByTask(byTask ByTask, taskID string, records []FruitItem) {
	taskMap, ok := byTask[taskID]
	if !ok {
		taskMap = ByFruit{}
		byTask[taskID] = taskMap
	}
	AccumulateByFruit(taskMap, records)
}

func AccumulateByFruit(byFruit ByFruit, records []FruitItem) {
	for _, fruitRecord := range records {
		if current, exists := byFruit[fruitRecord.Fruit]; exists {
			byFruit[fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			byFruit[fruitRecord.Fruit] = fruitRecord
		}
	}
}
