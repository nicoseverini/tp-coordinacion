package sum

import "github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"

type TaskState struct {
	Fruits          fruititem.ByFruit
	PendingMessages int
	EOFReceived     bool
	FlushDone       bool
}

func NewTaskState() *TaskState {
	return &TaskState{Fruits: fruititem.ByFruit{}}
}

func (task *TaskState) AddData(records []fruititem.FruitItem) {
	task.PendingMessages++
	fruititem.AccumulateByFruit(task.Fruits, records)
	task.PendingMessages--
}

func (task *TaskState) MarkEOF() {
	task.EOFReceived = true
}

func (task *TaskState) CanFlush() bool {
	return task.EOFReceived && task.PendingMessages == 0 && !task.FlushDone
}

func (task *TaskState) MarkFlushed() {
	task.FlushDone = true
}
