package join

import "github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"

type JoinTask struct {
	Fruits    fruititem.ByFruit
	EOFCount  int
	Completed bool
}

func NewJoinTask() *JoinTask {
	return &JoinTask{Fruits: fruititem.ByFruit{}}
}
