package util

import (
	"container/list"
	"sync"
)

type Queue struct {
	list *list.List
	lock sync.Mutex
}

func NewQueue() *Queue {
	return &Queue{
		list: list.New(),
	}
}

func (q *Queue) EnQueue(value interface{}) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.list.PushBack(value)
}

func (q *Queue) DeQueue() (interface{}, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	e := q.list.Front()
	if e == nil {
		return nil, false
	}
	v := e.Value
	q.list.Remove(e)
	return v, true
}
