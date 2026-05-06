package raft

import (
	"reflect"
	"sync"
)

// eventDispatcher is responsible for managing listeners for named events
// and dispatching event notifications to those listeners.

/*
eventDispatcher 负责管理命名事件的监听器，并将对应的事件通知到这些监听器,
使用了读写锁保证并发场景下的安全性。
*/
type eventDispatcher struct {
	sync.RWMutex
	source interface{}
	// name -> []EventListener
	listeners map[string]eventListeners
}

// EventListener is a function that can receive event notifications.
// EventListener 可以接收事件通知的函数
type EventListener func(Event)

// EventListeners represents a collection of individual listeners.
// eventListeners 代表一组独立的监听器集合
type eventListeners []EventListener

// newEventDispatcher creates a new eventDispatcher instance.
// newEventDispatcher 创建一个新的 eventDispatcher 示例
func newEventDispatcher(source interface{}) *eventDispatcher {
	return &eventDispatcher{
		source:    source,
		listeners: make(map[string]eventListeners),
	}
}

// AddEventListener adds a listener function for a given event type.
// AddEventListener 注册一个监听器函数用于指定的事件类型
func (d *eventDispatcher) AddEventListener(typ string, listener EventListener) {
	// 使用写锁
	d.Lock()
	defer d.Unlock()
	// 追加
	d.listeners[typ] = append(d.listeners[typ], listener)
}

// RemoveEventListener removes a listener function for a given event type.
// RemoveEventListener 移除指定的事件类型的监听器函数
func (d *eventDispatcher) RemoveEventListener(typ string, listener EventListener) {
	// 使用写锁
	d.Lock()
	defer d.Unlock()

	// Grab a reference to the function pointer once.
	// 获取函数指针的引用
	ptr := reflect.ValueOf(listener).Pointer()

	// Find listener by pointer and remove it.
	// 遍历监听器并移除
	listeners := d.listeners[typ]
	for i, l := range listeners {
		if reflect.ValueOf(l).Pointer() == ptr {
			d.listeners[typ] = append(listeners[:i], listeners[i+1:]...)
		}
	}
}

// DispatchEvent dispatches an event.
// DispatchEvent 分发事件到对应的监听器
func (d *eventDispatcher) DispatchEvent(e Event) {
	// 使用读锁
	d.RLock()
	defer d.RUnlock()

	// Automatically set the event source.
	// 设置事件的 source，即事件的来源
	if e, ok := e.(*event); ok {
		e.source = d.source
	}

	// Dispatch the event to all listeners.
	// 分发时间给对应监听器函数
	for _, l := range d.listeners[e.Type()] {
		l(e)
	}
}
