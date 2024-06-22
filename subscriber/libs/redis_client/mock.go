package redisclient

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type MockRedis struct {
	data  map[string]interface{}
	mutex sync.RWMutex
}

func NewMockRedis() *MockRedis {
	return &MockRedis{
		data: make(map[string]interface{}),
	}
}

func reverse(s []string) []string {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}

func (m *MockRedis) LPush(key string, values ...interface{}) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.data[key]; !ok {
		m.data[key] = []interface{}{}
	}

	list, ok := m.data[key].([]interface{})
	if !ok {
		return fmt.Errorf("value at key %s is not a list", key)
	}

	m.data[key] = append(values, list...)
	return nil
}

func (m *MockRedis) RPush(key string, values ...interface{}) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.data[key]; !ok {
		m.data[key] = []interface{}{}
	}

	list, ok := m.data[key].([]interface{})
	if !ok {
		return fmt.Errorf("value at key %s is not a list", key)
	}

	m.data[key] = append(list, values...)
	return nil
}

func (m *MockRedis) BRPop(timeout time.Duration, keys ...string) ([]string, error) {
	return m.BLPop(timeout, reverse(keys)...)
}

func (m *MockRedis) BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	start := time.Now()

	for {
		for _, key := range keys {
			m.mutex.Lock()
			list, ok := m.data[key].([]interface{})
			if !ok || len(list) == 0 {
				m.mutex.Unlock()
				continue
			}

			value := list[0]
			m.data[key] = list[1:]
			m.mutex.Unlock()
			return []string{key, fmt.Sprintf("%v", value)}, nil
		}

		if time.Since(start) >= timeout {
			return nil, errors.New("timeout")
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (m *MockRedis) Set(key string, value interface{}, expirations ...time.Duration) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.data[key] = value
	if len(expirations) > 0 {
		go m.expireKey(key, expirations[0])
	}
	return nil
}

func (m *MockRedis) expireKey(key string, expiration time.Duration) {
	time.Sleep(expiration)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.data, key)
}

func (m *MockRedis) Get(key string) (string, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if value, ok := m.data[key]; ok {
		return fmt.Sprintf("%v", value), nil
	}
	return "", errors.New("key not found")
}

func (m *MockRedis) Del(keys ...string) (int64, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var deleted int64
	for _, key := range keys {
		if _, ok := m.data[key]; !ok {
			continue
		}
		delete(m.data, key)
		deleted++
	}
	return deleted, nil
}

func (m *MockRedis) Clear() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.data = make(map[string]interface{})
	return nil
}
