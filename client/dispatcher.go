package client

import (
	"strings"
	"sync"
)

// TopicMatch checks if an MQTT topic matches a topic filter with wildcards (+ and #).
func TopicMatch(filter, topic string) bool {
	if filter == topic {
		return true
	}

	// Topics starting with '$' are system topics and cannot match wildcards starting with '+' or '#' [MQTT-4.7.2-1]
	if len(topic) > 0 && topic[0] == '$' {
		if len(filter) == 0 || (filter[0] == '+' || filter[0] == '#') {
			return false
		}
	}

	filterParts := strings.Split(filter, "/")
	topicParts := strings.Split(topic, "/")

	fLen := len(filterParts)
	tLen := len(topicParts)

	for i := 0; i < fLen; i++ {
		fPart := filterParts[i]

		// Multi-level wildcard '#' matches all remaining levels
		if fPart == "#" {
			// '#' must be the last token in the filter
			return i == fLen-1
		}

		// Single-level wildcard '+' matches exactly one level
		if fPart == "+" {
			if i >= tLen {
				return false
			}
			continue
		}

		// Exact level match
		if i >= tLen || fPart != topicParts[i] {
			return false
		}
	}

	return fLen == tLen
}

type routeEntry struct {
	filter  string
	handler MessageHandler
}

// messageDispatcher manages subscription routes and dispatches messages to matching handlers.
type messageDispatcher struct {
	mu             sync.RWMutex
	routes         []routeEntry
	defaultHandler MessageHandler
}

func newDispatcher(defaultHandler MessageHandler) *messageDispatcher {
	return &messageDispatcher{
		routes:         make([]routeEntry, 0),
		defaultHandler: defaultHandler,
	}
}

func (d *messageDispatcher) setDefaultHandler(h MessageHandler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.defaultHandler = h
}

func (d *messageDispatcher) register(filter string, handler MessageHandler) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i, r := range d.routes {
		if r.filter == filter {
			d.routes[i].handler = handler
			return
		}
	}
	d.routes = append(d.routes, routeEntry{filter: filter, handler: handler})
}

func (d *messageDispatcher) unregister(filter string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i, r := range d.routes {
		if r.filter == filter {
			d.routes = append(d.routes[:i], d.routes[i+1:]...)
			return
		}
	}
}

func (d *messageDispatcher) dispatch(client *Client, msg Message) {
	d.mu.RLock()
	matchedHandlers := make([]MessageHandler, 0, len(d.routes))
	for _, r := range d.routes {
		if TopicMatch(r.filter, msg.Topic) {
			matchedHandlers = append(matchedHandlers, r.handler)
		}
	}
	fallback := d.defaultHandler
	d.mu.RUnlock()

	if len(matchedHandlers) == 0 {
		if fallback != nil {
			fallback(client, msg)
		}
		return
	}

	for _, handler := range matchedHandlers {
		if handler != nil {
			handler(client, msg)
		}
	}
}
