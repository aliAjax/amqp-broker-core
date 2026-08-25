package domain

import (
	"path"
	"strings"
)

type Matcher interface {
	Match(routingKey string, headers map[string]string, pattern string, filter map[string]string) bool
}
type StandardMatcher struct{}

func (StandardMatcher) Match(key string, headers map[string]string, pattern string, filter map[string]string) bool {
	if pattern != "" && !topicMatch(pattern, key) {
		return false
	}
	for k, want := range filter {
		got, ok := headers[k]
		if !ok || got != want {
			return false
		}
	}
	return true
}
func topicMatch(pattern, key string) bool {
	if pattern == "#" || pattern == "" {
		return true
	}
	pp := strings.Split(pattern, ".")
	kp := strings.Split(key, ".")
	return matchParts(pp, kp)
}
func matchParts(pattern, key []string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case "#":
			if len(pattern) == 1 {
				return true
			}
			for i := 0; i <= len(key); i++ {
				if matchParts(pattern[1:], key[i:]) {
					return true
				}
			}
			return false
		case "*":
			if len(key) == 0 {
				return false
			}
			pattern = pattern[1:]
			key = key[1:]
		default:
			if len(key) == 0 {
				return false
			}
			ok, _ := path.Match(pattern[0], key[0])
			if !ok {
				return false
			}
			pattern = pattern[1:]
			key = key[1:]
		}
	}
	return len(key) == 0
}
