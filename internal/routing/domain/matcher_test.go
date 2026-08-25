package domain

import "testing"

func TestTopicAndHeaderMatching(t *testing.T) {
	m := StandardMatcher{}
	if !m.Match("billing.created", map[string]string{"region": "apac"}, "billing.*", map[string]string{"region": "apac"}) {
		t.Fatal("expected match")
	}
	if !m.Match("billing.invoice.created", nil, "billing.#", nil) {
		t.Fatal("expected hash match")
	}
	if m.Match("billing.created", map[string]string{"region": "eu"}, "billing.*", map[string]string{"region": "apac"}) {
		t.Fatal("unexpected header match")
	}
}
