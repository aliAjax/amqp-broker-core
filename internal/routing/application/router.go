package application

import (
	"context"
	"fmt"
	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	routing "github.com/enterprise/amqp-broker-core/internal/routing/domain"
	"sort"
)

type Route struct {
	Destination string
	Priority    int
}
type Router struct {
	addresses address.Repository
	bindings  address.BindingRepository
	matcher   routing.Matcher
}

func NewRouter(a address.Repository, b address.BindingRepository, m routing.Matcher) *Router {
	return &Router{addresses: a, bindings: b, matcher: m}
}
func (r *Router) Resolve(ctx context.Context, source, key string, headers map[string]string) ([]Route, error) {
	src, err := r.addresses.Get(ctx, source)
	if err != nil {
		return nil, fmt.Errorf("load source address: %w", err)
	}
	switch src.Kind {
	case address.KindQueue, address.KindTemporary:
		return []Route{{Destination: source}}, nil
	case address.KindFanout, address.KindTopic, address.KindDirect:
	default:
		return nil, fmt.Errorf("unsupported source kind %s", src.Kind)
	}
	bindings, err := r.bindings.ListBindings(ctx, source)
	if err != nil {
		return nil, fmt.Errorf("list bindings: %w", err)
	}
	seen := map[string]struct{}{}
	out := []Route{}
	for _, b := range bindings {
		pattern := b.RoutingKey
		if src.Kind == address.KindFanout {
			pattern = "#"
		}
		if src.Kind == address.KindDirect && pattern != key {
			continue
		}
		if !r.matcher.Match(key, headers, pattern, b.Filter) {
			continue
		}
		if _, ok := seen[b.Destination]; ok {
			continue
		}
		if _, err = r.addresses.Get(ctx, b.Destination); err != nil {
			continue
		}
		seen[b.Destination] = struct{}{}
		out = append(out, Route{Destination: b.Destination, Priority: b.Priority})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Priority > out[j].Priority })
	return out, nil
}
