package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaPublisher struct {
	client       *kgo.Client
	accessTopic  string
	usageTopic   string
	enabled      bool
}

func NewKafkaPublisher(brokers []string, enabled bool, accessTopic, usageTopic string) (*KafkaPublisher, error) {
	if !enabled {
		return &KafkaPublisher{enabled: false}, nil
	}
	if accessTopic == "" {
		accessTopic = "byz.gateway.access"
	}
	if usageTopic == "" {
		usageTopic = "byz.api.usage"
	}
	// Idempotent producer (franz-go default) requires acks=all.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("byz-api-gateway"),
		kgo.ProducerBatchMaxBytes(1_000_000),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProduceRequestTimeout(2*time.Second),
		kgo.RecordRetries(3),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{
		client:      client,
		accessTopic: accessTopic,
		usageTopic:  usageTopic,
		enabled:     true,
	}, nil
}

func (p *KafkaPublisher) Close() {
	if p != nil && p.client != nil {
		p.client.Close()
	}
}

type GatewayAccessEvent struct {
	EventID        string  `json:"eventId"`
	Type           string  `json:"type"`
	OccurredAt     string  `json:"occurredAt"`
	RequestID      string  `json:"requestId"`
	Method         string  `json:"method"`
	Path           string  `json:"path"`
	Status         int     `json:"status"`
	DurationMs     int64   `json:"durationMs"`
	ClientIP       string  `json:"clientIp"`
	RouteID        *string `json:"routeId"`
	OrganizationID *string `json:"organizationId"`
	ClientID       *string `json:"clientId"`
}

type ApiUsageEvent struct {
	EventID        string  `json:"eventId"`
	Type           string  `json:"type"`
	OccurredAt     string  `json:"occurredAt"`
	OrganizationID string  `json:"organizationId"`
	TokenID        string  `json:"tokenId"`
	AppID          string  `json:"appId"`
	GrantType      string  `json:"grantType"`
	UserID         *string `json:"userId"`
	TenantID       *string `json:"tenantId"`
	Method         string  `json:"method"`
	Path           string  `json:"path"`
	Status         int     `json:"status"`
	DurationMs     int64   `json:"durationMs"`
}

func (p *KafkaPublisher) PublishAccess(ev GatewayAccessEvent) {
	if p == nil || !p.enabled || p.client == nil {
		return
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	key := ev.RequestID
	if key == "" {
		key = ev.EventID
	}
	p.produce(p.accessTopic, key, b)
}

func (p *KafkaPublisher) PublishUsage(ev ApiUsageEvent) {
	if p == nil || !p.enabled || p.client == nil {
		return
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	key := ev.TokenID
	if key == "" {
		key = ev.EventID
	}
	p.produce(p.usageTopic, key, b)
}

func (p *KafkaPublisher) produce(topic, key string, value []byte) {
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}
	p.client.Produce(context.Background(), rec, func(_ *kgo.Record, err error) {
		if err != nil {
			log.Printf("kafka produce %s: %v", topic, err)
		}
	})
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
