package sqlite

import "github.com/viant/slack-ai-gateway/internal/storage"

// Use the shared diagnostic metadata across storage engines.
type Status = storage.Status
type SlackAgentSessionPublicationMetadata = storage.SlackAgentSessionPublicationMetadata
type GatewayJobMetadata = storage.GatewayJobMetadata
type ConversationBindingMetadata = storage.ConversationBindingMetadata
type SlackOutgoingMessageMetadata = storage.SlackOutgoingMessageMetadata
