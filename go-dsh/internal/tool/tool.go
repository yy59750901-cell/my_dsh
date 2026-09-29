package tool

import (
	"context"
	"encoding/json"
	"time"
)

type Definition struct {
	Name                 string
	Description          string
	InputSchema          json.RawMessage
	OutputSchema         json.RawMessage
	Version              string
	Generation           uint64
	RequiredCapabilities []string
	RequiresApproval     bool
}

type Call struct {
	ID                string
	SessionID         string
	TurnID            string
	StepID            string
	Name              string
	Arguments         json.RawMessage
	DefinitionVersion string
	Generation        uint64
	IdempotencyKey    string
	Timeout           time.Duration
}

type Result struct {
	CallID     string
	Content    string
	Structured json.RawMessage
	Err        error
	Duration   time.Duration
	Truncated  bool
	Redacted   bool
}

type Tool interface {
	Definition() Definition
	Execute(ctx context.Context, call Call) (Result, error)
}
