package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const EventSessionRepaired = "session/repaired"

// CoreRepairInitializer closes incomplete Stage 1 core Turn/Step boundaries
// before an Actor is published by Registry. Agent-specific attempt and claim
// repair is contributed by internal/agent through another initializer.
type CoreRepairInitializer struct{}

func (CoreRepairInitializer) Initialize(_ context.Context, snapshot Snapshot) ([]NewEvent, error) {
	core := snapshot.Core()
	if core.StepID == "" && core.TurnID == "" {
		return nil, nil
	}

	events := make([]NewEvent, 0, 3)
	closed := make([]string, 0, 2)
	if core.StepID != "" {
		data, err := json.Marshal(map[string]any{"reason": "interrupted", "synthetic": true})
		if err != nil {
			return nil, err
		}
		eventID, err := repairEventID()
		if err != nil {
			return nil, err
		}
		events = append(events, NewEvent{
			SchemaVersion: SchemaVersion{Major: 1},
			EventType:     "step/end",
			EventID:       eventID,
			ReplayPolicy:  ReplayRequired,
			Data:          data,
			TurnID:        core.TurnID,
			StepID:        core.StepID,
		})
		closed = append(closed, "step")
	}
	if core.TurnID != "" {
		data, err := json.Marshal(map[string]any{"reason": "interrupted", "synthetic": true})
		if err != nil {
			return nil, err
		}
		eventID, err := repairEventID()
		if err != nil {
			return nil, err
		}
		events = append(events, NewEvent{
			SchemaVersion: SchemaVersion{Major: 1},
			EventType:     "turn/end",
			EventID:       eventID,
			ReplayPolicy:  ReplayRequired,
			Data:          data,
			TurnID:        core.TurnID,
		})
		closed = append(closed, "turn")
	}

	data, err := json.Marshal(map[string]any{
		"reason":          "incomplete-boundary",
		"synthetic":       true,
		"closed":          closed,
		"previousHeadSeq": snapshot.HeadSeq,
	})
	if err != nil {
		return nil, err
	}
	eventID, err := repairEventID()
	if err != nil {
		return nil, err
	}
	events = append(events, NewEvent{
		SchemaVersion: SchemaVersion{Major: 1},
		EventType:     EventSessionRepaired,
		EventID:       eventID,
		ReplayPolicy:  ReplayIgnorable,
		Data:          data,
	})
	return events, nil
}

func repairEventID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate repair event id: %w", err)
	}
	return "repair-" + hex.EncodeToString(value[:]), nil
}
