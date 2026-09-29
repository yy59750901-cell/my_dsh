package session_test

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"

	"github.com/yy59750901/go-dsh/internal/session"
)

func TestHappyTurnFixtureHasContinuousSequence(t *testing.T) {
	file, err := os.Open("../../testdata/events/v1/happy-turn.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var events []session.Event
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event session.Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("decode fixture: %v", err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if err := session.ValidateSequence(events, 0); err != nil {
		t.Fatal(err)
	}
	if got, want := len(events), 7; got != want {
		t.Fatalf("event count = %d, want %d", got, want)
	}
}

func TestValidateSequenceRejectsGap(t *testing.T) {
	events := []session.Event{{Seq: 1}, {Seq: 3}}
	if err := session.ValidateSequence(events, 0); err == nil {
		t.Fatal("expected sequence gap error")
	}
}

func TestUnknownEventReplayPolicy(t *testing.T) {
	known := map[string]struct{}{"session/created": {}}
	if err := session.ValidateReplayability([]session.Event{{
		Seq: 1, EventType: "extension/example", ReplayPolicy: session.ReplayIgnorable,
	}}, known); err != nil {
		t.Fatalf("ignorable event should pass: %v", err)
	}
	if err := session.ValidateReplayability([]session.Event{{
		Seq: 1, EventType: "runtime/new-required-state", ReplayPolicy: session.ReplayRequired,
	}}, known); err == nil {
		t.Fatal("unknown required event should stop replay")
	}
}
