// Package orchestration 提供本地 single-user 的持久编排，不提供多租户或工具沙箱。
// database 必须与 Harness 的 GORM SessionStore 指向同一数据库；多个 Manager 必须在同进程共用同一个 Harness。
// 投递与控制的线性化仅覆盖这些 Manager API；直接调用 Harness 不经过共享 gate。
// 不支持跨进程或不同 Harness 并行编排同一数据库：持久租约不能原子约束外部 Prompt。
// 调度表保存定义、控制意图和可重算的执行投影，Session 事件仍是执行结果的唯一权威。
package orchestration

import (
	"errors"
	"time"
)

const SchemaVersion = 1

var (
	ErrInvalid   = errors.New("orchestration: invalid request")
	ErrNotFound  = errors.New("orchestration: resource not found")
	ErrConflict  = errors.New("orchestration: idempotency conflict")
	ErrState     = errors.New("orchestration: invalid state")
	ErrBusy      = errors.New("orchestration: coordinator busy; retry")
	ErrClosed    = errors.New("orchestration: manager closed")
	ErrStorage   = errors.New("orchestration: storage unavailable")
	ErrExecution = errors.New("orchestration: session operation unavailable")
	ErrVersion   = errors.New("orchestration: unsupported schema version")
)

type State string

const (
	Pending    State = "pending"
	Running    State = "running"
	Succeeded  State = "succeeded"
	Failed     State = "failed"
	Paused     State = "paused"
	Cancelling State = "cancelling"
	Cancelled  State = "cancelled"
	// Exhausted 只表示一次性计划已投递，不表示模型执行成功。
	Exhausted State = "exhausted"
)

type Job struct {
	ID              string `json:"id" gorm:"primaryKey"`
	ParentID        string `json:"parent_id" gorm:"index"`
	ChildSessionID  string `json:"child_session_id" gorm:"index"`
	WorkflowID      string `json:"workflow_id,omitempty" gorm:"index"`
	StepIndex       int    `json:"step_index"`
	Prompt          string `json:"prompt"`
	State           State  `json:"state" gorm:"index"`
	CancelRequested bool   `json:"cancel_requested"`
	AcceptedSeq     uint64 `json:"accepted_seq"`
	TurnID          string `json:"turn_id,omitempty"`
	EndSeq          uint64 `json:"end_seq"`
	// Outcome 仅使用固定类别，不含模型、SQL 或工具错误原文。
	Outcome   string    `json:"outcome,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type jobRecord struct {
	Job         `gorm:"embedded"`
	Version     int
	Kind        string `gorm:"index"`
	PayloadHash string
	LeaseOwner  string
	LeaseUntil  time.Time
}

func (jobRecord) TableName() string { return "dsh_jobs" }

type Schedule struct {
	ID        string     `json:"id" gorm:"primaryKey"`
	SessionID string     `json:"session_id" gorm:"index"`
	Text      string     `json:"text"`
	Due       time.Time  `json:"due"`
	NextDue   time.Time  `json:"next_due" gorm:"index"`
	LastDue   *time.Time `json:"last_due,omitempty"`
	// Interval 以纳秒序列化；0 为一次性，非零范围为一分钟至一年。
	Interval  time.Duration `json:"interval_ns"`
	State     State         `json:"state" gorm:"index"`
	FireCount uint64        `json:"fire_count"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
}

type scheduleRecord struct {
	Schedule    `gorm:"embedded"`
	Version     int
	PayloadHash string
}

func (scheduleRecord) TableName() string { return "dsh_schedules" }

type Workflow struct {
	ID          string    `json:"id" gorm:"primaryKey"`
	ParentID    string    `json:"parent_id" gorm:"index"`
	Steps       []string  `json:"steps" gorm:"serializer:json"`
	State       State     `json:"state" gorm:"index"`
	CurrentStep int       `json:"current_step"`
	Jobs        []Job     `json:"jobs" gorm:"-"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type workflowRecord struct {
	Workflow    `gorm:"embedded"`
	Version     int
	PayloadHash string
}

func (workflowRecord) TableName() string { return "dsh_workflows" }

// Clock 仅控制编排时钟；不改系统、Harness 或用户的定时配置。
type Clock interface {
	Now() time.Time
	NewTicker(time.Duration) Ticker
}
type Ticker interface {
	C() <-chan time.Time
	Stop()
}
type Options struct {
	Clock        Clock         `json:"-"`
	PollInterval time.Duration `json:"poll_interval_ns"`
}
type wallClock struct{}

func (wallClock) Now() time.Time                   { return time.Now() }
func (wallClock) NewTicker(d time.Duration) Ticker { return wallTicker{time.NewTicker(d)} }

type wallTicker struct{ *time.Ticker }

func (t wallTicker) C() <-chan time.Time { return t.Ticker.C }

func terminal(s State) bool { return s == Succeeded || s == Failed || s == Cancelled }
