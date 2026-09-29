package orchestration

import (
	"context"
	"errors"
	"strconv"
	"time"

	"gorm.io/gorm"
)

// CreateSchedule 只保存记录，不创建操作系统或 WorkBuddy 定时任务。
// 固定间隔锚定 Due；停机错过的多个时隙合并投递一次，再跳到未来时隙。
func (m *Manager) CreateSchedule(ctx context.Context, sessionID, text, key string, due time.Time, interval time.Duration) (Schedule, error) {
	ctx, done, err := m.operation(ctx)
	if err != nil {
		return Schedule{}, err
	}
	defer done()
	if !validText(text) || !validKey(key) || due.IsZero() || interval < 0 || (interval > 0 && interval < time.Minute) || interval > maxInterval {
		return Schedule{}, ErrInvalid
	}
	if _, err = m.parent(ctx, sessionID); err != nil {
		return Schedule{}, err
	}
	due = due.UTC().Truncate(time.Microsecond)
	id, hash := stableID("schedule", sessionID, key), digest([]any{text, due, interval})
	var row scheduleRecord
	err = m.withLease(ctx, func(ctx context.Context) error {
		err := m.db.WithContext(ctx).Where("id = ? AND session_id = ?", id, sessionID).First(&row).Error
		if err == nil {
			if row.PayloadHash != hash {
				return ErrConflict
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return storageError(ctx)
		}
		now := m.now()
		if !due.After(now) {
			return ErrInvalid
		}
		if err := m.admission(ctx, sessionID); err != nil {
			return err
		}
		row = scheduleRecord{Schedule: Schedule{ID: id, SessionID: sessionID, Text: text, Due: due, NextDue: due, Interval: interval, State: Pending, CreatedAt: now, UpdatedAt: now}, Version: SchemaVersion, PayloadHash: hash}
		return m.transaction(ctx, func(tx *gorm.DB) error { return tx.Create(&row).Error })
	})
	return row.Schedule, safeError(ctx, err)
}

func (m *Manager) Schedules(ctx context.Context, sessionID string) ([]Schedule, error) {
	ctx, done, err := m.operation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if _, err = m.parent(ctx, sessionID); err != nil {
		return nil, err
	}
	var rows []scheduleRecord
	if err = m.db.WithContext(ctx).Where("session_id = ?", sessionID).Order("created_at, id").Find(&rows).Error; err != nil {
		return nil, storageError(ctx)
	}
	result := make([]Schedule, 0, len(rows))
	for _, row := range rows {
		result = append(result, row.Schedule)
	}
	return result, nil
}

func (m *Manager) CancelSchedule(ctx context.Context, sessionID, id string) error {
	ctx, done, err := m.operation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if _, err = m.parent(ctx, sessionID); err != nil {
		return err
	}
	return m.withLease(ctx, func(ctx context.Context) error {
		return m.transaction(ctx, func(tx *gorm.DB) error {
			var row scheduleRecord
			err := tx.Where("id = ? AND session_id = ?", id, sessionID).First(&row).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			if row.State != Pending {
				return nil
			}
			return tx.Model(&row).Update("state", Cancelled).Error
		})
	})
}

func schedulePromptKey(row scheduleRecord) string {
	return "orchestration:" + row.ID + ":" + strconv.FormatInt(row.NextDue.UnixMicro(), 10)
}

func (m *Manager) fireSchedule(ctx context.Context, row scheduleRecord) error {
	// 在 withLease 的共享 gate 内重读，不能使用进门前缓存的投递状态。
	var current scheduleRecord
	if err := m.db.WithContext(ctx).Where("id = ? AND session_id = ?", row.ID, row.SessionID).First(&current).Error; err != nil {
		return storageError(ctx)
	}
	if current.State != Pending || current.NextDue.After(m.now()) || !current.NextDue.Equal(row.NextDue) {
		return nil
	}
	row = current
	if err := m.admission(ctx, row.SessionID); err != nil {
		if !errors.Is(err, ErrState) {
			return err
		}
		return m.transaction(ctx, func(tx *gorm.DB) error { return tx.Model(&row).Update("state", Cancelled).Error })
	}
	if err := m.transaction(ctx, func(*gorm.DB) error { return nil }); err != nil {
		return err
	}
	if _, err := m.h.Prompt(ctx, row.SessionID, row.Text, schedulePromptKey(row)); err != nil {
		return ErrExecution
	}
	// 不先推进 next_due：投递成功后丢失本次更新仍重用原 key，Inbox 会去重。
	last := row.NextDue
	row.LastDue, row.FireCount = &last, row.FireCount+1
	if row.Interval == 0 {
		row.State = Exhausted
	} else {
		now := m.now()
		missed := now.Sub(row.NextDue) / row.Interval
		if missed < 0 {
			missed = 0
		}
		row.NextDue = row.NextDue.Add((missed + 1) * row.Interval)
	}
	row.UpdatedAt = m.now()
	return m.transaction(ctx, func(tx *gorm.DB) error { return tx.Save(&row).Error })
}
