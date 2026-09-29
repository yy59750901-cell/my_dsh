package orchestration

import (
	"context"
	"errors"

	"github.com/yy59750901/go-dsh/internal/agent"
	"gorm.io/gorm"
)

const batchSize = 16

func (m *Manager) pump(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	return m.withLease(ctx, func(ctx context.Context) error {
		var jobs []jobRecord
		if err := m.db.WithContext(ctx).Where("kind = ? AND id > ? AND (state IN ? OR cancel_requested = ?)", "job", m.jobCursor, []State{Pending, Running, Cancelling}, true).Order("id").Limit(batchSize).Find(&jobs).Error; err != nil {
			return storageError(ctx)
		}
		for _, row := range jobs {
			if err := m.reconcileJob(ctx, row); err != nil {
				return err
			}
			m.jobCursor = row.ID
		}
		if len(jobs) < batchSize {
			m.jobCursor = ""
		}

		var flows []workflowRecord
		if err := m.db.WithContext(ctx).Where("id > ? AND state IN ?", m.workflowCursor, []State{Running, Paused, Cancelling}).Order("id").Limit(batchSize).Find(&flows).Error; err != nil {
			return storageError(ctx)
		}
		for _, row := range flows {
			if err := m.advanceWorkflow(ctx, row); err != nil {
				return err
			}
			m.workflowCursor = row.ID
		}
		if len(flows) < batchSize {
			m.workflowCursor = ""
		}

		var schedules []scheduleRecord
		if err := m.db.WithContext(ctx).Where("state = ? AND next_due <= ?", Pending, m.now()).Order("next_due, id").Limit(batchSize).Find(&schedules).Error; err != nil {
			return storageError(ctx)
		}
		for _, row := range schedules {
			if err := m.fireSchedule(ctx, row); err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *Manager) reconcileJob(ctx context.Context, row jobRecord) error {
	// 共享 gate 保护从读取最新取消/暂停意图到 Prompt 返回的整个区间。
	var current jobRecord
	if err := m.db.WithContext(ctx).Where("id = ? AND parent_id = ? AND kind = ?", row.ID, row.ParentID, "job").First(&current).Error; err != nil {
		return storageError(ctx)
	}
	row = current
	if !row.CancelRequested {
		if err := m.ensureChild(ctx, row); err != nil {
			return err
		}
	}
	value, err := m.jobResult(ctx, row)
	if err != nil {
		return err
	}
	if row.CancelRequested {
		// 持续对账已投递输入的取消；尚未投递的输入不会越过共享 gate 内的取消屏障。
		if _, err := m.parent(ctx, row.ChildSessionID); err == nil {
			if err = m.h.Cancel(ctx, row.ChildSessionID, "orchestration-cancel"); err != nil && !errors.Is(err, agent.ErrStaleDriverActivity) {
				return ErrExecution
			}
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
	} else if value.State == Pending || (value.State == Running && value.TurnID == "") {
		if row.WorkflowID != "" && value.AcceptedSeq == 0 {
			var flow workflowRecord
			if err := m.db.WithContext(ctx).Where("id = ? AND parent_id = ?", row.WorkflowID, row.ParentID).First(&flow).Error; err != nil {
				return storageError(ctx)
			}
			if flow.State != Running {
				return nil
			}
		}
		if err := m.transaction(ctx, func(*gorm.DB) error { return nil }); err != nil {
			return err
		}
		_, promptErr := m.h.Prompt(ctx, row.ChildSessionID, row.Prompt, promptKey(row.ID))
		// 主事件提交成功而调度更新失败时，下一轮回放会找到原 input，而非重跑节点。
		if promptErr != nil {
			return ErrExecution
		}
	}
	value, err = m.jobResult(ctx, row)
	if err != nil {
		return err
	}
	value.UpdatedAt = m.now()
	row.Job = value
	return m.transaction(ctx, func(tx *gorm.DB) error { return tx.Save(&row).Error })
}
