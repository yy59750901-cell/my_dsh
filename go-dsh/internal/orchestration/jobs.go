package orchestration

import (
	"context"
	"errors"

	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"github.com/yy59750901/go-dsh/internal/session"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (m *Manager) CreateChild(ctx context.Context, parentID, prompt, idempotencyKey string) (Job, error) {
	ctx, done, err := m.operation(ctx)
	if err != nil {
		return Job{}, err
	}
	defer done()
	if !validText(prompt) || !validKey(idempotencyKey) {
		return Job{}, ErrInvalid
	}
	if _, err = m.parent(ctx, parentID); err != nil {
		return Job{}, err
	}
	var record jobRecord
	err = m.withLease(ctx, func(ctx context.Context) error {
		var err error
		record, err = m.createJob(ctx, stableID("job", parentID, idempotencyKey), parentID, prompt, "", 0)
		if err != nil {
			return err
		}
		return m.ensureChild(ctx, record)
	})
	return record.Job, safeError(ctx, err)
}

func (m *Manager) createJob(ctx context.Context, id, parentID, prompt, workflowID string, step int) (jobRecord, error) {
	var row jobRecord
	hash := digest([]any{parentID, prompt, workflowID, step})
	err := m.db.WithContext(ctx).Where("id = ? AND kind = ?", id, "job").First(&row).Error
	if err == nil {
		if row.PayloadHash != hash {
			return row, ErrConflict
		}
		return row, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return row, storageError(ctx)
	}
	if err = m.admission(ctx, parentID); err != nil {
		return row, err
	}
	now := m.now()
	row = jobRecord{Job: Job{ID: id, ParentID: parentID, Prompt: prompt, ChildSessionID: stableID("child", id), WorkflowID: workflowID, StepIndex: step, State: Pending, CreatedAt: now, UpdatedAt: now}, Version: SchemaVersion, Kind: "job", PayloadHash: hash}
	err = m.transaction(ctx, func(tx *gorm.DB) error { return tx.Create(&row).Error })
	return row, err
}

// 固定 child ID 使“建会话后、更新 job 前”崩溃可恢复；fork_seq=0 不继承父历史。
// 已存在的 ID 必须精确匹配来源，绝不把其他会话冒认为重试成功。
func (m *Manager) ensureChild(ctx context.Context, row jobRecord) error {
	check := func() error {
		var child gormrepo.SessionModel
		err := m.db.WithContext(ctx).Where("id = ?", row.ChildSessionID).First(&child).Error
		if err != nil {
			return err
		}
		if child.ParentID == nil || *child.ParentID != row.ParentID || child.ForkSeq == nil || *child.ForkSeq != 0 || child.TenantID != "local" || child.WorkspaceID != "default" || child.ArchivedAt != nil {
			return ErrConflict
		}
		return nil
	}
	err := check()
	if err == nil || errors.Is(err, ErrConflict) {
		return err
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return storageError(ctx)
	}
	zero := uint64(0)
	// 先取得调度写锁，再由原 SessionStore 创建；避免 SQLite 读事务升级
	// 与另一 Manager 的 claim UPDATE 竞争导致 SQLITE_BUSY_SNAPSHOT。
	err = m.transaction(ctx, func(tx *gorm.DB) error {
		return gormrepo.NewEventStore(tx).Create(ctx, session.NewSession{ID: row.ChildSessionID, ParentID: row.ParentID, ForkSeq: &zero, TenantID: "local", WorkspaceID: "default"})
	})
	if verified := check(); verified == nil {
		return nil
	} else if errors.Is(verified, ErrConflict) {
		return ErrConflict
	}
	if err != nil {
		return safeError(ctx, err)
	}
	return storageError(ctx)
}

func (m *Manager) Jobs(ctx context.Context, parentID string) ([]Job, error) {
	ctx, done, err := m.operation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if _, err = m.parent(ctx, parentID); err != nil {
		return nil, err
	}
	var rows []jobRecord
	if err = m.db.WithContext(ctx).Where("parent_id = ? AND kind = ?", parentID, "job").Order("created_at, id").Find(&rows).Error; err != nil {
		return nil, storageError(ctx)
	}
	result := make([]Job, 0, len(rows))
	for _, row := range rows {
		job, err := m.jobResult(ctx, row)
		if err != nil {
			return nil, err
		}
		result = append(result, job)
	}
	return result, nil
}

func (m *Manager) GetJob(ctx context.Context, parentID, id string) (Job, error) {
	ctx, done, err := m.operation(ctx)
	if err != nil {
		return Job{}, err
	}
	defer done()
	row, err := m.ownedJob(ctx, parentID, id)
	if err != nil {
		return Job{}, err
	}
	return m.jobResult(ctx, row)
}

func (m *Manager) ownedJob(ctx context.Context, parentID, id string) (jobRecord, error) {
	var row jobRecord
	if _, err := m.parent(ctx, parentID); err != nil {
		return row, err
	}
	err := m.db.WithContext(ctx).Where("id = ? AND parent_id = ? AND kind = ?", id, parentID, "job").First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return row, ErrNotFound
	}
	return row, safeError(ctx, err)
}

// CancelJob 持久化取消意图并递归传播至所有后代、后代计划及工作流。
// 返回成功表示意图已提交；Run 会调用 Harness.Cancel，直到事件回放确认收敛。
// 即使 job 已成功，其后代仍会被取消，并禁止此 child 下新增编排。
func (m *Manager) CancelJob(ctx context.Context, parentID, id string) error {
	ctx, done, err := m.operation(ctx)
	if err != nil {
		return err
	}
	defer done()
	return m.withLease(ctx, func(ctx context.Context) error {
		row, err := m.ownedJob(ctx, parentID, id)
		if err != nil {
			return err
		}
		return m.transaction(ctx, func(tx *gorm.DB) error {
			if err := requestCancel(tx, row); err != nil {
				return err
			}
			return m.cancelTree(tx, row.ChildSessionID)
		})
	})
}

// CancelChildren 是显式父取消入口：永久关闭 parentID 下的编排准入并传播取消。
// 它不取消父会话本身；宿主取消父会话时应先调用本方法，再调用 Harness.Cancel。
// 普通 turn/end 不等于父会话取消，不触发传播。
func (m *Manager) CancelChildren(ctx context.Context, parentID string) error {
	ctx, done, err := m.operation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if _, err = m.parent(ctx, parentID); err != nil {
		return err
	}
	return m.withLease(ctx, func(ctx context.Context) error {
		return m.transaction(ctx, func(tx *gorm.DB) error { return m.cancelTree(tx, parentID) })
	})
}

func requestCancel(tx *gorm.DB, row jobRecord) error {
	state := Cancelling
	if terminal(row.State) {
		state = row.State
	}
	return tx.Model(&jobRecord{}).Where("id = ? AND kind = ?", row.ID, "job").Updates(map[string]any{"cancel_requested": true, "state": state}).Error
}

func (m *Manager) cancelTree(tx *gorm.DB, parentID string) error {
	queue := []string{parentID}
	seen := map[string]bool{}
	for len(queue) > 0 {
		parentID, queue = queue[0], queue[1:]
		if seen[parentID] {
			continue
		}
		seen[parentID] = true
		barrier := jobRecord{Job: Job{ID: stableID("cancel-parent", parentID), ParentID: parentID, State: Cancelled, CancelRequested: true}, Kind: "parent_cancel", Version: SchemaVersion}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&barrier).Error; err != nil {
			return err
		}
		var children []jobRecord
		if err := tx.Where("parent_id = ? AND kind = ?", parentID, "job").Find(&children).Error; err != nil {
			return err
		}
		for _, child := range children {
			if err := requestCancel(tx, child); err != nil {
				return err
			}
			queue = append(queue, child.ChildSessionID)
		}
		if err := tx.Model(&scheduleRecord{}).Where("session_id = ? AND state = ?", parentID, Pending).Update("state", Cancelled).Error; err != nil {
			return err
		}
		if err := tx.Model(&workflowRecord{}).Where("parent_id = ? AND state IN ?", parentID, []State{Running, Paused, Cancelling}).Update("state", Cancelling).Error; err != nil {
			return err
		}
	}
	return nil
}
