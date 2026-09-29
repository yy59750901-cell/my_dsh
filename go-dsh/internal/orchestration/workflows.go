package orchestration

import (
	"context"
	"errors"
	"strconv"

	"gorm.io/gorm"
)

func (m *Manager) CreateWorkflow(ctx context.Context, parentID string, steps []string, key string) (Workflow, error) {
	ctx, done, err := m.operation(ctx)
	if err != nil {
		return Workflow{}, err
	}
	defer done()
	if !validKey(key) || len(steps) == 0 || len(steps) > 128 {
		return Workflow{}, ErrInvalid
	}
	total := 0
	for _, step := range steps {
		if !validText(step) {
			return Workflow{}, ErrInvalid
		}
		total += len(step)
	}
	if total > 1024*1024 {
		return Workflow{}, ErrInvalid
	}
	if _, err = m.parent(ctx, parentID); err != nil {
		return Workflow{}, err
	}
	id, hash := stableID("workflow", parentID, key), digest(steps)
	var row workflowRecord
	err = m.withLease(ctx, func(ctx context.Context) error {
		err := m.db.WithContext(ctx).Where("id = ? AND parent_id = ?", id, parentID).First(&row).Error
		if err == nil {
			if row.PayloadHash != hash {
				return ErrConflict
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return storageError(ctx)
		}
		if err := m.admission(ctx, parentID); err != nil {
			return err
		}
		now := m.now()
		row = workflowRecord{Workflow: Workflow{ID: id, ParentID: parentID, Steps: append([]string(nil), steps...), State: Running, Jobs: []Job{}, CreatedAt: now, UpdatedAt: now}, Version: SchemaVersion, PayloadHash: hash}
		return m.transaction(ctx, func(tx *gorm.DB) error { return tx.Create(&row).Error })
	})
	return row.Workflow, safeError(ctx, err)
}

func (m *Manager) ownedWorkflow(ctx context.Context, parentID, id string) (workflowRecord, error) {
	var row workflowRecord
	if _, err := m.parent(ctx, parentID); err != nil {
		return row, err
	}
	err := m.db.WithContext(ctx).Where("id = ? AND parent_id = ?", id, parentID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return row, ErrNotFound
	}
	return row, safeError(ctx, err)
}

func (m *Manager) GetWorkflow(ctx context.Context, parentID, id string) (Workflow, error) {
	ctx, done, err := m.operation(ctx)
	if err != nil {
		return Workflow{}, err
	}
	defer done()
	row, err := m.ownedWorkflow(ctx, parentID, id)
	if err != nil {
		return Workflow{}, err
	}
	return m.workflowResult(ctx, row)
}

// PauseWorkflow 关闭尚未投递的节点准入；已投递的当前节点可以继续执行与收尾。
func (m *Manager) PauseWorkflow(ctx context.Context, parentID, id string) error {
	return m.workflowControl(ctx, parentID, id, Paused)
}

// ResumeWorkflow 仅恢复 paused；failed/cancelled 不可重放，需创建新工作流。
func (m *Manager) ResumeWorkflow(ctx context.Context, parentID, id string) error {
	return m.workflowControl(ctx, parentID, id, Running)
}

func (m *Manager) CancelWorkflow(ctx context.Context, parentID, id string) error {
	return m.workflowControl(ctx, parentID, id, Cancelling)
}

func (m *Manager) workflowControl(ctx context.Context, parentID, id string, target State) error {
	ctx, done, err := m.operation(ctx)
	if err != nil {
		return err
	}
	defer done()
	return m.withLease(ctx, func(ctx context.Context) error {
		row, err := m.ownedWorkflow(ctx, parentID, id)
		if err != nil {
			return err
		}
		if target != Cancelling {
			value, err := m.workflowResult(ctx, row)
			if err != nil {
				return err
			}
			if terminal(value.State) {
				return ErrState
			}
			if row.State == target {
				return nil
			}
			if row.State != Paused && row.State != Running {
				return ErrState
			}
			if err = m.admission(ctx, parentID); err != nil {
				return err
			}
		}
		return m.transaction(ctx, func(tx *gorm.DB) error {
			if target == Cancelling {
				var jobs []jobRecord
				if err := tx.Where("workflow_id = ? AND parent_id = ? AND kind = ?", id, parentID, "job").Find(&jobs).Error; err != nil {
					return err
				}
				for _, job := range jobs {
					if err := requestCancel(tx, job); err != nil {
						return err
					}
					if err := m.cancelTree(tx, job.ChildSessionID); err != nil {
						return err
					}
				}
				if terminal(row.State) {
					return nil
				}
			}
			return tx.Model(&row).Update("state", target).Error
		})
	})
}

func (m *Manager) workflowResult(ctx context.Context, row workflowRecord) (Workflow, error) {
	result := row.Workflow
	result.Jobs = []Job{}
	var jobs []jobRecord
	if err := m.db.WithContext(ctx).Where("workflow_id = ? AND parent_id = ? AND kind = ?", row.ID, row.ParentID, "job").Order("step_index").Find(&jobs).Error; err != nil {
		return Workflow{}, storageError(ctx)
	}
	allTerminal := true
	for _, job := range jobs {
		value, err := m.jobResult(ctx, job)
		if err != nil {
			return Workflow{}, err
		}
		result.Jobs = append(result.Jobs, value)
		if !terminal(value.State) {
			allTerminal = false
		}
	}
	if row.State == Cancelling || row.State == Cancelled {
		result.State = Cancelling
		if allTerminal {
			result.State = Cancelled
		}
		return result, nil
	}
	result.CurrentStep = 0
	for index := range row.Steps {
		if index >= len(result.Jobs) {
			break
		}
		job := result.Jobs[index]
		if job.StepIndex != index || job.Prompt != row.Steps[index] {
			return Workflow{}, ErrConflict
		}
		if job.State == Failed || job.State == Cancelled {
			result.State = Failed
			return result, nil
		}
		if job.State != Succeeded {
			break
		}
		result.CurrentStep++
	}
	if result.CurrentStep == len(row.Steps) {
		result.State = Succeeded
	} else if result.State == Succeeded {
		result.State = Running
	}
	return result, nil
}

func (m *Manager) advanceWorkflow(ctx context.Context, row workflowRecord) error {
	value, err := m.workflowResult(ctx, row)
	if err != nil {
		return err
	}
	row.Workflow = value
	if value.State == Running && value.CurrentStep < len(value.Steps) {
		if err := m.admission(ctx, row.ParentID); err != nil {
			return err
		}
		index := value.CurrentStep
		_, err = m.createJob(ctx, stableID("workflow-job", row.ID, strconv.Itoa(index)), row.ParentID, row.Steps[index], row.ID, index)
		if err != nil {
			return err
		}
	}
	row.UpdatedAt = m.now()
	return m.transaction(ctx, func(tx *gorm.DB) error { return tx.Save(&row).Error })
}
