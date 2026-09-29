package profile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func bundle(id string) Bundle { return Bundle{Profiles: []Profile{{ID: id}}} }

func TestStrictJSONAndCredentialReferences(t *testing.T) {
	for _, raw := range []string{
		``, `null`, `{}`, `{"profiles":null}`, `{"profiles":[]} {}`, `{"profiles":[}`, `{"profiles":[],"profiles":[]}`,
		`{"profiles":[],"Profiles":[]}`, `{"Profiles":[]}`, `{"profiles":[{"id":"a","id":"b"}]}`,
		`{"profiles":[{"id":"a","\u0069d":"b"}]}`, `{"profiles":[{"id":"a","ID":"b"}]}`,
		`{"profiles":[{"id":"a","credential":"SECRET"}]}`,
		`{"profiles":[{"id":"a","models":[{"id":"m","provider":"p","model":"m","api_key":"SECRET"}]}]}`,
		`{"profiles":[{"id":"a","models":[{"id":"m","provider":"p","model":"m","credential_ref":{"env":"KEY","value":"SECRET"}}]}]}`,
		`{"profiles":[{"id":"a","models":[{"id":"m","provider":"p","model":"m","credential_ref":{"env":"${KEY}"}}]}]}`,
		`{"profiles":[{"id":"a"},{"id":"a"}]}`,
		`{"profiles":[{"id":"a","models":[{"id":"m","provider":"p","model":"m"},{"id":"m","provider":"p","model":"m"}]}]}`,
		`{"profiles":[{"id":"a","models":[{"id":"m","provider":"p","model":"\ud800"}]}]}`,
		`{"profiles":[{"id":"a","models":[{"id":"m","provider":"p","model":"\udc00"}]}]}`,
		"{\"profiles\":[{\"id\":\"\xff\"}]}",
		`{"profiles":[{"id":"a","tools":[{"id":"t","endpoint":"grpcs://user:SECRET@host"}]}]}`,
		`{"profiles":[{"id":"a","models":[{"id":"m","provider":"p","model":"m","endpoint":"https://host/?token=SECRET"}]}]}`,
		`{"profiles":[{"id":"a","mcp":[{"id":"s","transport":"stdio","executable":"/bin/false"}]}]}`,
		strings.Repeat(" ", MaxJSONBytes+1),
	} {
		if _, err := ParseBundle([]byte(raw)); !errors.Is(err, ErrInvalid) || strings.Contains(err.Error(), "SECRET") {
			t.Fatalf("接受非法配置或泄密: %q: %v", raw, err)
		}
	}
	raw := []byte(`{"profiles":[{"id":"a","models":[{"id":"m","provider":"openai","model":"m","credential_ref":{"env":"TEST_KEY"}}],"mcp":[{"id":"s","transport":"stdio","executable":"/trusted/server","trusted_operator_only":true,"args":["$literal",";literal"],"env":{"KEY":{"env":"TEST_KEY"}}}]}]}`)
	b, err := ParseBundle(raw)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := b.Profiles[0].Models[0].CredentialRef.Resolve(func(key string) (string, bool) { return "SECRET", key == "TEST_KEY" })
	if err != nil || secret != "SECRET" {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(b)
	if strings.Contains(string(encoded), "SECRET") {
		t.Fatal("配置保存了解析后的凭据")
	}
	if _, err := (EnvRef{Env: "TEST_KEY"}).Resolve(func(string) (string, bool) { return "", false }); !errors.Is(err, ErrCredential) {
		t.Fatal(err)
	}
	if _, err := (EnvRef{Env: "bad name"}).Resolve(nil); !errors.Is(err, ErrCredential) {
		t.Fatal(err)
	}
}

func TestPatchStableWholeReplacement(t *testing.T) {
	b := Bundle{Profiles: []Profile{{ID: "a", Models: []Model{{ID: "m", Provider: "p", Model: "old"}}}, {ID: "b"}}}
	p, err := ParsePatch([]byte(`{"profiles":[{"id":"b","mcp":[{"id":"s","transport":"http","endpoint":"https://example.test"}]},{"id":"a"},{"id":"c"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	out, err := b.Apply(p)
	if err != nil {
		t.Fatal(err)
	}
	if out.Profiles[0].ID != "a" || len(out.Profiles[0].Models) != 0 || out.Profiles[1].ID != "b" || out.Profiles[2].ID != "c" {
		t.Fatal(out)
	}
	out.Profiles[1].MCP[0].Endpoint = "changed"
	if len(b.Profiles[0].Models) != 1 || p.Profiles[0].MCP[0].Endpoint == "changed" {
		t.Fatal("输入存在别名")
	}
	if _, err := b.Apply(Patch{Profiles: []Profile{{ID: "a"}, {ID: "a"}}}); err == nil {
		t.Fatal("重复 ID 未拒绝")
	}
}

type resource struct {
	closed atomic.Int32
	fail   bool
}

func (r *resource) Close() error {
	r.closed.Add(1)
	if r.fail {
		return errors.New("SECRET")
	}
	return nil
}

func TestManagerCommitRollbackLeaseAndImmutable(t *testing.T) {
	var resources []*resource
	m := NewManager(func(ctx context.Context, s Snapshot) (io.Closer, error) {
		r := &resource{}
		resources = append(resources, r)
		copy := s.Bundle()
		copy.Profiles[0].ID = "mutated"
		if s.Bundle().Profiles[0].ID == "reject" {
			return r, errors.New("SECRET")
		}
		return r, nil
	})
	defer m.Close()
	first, err := m.Reload(context.Background(), []byte(`{"profiles":[{"id":"a"}]}`))
	if err != nil || first.Version() != 1 {
		t.Fatal(first, err)
	}
	lease, _ := m.Acquire()
	copy := first.Bundle()
	copy.Profiles[0].ID = "mutated"
	if first.Bundle().Profiles[0].ID != "a" {
		t.Fatal("快照可变")
	}
	if _, err := m.ValidateCandidate(context.Background(), bundle("reject")); !errors.Is(err, ErrValidation) || strings.Contains(err.Error(), "SECRET") {
		t.Fatal(err)
	}
	if resources[1].closed.Load() != 1 {
		t.Fatal("失败候选未回滚")
	}
	c, _ := m.ValidateCandidate(context.Background(), bundle("b"))
	competing, _ := m.ValidateCandidate(context.Background(), bundle("c"))
	if _, err := m.Commit(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Commit(context.Background(), competing); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if resources[3].closed.Load() != 1 || resources[0].closed.Load() != 0 {
		t.Fatal("回滚或 lease 释放错误")
	}
	held, _ := lease.Snapshot()
	if held.Version() != 1 || held.Bundle().Profiles[0].ID != "a" {
		t.Fatal(held)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	_ = lease.Close()
	if resources[0].closed.Load() != 1 {
		t.Fatal("未恰好关闭一次")
	}
	if _, err := lease.Resource(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	pending, _ := m.ValidateCandidate(context.Background(), bundle("d"))
	current, _ := m.Acquire()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	_ = pending.Close()
	if resources[4].closed.Load() != 1 || resources[2].closed.Load() != 0 {
		t.Fatal("Close 生命周期错误")
	}
	_ = current.Close()
	if resources[2].closed.Load() != 1 {
		t.Fatal("最后 lease 未关闭")
	}
	if _, err := m.Acquire(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestManagerCancellationCloseDuringValidationAndCleanupErrors(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	r := &resource{fail: true}
	m := NewManager(func(context.Context, Snapshot) (io.Closer, error) { close(started); <-release; return r, nil })
	done := make(chan error, 1)
	go func() { _, err := m.ValidateCandidate(context.Background(), bundle("a")); done <- err }()
	<-started
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if r.closed.Load() != 1 || !errors.Is(m.CleanupError(), ErrCleanup) {
		t.Fatal("未清理或泄露了关闭错误")
	}
	m = NewManager(nil)
	c, _ := m.ValidateCandidate(context.Background(), bundle("a"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Commit(ctx, c); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	l, _ := m.Acquire()
	s, _ := l.Snapshot()
	_ = l.Close()
	if s.Version() != 0 {
		t.Fatal("取消后仍然发布")
	}
	_ = m.Close()
}

func TestManagerConcurrentLeasesCandidatesAndClose(t *testing.T) {
	var mu sync.Mutex
	var all []*resource
	m := NewManager(func(context.Context, Snapshot) (io.Closer, error) {
		r := &resource{}
		mu.Lock()
		all = append(all, r)
		mu.Unlock()
		return r, nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 12; j++ {
				c, err := m.ValidateCandidate(context.Background(), bundle(fmt.Sprintf("p%d", i)))
				if err == nil {
					if j%3 == 0 {
						_ = c.Close()
					} else {
						_, _ = m.Commit(context.Background(), c)
					}
				}
				l, err := m.Acquire()
				if err == nil {
					s, err := l.Snapshot()
					if err != nil {
						t.Error(err)
					}
					b := s.Bundle()
					if len(b.Profiles) > 0 {
						b.Profiles[0].ID = "copy"
					}
					_, _ = l.Resource()
					_ = l.Close()
				}
			}
		}(i)
	}
	wg.Wait()
	var closer sync.WaitGroup
	for range 8 {
		closer.Add(1)
		go func() { defer closer.Done(); _ = m.Close() }()
	}
	closer.Wait()
	for _, r := range all {
		if r.closed.Load() != 1 {
			t.Fatal("资源未恰好释放一次", r.closed.Load())
		}
	}
}
