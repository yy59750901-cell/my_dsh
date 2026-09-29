package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/yy59750901/go-dsh/internal/tool"
)

func setup(t *testing.T) (*Workspace, string) {
	t.Helper()
	root := t.TempDir()
	w, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w, root
}
func put(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(name, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceToolsRoundTripAndApproval(t *testing.T) {
	root := t.TempDir()
	tools, err := NewWorkspaceTools(root)
	if err != nil {
		t.Fatal(err)
	}
	defer tools[0].(io.Closer).Close()
	r := tool.NewRegistry()
	for _, f := range tools {
		d := f.Definition()
		if d.RequiresApproval != (d.Name == "write_file") {
			t.Fatal(d)
		}
		if err := r.Register(f); err != nil {
			t.Fatal(err)
		}
	}
	call := func(name, args string) tool.Result {
		t.Helper()
		result, err := r.Execute(context.Background(), tool.Call{Name: name, Arguments: json.RawMessage(args)})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	call("write_file", `{"path":"hello.txt","content":"你好"}`)
	if result := call("read_file", `{"path":"hello.txt"}`); result.Content != "你好" {
		t.Fatal(result)
	}
	result := call("list_files", `{}`)
	var entries []Entry
	if err := json.Unmarshal(result.Structured, &entries); err != nil || len(entries) != 1 || entries[0].Name != "hello.txt" {
		t.Fatal(result, err)
	}
	if _, err := r.Execute(context.Background(), tool.Call{Name: "write_file", Arguments: json.RawMessage(`{"path":"x"}`)}); !errors.Is(err, tool.ErrInvalidArguments) {
		t.Fatal(err)
	}
	put(t, filepath.Join(root, "binary"), string([]byte{0xff}))
	if _, err := r.Execute(context.Background(), tool.Call{Name: "read_file", Arguments: json.RawMessage(`{"path":"binary"}`)}); err == nil {
		t.Fatal("binary accepted")
	}
}

func TestWorkspaceRejectsTraversalSensitiveAndSymlinks(t *testing.T) {
	w, root := setup(t)
	outside := t.TempDir()
	put(t, filepath.Join(outside, "secret"), "secret")
	for _, name := range []string{".git", ".workbuddy", ".ssh"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
		put(t, filepath.Join(root, name, "secret"), "secret")
	}
	put(t, filepath.Join(root, ".env"), "secret")
	put(t, filepath.Join(root, "safe"), "safe")
	for name, target := range map[string]string{"escape": outside, "alias": ".git", "link": "safe", "fileescape": filepath.Join(outside, "secret")} {
		if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"/etc/passwd", "../secret", "x/../../secret", "x/../safe", "./safe", "C:\\secret", ".git/secret", ".workbuddy/secret", ".ssh/secret", ".env", ".ENV", ".env.local", "escape/secret", "alias/secret", "link", "fileescape", "safe\x00"} {
		if _, err := w.ReadFile(context.Background(), name, MaxFileBytes); err == nil {
			t.Errorf("read accepted %q", name)
		}
		if err := w.WriteFile(context.Background(), name, []byte("overwrite")); err == nil {
			t.Errorf("write accepted %q", name)
		}
	}
	for _, name := range []string{"escape", "alias", ".git", "../"} {
		if _, err := w.List(context.Background(), name); err == nil {
			t.Errorf("list accepted %q", name)
		}
	}
	entries, err := w.List(context.Background(), ".")
	if err != nil || len(entries) != 1 || entries[0].Name != "safe" {
		t.Fatal(entries, err)
	}
	data, err := os.ReadFile(filepath.Join(outside, "secret"))
	if err != nil || string(data) != "secret" {
		t.Fatal("outside changed", err)
	}
}

func TestWorkspaceBoundsSpecialFilesCancellationAndClose(t *testing.T) {
	w, root := setup(t)
	put(t, filepath.Join(root, "big"), strings.Repeat("x", MaxFileBytes+1))
	if _, err := w.ReadFile(context.Background(), "big", MaxFileBytes); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err := w.WriteFile(context.Background(), "big", make([]byte, MaxFileBytes+1)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := w.ReadFile(context.Background(), "big", 0); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(root, "pipe"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ReadFile(context.Background(), "pipe", MaxFileBytes); err == nil {
		t.Fatal("read FIFO accepted")
	}
	if err := w.WriteFile(context.Background(), "pipe", []byte("x")); err == nil {
		t.Fatal("write FIFO accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.WriteFile(ctx, "canceled", []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := w.ReadFile(ctx, "big", MaxFileBytes); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := w.List(ctx, "."); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.List(context.Background(), "."); err == nil {
		t.Fatal("closed workspace usable")
	}
}

func TestWorkspacePinnedRootAtomicWriteAndDirectoryLimit(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "workspace")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	w, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	moved := filepath.Join(base, "moved")
	if err := os.Rename(root, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteFile(context.Background(), "pinned", []byte("yes")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(moved, "pinned")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "pinned")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("root was not pinned")
	}
	outside := filepath.Join(base, "hardlink-source")
	put(t, outside, "old")
	if err := os.Link(outside, filepath.Join(moved, "hardlink")); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteFile(context.Background(), "hardlink", []byte("new")); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "old" {
		t.Fatal("write modified external hardlink inode", err)
	}
	for i := 0; i < MaxEntries; i++ {
		put(t, filepath.Join(moved, fmt.Sprintf("f%d", i)), "")
	}
	if _, err := w.List(context.Background(), "."); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}

func TestWorkspaceSymlinkSwapCannotReadOrWriteSensitiveDirectory(t *testing.T) {
	w, root := setup(t)
	if err := os.Mkdir(filepath.Join(root, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(root, ".git", "secret"), "private")
	if err := os.Mkdir(filepath.Join(root, "safe"), 0700); err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(root, "safe", "secret"), "public")
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			_ = os.Rename(filepath.Join(root, "safe"), filepath.Join(root, "parked"))
			_ = os.Symlink(".git", filepath.Join(root, "safe"))
			_ = os.Remove(filepath.Join(root, "safe"))
			_ = os.Rename(filepath.Join(root, "parked"), filepath.Join(root, "safe"))
		}
	}()
	defer func() { close(done); wg.Wait() }()
	for range 200 {
		data, err := w.ReadFile(context.Background(), "safe/secret", MaxFileBytes)
		if err == nil && string(data) == "private" {
			t.Fatal("sensitive symlink race escaped")
		}
		_ = w.WriteFile(context.Background(), "safe/new", []byte("x"))
	}
	if _, err := os.Stat(filepath.Join(root, ".git", "new")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("write reached sensitive directory", err)
	}
}
