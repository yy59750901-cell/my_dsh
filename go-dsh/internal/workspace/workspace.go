package workspace

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
)

const MaxFileBytes = 256 << 10
const MaxEntries = 512

var (
	ErrUnsafePath = errors.New("unsafe workspace path")
	ErrLimit      = errors.New("workspace size limit exceeded")
)

// Workspace 是文件围栏，不是 OS sandbox。它不隔离挂载点、既有硬链接或恶意进程。
// 仅在可信管理员指定的工作目录使用；禁止将其当作运行 shell/脚本的隔离层。
// 非阻塞文件打开实现面向 Linux/macOS。
type Workspace struct {
	root      *os.Root
	closeOnce sync.Once
	closeErr  error
}

func Open(root string) (*Workspace, error) {
	if root == "" {
		return nil, errors.New("workspace root is required")
	}
	r, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	return &Workspace{root: r}, nil
}

func (w *Workspace) Close() error {
	w.closeOnce.Do(func() { w.closeErr = w.root.Close() })
	return w.closeErr
}

func sensitive(name string) bool {
	n := strings.ToLower(name)
	switch n {
	case ".git", ".workbuddy", ".ssh", ".aws", ".gnupg", ".kube", ".docker", ".hg", ".svn", ".netrc", ".npmrc", ".gitconfig", ".git-credentials":
		return true
	}
	return n == ".env" || strings.HasPrefix(n, ".env.") || strings.HasPrefix(n, ".dsh-write-")
}

func ValidatePath(name string) error {
	if !fs.ValidPath(name) || strings.ContainsAny(name, "\\:\x00") {
		return ErrUnsafePath
	}
	for _, part := range strings.Split(name, "/") {
		if sensitive(part) {
			return ErrUnsafePath
		}
	}
	return nil
}

// os.Root 会自行解析 symlink，因此不能只依赖 O_NOFOLLOW。逐级 Lstat 拒绝
// 链接，再核对打开后的 inode，阻止检查后替换为敏感目录链接；父句柄始终固定。
func (w *Workspace) openDir(name string) (*os.Root, error) {
	if err := ValidatePath(name); err != nil {
		return nil, err
	}
	current, err := w.root.OpenRoot(".")
	if err != nil {
		return nil, err
	}
	if name == "." {
		return current, nil
	}
	for _, component := range strings.Split(name, "/") {
		before, err := current.Lstat(component)
		if err != nil {
			current.Close()
			return nil, err
		}
		if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
			current.Close()
			return nil, ErrUnsafePath
		}
		next, e2 := current.OpenRoot(component)
		var after fs.FileInfo
		var e3 error
		if e2 == nil {
			after, e3 = next.Stat(".")
		}
		current.Close()
		if e2 != nil || e3 != nil || !os.SameFile(before, after) {
			if next != nil {
				next.Close()
			}
			return nil, ErrUnsafePath
		}
		current = next
	}
	return current, nil
}

func (w *Workspace) parent(name string) (*os.Root, string, error) {
	if err := ValidatePath(name); err != nil {
		return nil, "", err
	}
	if name == "." {
		return nil, "", ErrUnsafePath
	}
	r, err := w.openDir(path.Dir(name))
	return r, path.Base(name), err
}

type Entry struct {
	Name  string `json:"name"`
	IsDir bool   `json:"is_dir"`
	Size  int64  `json:"size"`
}

func (w *Workspace) List(ctx context.Context, name string) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r, err := w.openDir(name)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	f, err := r.Open(".")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	entries, err := f.ReadDir(MaxEntries + 1)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(entries) > MaxEntries {
		return nil, ErrLimit
	}
	out := make([]Entry, 0, len(entries))
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if sensitive(e.Name()) || e.Type()&os.ModeSymlink != 0 {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			continue
		}
		out = append(out, Entry{Name: e.Name(), IsDir: info.IsDir(), Size: info.Size()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (w *Workspace) ReadFile(ctx context.Context, name string, limit int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > MaxFileBytes {
		return nil, ErrLimit
	}
	r, base, err := w.parent(name)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	before, err := r.Lstat(base)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, ErrUnsafePath
	}
	f, err := r.OpenFile(base, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || !os.SameFile(before, info) {
		return nil, ErrUnsafePath
	}
	if info.Size() > int64(limit) {
		return nil, ErrLimit
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, ErrLimit
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

// WriteFile 使用同目录临时文件和原子重命名，不跟随目标 symlink，且不原地改写硬链接 inode。
func (w *Workspace) WriteFile(ctx context.Context, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(data) > MaxFileBytes {
		return ErrLimit
	}
	r, base, err := w.parent(name)
	if err != nil {
		return err
	}
	defer r.Close()
	info, err := r.Lstat(base)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil && !info.Mode().IsRegular() {
		return ErrUnsafePath
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	tmp := fmt.Sprintf(".dsh-write-%x", token)
	f, err := r.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	defer r.Remove(tmp)
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.Rename(tmp, base)
}
