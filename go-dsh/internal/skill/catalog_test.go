package skill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yy59750901/go-dsh/internal/tool"
	"github.com/yy59750901/go-dsh/internal/workspace"
)

func skillFile(t *testing.T, root, dir, content string) string {
	t.Helper()
	folder := filepath.Join(root, "skills", dir)
	if err := os.MkdirAll(folder, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(folder, "SKILL.md")
	if err := os.WriteFile(file, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return file
}
func document(name, body string) string {
	return "---\nname: " + name + "\ndescription: 用于本地测试\n---\n" + body
}

func TestSkillProgressiveDisclosureAndNoPrivateDiscovery(t *testing.T) {
	root := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)
	skillFile(t, home, "private", document("private", "private body"))
	skillFile(t, root, "local", document("local", "BODY_ONLY_ON_READ"))
	if err := os.MkdirAll(filepath.Join(root, "skills", "nested", "deep"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "skills", "nested", "deep", "SKILL.md"), []byte(document("nested", "no recursive discovery")), 0600); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(root, "executed")
	if err := os.WriteFile(filepath.Join(root, "skills", "local", "script.sh"), []byte("#!/bin/sh\ntouch "+marker+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	c, err := NewCatalog(root, "skills")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := c.List(); len(got) != 1 || got[0].Name != "local" || strings.Contains(got[0].Description, "BODY_") {
		t.Fatal(got)
	}
	list := c.List()
	list[0].Description = "mutated"
	if c.List()[0].Description == "mutated" {
		t.Fatal("mutable catalog metadata")
	}
	r := tool.NewRegistry()
	for _, f := range c.Tools() {
		if f.Definition().RequiresApproval {
			t.Fatal("read-only tool requires approval")
		}
		if err := r.Register(f); err != nil {
			t.Fatal(err)
		}
	}
	result, err := r.Execute(context.Background(), tool.Call{Name: "list_skills", Arguments: json.RawMessage(`{}`)})
	if err != nil || strings.Contains(result.Content, "BODY_") || strings.Contains(result.Content, "private") {
		t.Fatal(result, err)
	}
	result, err = r.Execute(context.Background(), tool.Call{Name: "read_skill", Arguments: json.RawMessage(`{"name":"local"}`)})
	if err != nil || result.Content != "BODY_ONLY_ON_READ" {
		t.Fatal(result, err)
	}
	if _, err := c.Read(context.Background(), "../private"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("skill script executed")
	}
}

func TestSkillParserScalarAndBlockSubset(t *testing.T) {
	cases := []struct{ header, description string }{
		{"name: local\ndescription: simple", "simple"},
		{"description: \"quoted: text\"\nname: local", "quoted: text"},
		{"name: local\ndescription: 'it''s local'", "it's local"},
		{"name: local\ndescription: >\n  first line\n  second line", "first line second line"},
		{"name: local\ndescription: |-\n  first line\n  second line", "first line\nsecond line"},
	}
	for _, tc := range cases {
		d, body, err := parse([]byte("---\n" + tc.header + "\n---\nbody"))
		if err != nil || d.Name != "local" || d.Description != tc.description || body != "body" {
			t.Fatal(d, body, err)
		}
	}
	d, body, err := parse([]byte("---\r\nname: local\r\ndescription: test\r\n---\r\nbody"))
	if err != nil || d.Name != "local" || body != "body" {
		t.Fatal(d, body, err)
	}
}

func TestSkillRejectsUnsafeMalformedOrUnboundedFrontmatter(t *testing.T) {
	bad := []string{
		"no frontmatter", "---\nname: local\ndescription: test", "---\nname: local\n---\nbody",
		"---\nname: local\nname: twice\ndescription: test\n---\n",
		"---\nname: local\ndescription: !!python/object:danger\n---\n",
		"---\nname: local\ndescription: &anchor text\n---\n",
		"---\nname: local\ndescription: *anchor\n---\n",
		"---\nname: local\ndescription: [one, two]\n---\n",
		"---\nname: local\ndescription: text\nmetadata:\n  run: bad\n---\n",
		"---\nname: ../private\ndescription: test\n---\n",
		"---\nname: local\n\tdescription: test\n---\n",
		"---\nname: local\ndescription: 'broken'quote'\n---\n",
		"---\nname: local\ndescription: \"\\u0000\"\n---\n",
		"---\nname: local\ndescription: >\n---\n",
		document("local", strings.Repeat("x", MaxDocumentBytes)),
		"---\nname: local\ndescription: " + strings.Repeat("x", 4097) + "\n---\n",
		"---\nname: local\n" + strings.Repeat("# comment\n", 129) + "description: test\n---\n",
	}
	for i, data := range bad {
		if _, _, err := parse([]byte(data)); !errors.Is(err, ErrFrontmatter) {
			t.Errorf("case %d accepted: %v", i, err)
		}
	}
	if _, _, err := parse([]byte{0xff}); err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}

func TestSkillDiscoveryFenceAndPinnedContent(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	file := skillFile(t, root, "local", document("local", "original"))
	c, err := NewCatalog(root, "skills")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := os.WriteFile(file, []byte(document("local", "changed")), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(context.Background(), "local"); !errors.Is(err, ErrChanged) {
		t.Fatal(err)
	}
	outsideFile := skillFile(t, outside, "private", document("private", "private body"))
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsideFile, file); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(context.Background(), "local"); err == nil {
		t.Fatal("followed outside skill symlink")
	}
	if unsafe, err := NewCatalog(root, "skills"); err == nil {
		unsafe.Close()
		t.Fatal("discovered symlink skill")
	}
	for _, name := range []string{"", outside + "/skills", "../skills", ".workbuddy/skills", ".git/skills", "~/skills", "$HOME/skills", "other"} {
		if unsafe, err := NewCatalog(root, name); err == nil {
			unsafe.Close()
			t.Errorf("accepted path %q", name)
		}
	}
	root2 := t.TempDir()
	if err := os.Symlink(filepath.Join(outside, "skills"), filepath.Join(root2, "skills")); err != nil {
		t.Fatal(err)
	}
	if unsafe, err := NewCatalog(root2, "skills"); err == nil {
		unsafe.Close()
		t.Fatal("followed skills directory link")
	}
}

func TestSkillDuplicateLimitAndCancellation(t *testing.T) {
	root := t.TempDir()
	skillFile(t, root, "one", document("same", "one"))
	skillFile(t, root, "two", document("same", "two"))
	if c, err := NewCatalog(root, "skills"); err == nil {
		c.Close()
		t.Fatal("duplicate names accepted")
	}
	large := t.TempDir()
	for i := 0; i <= MaxSkills; i++ {
		name := fmt.Sprintf("skill-%d", i)
		skillFile(t, large, name, document(name, "body"))
	}
	if c, err := NewCatalog(large, "skills"); !errors.Is(err, workspace.ErrLimit) {
		if c != nil {
			c.Close()
		}
		t.Fatal(err)
	}
	root = t.TempDir()
	skillFile(t, root, "local", document("local", "body"))
	c, err := NewCatalog(root, "skills")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Read(ctx, "local"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(context.Background(), "local"); err == nil {
		t.Fatal("read after close")
	}
}
