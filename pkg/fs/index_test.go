package fs_test

import (
	"encoding/json"
	"github.com/ozkatz/cloudzip/pkg/fs"
	"os"
	"sort"
	"testing"
	"time"
)

func TestDirParts(t *testing.T) {
	cases := []struct {
		Name     string
		Path     string
		Expected []string
	}{
		{
			Name:     "nothing",
			Path:     "",
			Expected: []string{""},
		},
		{
			Name:     "just a file",
			Path:     "foo.txt",
			Expected: []string{"", "foo.txt"},
		},
		{
			Name:     "just a file, absolute",
			Path:     "/foo.txt",
			Expected: []string{"", "foo.txt"},
		},
		{
			Name:     "nested",
			Path:     "foo/bar.txt",
			Expected: []string{"", "foo", "foo/bar.txt"},
		},
		{
			Name:     "nested absolute",
			Path:     "/foo/bar.txt",
			Expected: []string{"", "foo", "foo/bar.txt"},
		},
		{
			Name:     "nested deeper",
			Path:     "/foo/bar/baz/bar.txt",
			Expected: []string{"", "foo", "foo/bar", "foo/bar/baz", "foo/bar/baz/bar.txt"},
		},
	}

	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			got := fs.DirParts(cas.Path)
			if len(got) != len(cas.Expected) {
				t.Errorf("expected %d parts, got %d (%s)", len(cas.Expected), len(got), mustJson(t, got))
				return
			}

			for i, gotPart := range got {
				if gotPart != cas.Expected[i] {
					t.Errorf("expected part %d to equal '%s', got '%s' instead", i, cas.Expected[i], gotPart)
				}
			}
		})
	}
}

type ByStringValue []string

func (a ByStringValue) Len() int           { return len(a) }
func (a ByStringValue) Less(i, j int) bool { return a[i] < a[j] }
func (a ByStringValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func TestInMemoryTreeBuilder_Readdir(t *testing.T) {
	treeData := []string{
		"hello/world/a.txt",
		"hello/world/b.txt",
		"hello/world/c.txt",
		"hello/world/d/e.txt",
		"hello/world/d/f.txt",
		"hello/world/e",
		"hello/zzz.info",
	}

	idx := fs.NewInMemoryTreeBuilder(func(s string) os.FileInfo {
		return &fs.IndexFileInfo{
			SetName: s,
			SetMode: os.ModeDir,
		}
	})
	infos := make([]os.FileInfo, len(treeData))
	for i, p := range treeData {
		infos[i] = &fs.IndexFileInfo{
			SetName:    p,
			SetSize:    100,
			SetMode:    os.ModePerm,
			SetModTime: time.Now(),
			SetSys:     nil,
		}
	}
	sort.Sort(fs.ByName(infos))
	idx.Index(infos)

	// root
	children, err := idx.Readdir("")
	if err != nil {
		t.Fatalf("unexpected error listing dir /: %v", err)
	}
	if len(children) != 1 {
		t.Errorf("expected 1 child, got %d", len(children))
	}

	// inner dir
	children, err = idx.Readdir("hello")
	if err != nil {
		t.Fatalf("unexpected error listing dir hello: %v", err)
	}
	if len(children) != 2 {
		t.Errorf("expected 2 children, got %d", len(children))
	}

	f, err := idx.Stat("hello/world/a.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.Mode() != os.ModePerm {
		t.Errorf("expected file to exist with modPerm")
	}
}

func mustJson(t *testing.T, v interface{}) string {
	d, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("could not get JSON representation of %v: %v", v, err)
	}
	return string(d)
}
