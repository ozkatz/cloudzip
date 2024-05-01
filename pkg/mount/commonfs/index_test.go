package commonfs_test

import (
	"os"
	"sort"
	"testing"
	"time"

	"github.com/ozkatz/cloudzip/pkg/mount/commonfs"
)

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

	idx := commonfs.NewInMemoryTreeBuilder(func(filename string) *commonfs.FileInfo {
		return commonfs.ImmutableDir(filename, time.Now())
	})
	infos := make(commonfs.FileInfoList, len(treeData))
	for i, p := range treeData {
		infos[i] = commonfs.ImmutableInfo(p, time.Now(), os.ModePerm, 100, nil)
	}
	sort.Sort(infos)
	err := idx.Index(infos)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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
