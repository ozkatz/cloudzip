package webdav_test

import (
	"github.com/ozkatz/cloudzip/pkg/webdav"
	"testing"
)

func TestIsDirectDescendant(t *testing.T) {
	cases := []struct {
		Name     string
		Path     string
		BaseDir  string
		Expected bool
	}{
		{
			Name:     "direct_descendant",
			Path:     "a/b/c",
			BaseDir:  "a/b",
			Expected: true,
		},
		{
			Name:     "direct_descendant_with_slash",
			Path:     "a/b/c",
			BaseDir:  "a/b/",
			Expected: true,
		},
		{
			Name:     "direct_descendant_with_slash_in_path",
			Path:     "a/b/c/",
			BaseDir:  "a/b",
			Expected: true,
		},
		{
			Name:     "direct_descendant_slash_both",
			Path:     "a/b/c/",
			BaseDir:  "a/b/",
			Expected: true,
		},
		{
			Name:     "indirect_descendant",
			Path:     "a/b/c/",
			BaseDir:  "a",
			Expected: false,
		},
		{
			Name:     "indirect_descendant_slash_base",
			Path:     "a/b/c",
			BaseDir:  "a/",
			Expected: false,
		},
		{
			Name:     "non_descendant",
			Path:     "x/a/b/c",
			BaseDir:  "a/",
			Expected: false,
		},
		{
			Name:     "non_descendant_qualifying_slash",
			Path:     "/x/a/b/c",
			BaseDir:  "a/",
			Expected: false,
		},
		{
			Name:     "non_descendant_qualifying_slash_both",
			Path:     "/x/a/b/c",
			BaseDir:  "/a/",
			Expected: false,
		},
	}
	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			got := webdav.IsDirectDescendant(cas.Path, cas.BaseDir)
			if got != cas.Expected {
				t.Errorf("expected IsDirectDescendant() -> %t, got %t", cas.Expected, got)
			}
		})
	}
}
