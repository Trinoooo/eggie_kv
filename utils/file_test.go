package utils

import (
	"fmt"
	"os"
	"syscall"
	"testing"
)

type TestFile struct {
	Description string
	Path        string
}

func TestCheckAndCreateFile(t *testing.T) {
	testList := []*TestFile{
		{
			Description: "abs path & dir not exist & have dir permission",
			Path:        fmt.Sprintf("/Users/%s/go/src/github.com/Trinoooo/eggieKv/test_data/f1", os.Getenv("USER")),
		},
		{
			Description: "abs path & dir exist & have dir permission",
			Path:        fmt.Sprintf("/Users/%s/go/src/github.com/Trinoooo/eggieKv/test_data/f2", os.Getenv("USER")),
		},
		{
			Description: "relative path & dir exist & have dir permission",
			Path:        fmt.Sprintf("/Users/%s/go/src/github.com/Trinoooo/eggieKv/test_data/f3", os.Getenv("USER")),
		},
		{
			Description: "abs path & dir exist & not have dir permission",
			Path:        fmt.Sprintf("/tmp/eggie_kv/test_data/f4"),
		},
	}

	for _, item := range testList {
		_, err := CheckAndCreateFile(item.Path, syscall.O_APPEND|syscall.O_CREAT|syscall.O_RDWR, 0660)
		if err != nil {
			t.Log(item.Description, ":", err)
		} else {
			t.Log(item.Description, "pass")
		}
	}
}
