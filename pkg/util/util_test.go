package util

import (
	"testing"
)

func TestCheck(t *testing.T) {

}

func TestGetPrePath(t *testing.T) {
	prePath := GetPrePath("/test/hello.txt")
	if prePath != "/test/" {
		t.Error("boom")
	}
}

func TestModFilePath(t *testing.T) {
	modedFilePath := ModFilePath("test/hello.txt")
	if modedFilePath != "/test/hello.txt" {
		t.Error("expected: ", "/test/hello.txt", " get: ", modedFilePath)
	}

	modedFilePath = ModFilePath("test/hello.txt/")
	if modedFilePath != "/test/hello.txt" {
		t.Error("expected: ", "/test/hello.txt", " get: ", modedFilePath)
	}

}
