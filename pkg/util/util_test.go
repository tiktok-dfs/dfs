package util

import (
	"fmt"
	"testing"
)

func TestGetPrePath(t *testing.T) {
	prePath := GetPrePath("/test/hello.txt/")
	fmt.Println(prePath)
}
