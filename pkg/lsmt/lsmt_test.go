package lsmt

import "testing"

func TestGet(t *testing.T) {
	err := Set("a", "b")
	if err != nil {
		panic(err)
	}
	get, err := Get("a")
	if err != nil {
		panic(err)
	}
	t.Log(get)

}
