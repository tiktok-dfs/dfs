package tree

import (
	"testing"
)

// setup 初始化
//          /
//  2       3       4
//5 6 7    8 9     10
func setup() *DirTree {
	root := &DirTreeNode{
		Name: "/",
		Children: []*DirTreeNode{
			&DirTreeNode{
				Name: "2",
				Children: []*DirTreeNode{
					&DirTreeNode{Name: "5"},
					&DirTreeNode{Name: "6"},
					&DirTreeNode{Name: "7"},
				},
			},

			&DirTreeNode{
				Name: "3",
				Children: []*DirTreeNode{
					&DirTreeNode{Name: "8"},
					&DirTreeNode{Name: "9"},
				},
			},

			&DirTreeNode{
				Name: "4",
				Children: []*DirTreeNode{
					&DirTreeNode{Name: "10"},
				},
			},
		},
	}

	return &DirTree{root}
}

func TestLookAll(t *testing.T) {
	tree := setup()
	t.Log(tree.LookAll())
}

// TestInsert 结果应为
//          /
//  2       3       4
//5 6 7    8 9     10
//11
func TestInsert(t *testing.T) {
	tree := setup()
	ok := tree.Insert("/2/5/11/")
	if !ok {
		t.Error("插入失败")
	}

	t.Log(tree.LookAll())
	t.Log("以上结果应为 [/ 2 3 4 5 6 7 8 9 10 11]")
}

// TestFindSubDir 结果应为 [5, 6, 7]
func TestFindSubDir(t *testing.T) {
	tree := setup()
	t.Log(tree.FindSubDir("/2/"))
	t.Log("以上结果应为 [5, 6, 7]")
}
