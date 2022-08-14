package tree

import (
	"fmt"
	"strings"
)

type DirTree struct {
	Root *DirTreeNode
}

type DirTreeNode struct {
	Name     string
	Children []*DirTreeNode
}

// Insert 插入目录 参数应带/
func (tree *DirTree) Insert(path string) bool {
	// len(strings.Split("/")) == 2 ["", ""]
	// len(strings.Split("/hello/")) == 3 ["", "hello", ""]
	ancestors := strings.Split(path, "/")[1:]

	if len(ancestors) < 1 && ancestors[0] == "" {
		return false
	}

	var parent *DirTreeNode = tree.Root

	//确保所有的父节点都存在
	for i := 0; i < len(ancestors)-2; i++ {
		parent = tree.seek(parent, ancestors[i])
	}

	dirName := ancestors[len(ancestors)-2]

	parent.Children = append(parent.Children, &DirTreeNode{Name: dirName})

	return true
}

// seek 从node开始查找name节点
func (tree *DirTree) seek(node *DirTreeNode, name string) *DirTreeNode {
	if node == nil {
		return nil
	}

	if node.Name == name {
		return node
	}

	if node.Children != nil && len(node.Children) > 0 {
		for i := 0; i < len(node.Children); i++ {
			node := tree.seek(node.Children[i], name)
			if node != nil {
				return node
			}
		}
	}
	return nil
}

// FindSubDir 查找路径下的子目录 参数应带上/
func (tree *DirTree) FindSubDir(path string) (subDirs []string) {
	ancestors := strings.Split(path, "/")[1:]

	if len(ancestors) < 1 && ancestors[0] == "" {
		return
	}

	var parent *DirTreeNode = tree.Root

	// 找到最后一个parent
	for i := 0; i < len(ancestors)-1; i++ {
		parent = tree.seek(parent, ancestors[i])
	}

	for _, child := range parent.Children {
		subDirs = append(subDirs, child.Name)
	}

	return
}

// LookAll 调试用, DFS查看整个目录树的内容
func (tree *DirTree) LookAll() string {
	nodes := make([]string, 0)
	// 初始化队列
	queue := []*DirTreeNode{tree.Root}
	// 当队列中没有元素，那么结束
	for len(queue) > 0 {
		var count = 0
		for i := range queue {
			// 计数+1
			count++
			// 保存值
			nodes = append(nodes, queue[i].Name)
			// 子节点入队
			for j := range queue[i].Children {
				queue = append(queue, queue[i].Children[j])
			}

		}
		// 类似于出队，将遍历过的删掉
		queue = queue[count:]

	}

	return fmt.Sprintf("%s", nodes) // [1 2 3 4 5 6 7 8 9 10]
}
