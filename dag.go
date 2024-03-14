package merkledag

import (
	"encoding/json"
	"hash"
)

// 定义分片长度
const (
	K         = 1 << 10
	FILE_SIZE = 256 * K
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) []byte {
	// TODO 将分片写入到KVStore中，并返回Merkle Root

	//判断node类型
	switch node.Type() {
	case FILE:
		// 对Node进行文件形式存储
		fileNode := node.(File)
		fileData := fileNode.Bytes()

		// 判断文件大小是否需要分片
		nodeSize := fileNode.Size() / K
		if nodeSize > FILE_SIZE {
			// 大于size则进行分片，构建一个list
			list := Object{
				Links: []Link{},
				Data:  []byte("list"),
			}

			for i := 0; ; i++ {
				start := i * FILE_SIZE
				end := (i + 1) * FILE_SIZE

				// 退出条件, 当start大于等于nodeSize时退出
				if start >= int(nodeSize) {
					break
				}

				// 判断是否为最后一个分片
				if end > int(nodeSize) {
					end = int(nodeSize)
				}

				// 每个分片存储为Object
				data := fileData[start:end]
				obj := Object{
					Links: nil,
					Data:  data,
				}
				// 计算Objecthash
				h.Reset()
				h.Write(StructToByte(obj))
				fileHash := h.Sum(nil)
				// 存入KVStore
				store.Put(fileHash, StructToByte(obj))
				// 构建list的link
				link := Link{
					Name: "part" + string(i),
					Hash: fileHash,
					Size: end - start,
				}
				// 存储每部分的listhash
				list.Links = append(list.Links, link)
			}
			// 将list存入KVStore
			h.Reset()
			h.Write(StructToByte(list))
			ListHash := h.Sum(nil)

			store.Put(ListHash, StructToByte(list))
			return ListHash
		} else {
			// 小于size则直接存储
			obj := Object{
				Links: nil,
				Data:  fileData,
			}

			h.Reset()
			h.Write(StructToByte(obj))
			fileHash := h.Sum(nil)

			// 存入KVStore
			store.Put(fileHash, StructToByte(obj))
			return fileHash
		}
	case DIR:
		// 对Node进行目录形式存储
		dirNode := node.(Dir)
		// 最终形成一个tree对象
		tree := Object{
			Links: []Link{},
			Data:  []byte("tree"),
		}
		// 遍历目录下的文件
		dirIterator := dirNode.It()

		for dirIterator.Next() {
			childNode := dirIterator.Node()
			if childNode.Type() == DIR {
				// 对子目录进行添加
				childDir := childNode.(Dir)
				childTreeHash := Add(store, childNode, h)
				link := Link{
					Name: childDir.Name(),
					Hash: childTreeHash,
					Size: int(childDir.Size()),
				}
				tree.Links = append(tree.Links, link)
			} else {
				// 对子文件进行添加
				childFile := childNode.(File)
				childFileHash := Add(store, childNode, h)
				link := Link{
					Name: childFile.Name(),
					Hash: childFileHash,
					Size: int(childFile.Size()),
				}
				tree.Links = append(tree.Links, link)
			}
		}

		// 将tree存入KVStore
		h.Reset()
		h.Write(StructToByte(tree))
		treeHash := h.Sum(nil)

		store.Put(treeHash, StructToByte(tree))
		return treeHash
	}

	return nil
}

func StructToByte(obj interface{}) []byte {
	o, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}
	return o
}
