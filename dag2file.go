package merkledag

import (
	"encoding/json"
	"strings"
)

// Hash to file
// hash作为roothash
// path是一个文件从根路径开始的文件路径
// TODO 分片的文件采用递归进行获取每个分片的数据进行拼接
func Hash2File(store KVStore, hash []byte, path string, hp HashPool) []byte {
	// 根据hash和path， 返回对应的文件, hash对应的类型是tree
	// h := hp.Get()

	// 获取hash对应的object的二进制数据
	obj, success := store.Get(hash)
	if success != nil {
		panic("Hash2File: hash not found")
	}

	// 将二进制数据转换为Object
	var t Object
	json.Unmarshal(obj, t)

	// 判断Object的类型
	if t.Links == nil {
		// links为空，说明是file类型
		return t.Data
	} else {
		// links不为空，说明是tree类型
		// 遍历links，找到对应的path
		headPath, tailPath:= splitPath(path)
		for _, link := range t.Links {
			if link.Name == headPath {
				// 递归调用Hash2File
				return Hash2File(store, link.Hash, tailPath, hp)
			}
		}
	}
	return nil
}

func splitPath(path string) (head, tail string) {
	parts := strings.Split(path, "/")
	if len(parts) > 1 {
		head = parts[0]
		tail = strings.Join(parts[1:], "/")
	} else {
		head = parts[0]
		tail = ""
	}
	return
}