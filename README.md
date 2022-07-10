# learn-db
Bitcask 引擎实现，

## 索引结构
```rust
struct Node {
    // key的hash值
    hash: u64,
    // 文件id
    file_id: u32,
    // 偏移量
    offset: u32,
    // value大小
    length: u32
}
```

可以看到，node不记录原始key的值，这样是为了让node大小可控，占用的内存更少。
node的大小是 8+4+4 +4= 20字节，1G内存可存储53687091个key。

value记录的是文件系统的偏移量，考虑到hash碰撞，value其实是个链表，
需要读取后再根据key比对，才能找到实际值。

hash算法一定要尽量避免碰撞

[Bitcask 引擎]https://www.51cto.com/article/697752.html