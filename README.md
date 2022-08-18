# learn-db
使用rust实现的基于 [Bitcask 引擎](https://www.51cto.com/article/697752.html) 的kv数据库。

## 索引篇
为了快速查找，所有的索引数据都是放在内存中，可以简单的理解为 HashMap.

不过为了能够并发的查询和写入，所以实现了分段锁（数组+链表）；

另外，数据库运行的过程中，索引数量会不断的增减，所以也实现了动态扩缩容的能力。

具体实现：src/index/dynamic_index.rs


