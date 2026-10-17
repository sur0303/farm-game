# 脏标记和水位（面试常问）

`DirtyFlags` 只表示**这一轮 handle 改过模型**，结束就清。  
真正表示「库落后于内存」的是两个**只活在进程里**的数字：

```text
version        内存已经走到第几代
savedVersion   已经成功写进 Redis 的是第几代
IsDirty     =  savedVersion < version
```

Redis / Tcaplus **不存**这两个数。

## 正常一轮

```text
version=5, savedVersion=5   干净
浇水，DirtyFlags 打上
handle 结束：version=6，flags 清空
5 < 6 → 入队存盘
OnSave 冻结 6，HMSet
OnSaveSucc(6)：savedVersion=6   干净
```

## 边存边改

存盘会让出 mailbox，别的命令可以继续跑：

```text
浇水1 结束：version=6，开始写库
浇水2 进来：version=7
第 6 代写成功：savedVersion=6
6 < 7 → 仍然脏，再入队
再把第 7 代写出去
```

`OnSaveSucc` 只承认**这次带出去的那一代**，不会一成功就跳到当前最新。否则浇水2 会丢。

和 CAS 的差别：水位是内存里比大小；CAS 是 Redis 脚本里带 expected version，库不同意就失败。
