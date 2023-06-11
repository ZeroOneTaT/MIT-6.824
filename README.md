# MIT-6.824
该项目为MIT公开课项目，我们将逐步学习分布式存储，并完成一个分布式存储系统。

***

## 项目进度：
- [ ] MapReduce

***
## lab 1. 实现MapReduce
基础代码框架中提供了**简单的单进程串行**`MapReduce`参考实现，位于`main/mrsequential.go`文件中，可以通过以下命令运行、查看一下效果：

```shell
cd src/main
go build -buildmode=plugin ../mrapps/wc.go

rm mr-out*
go run mrsequential.go wc.so pg*.txt

more mr-out-0
```
输出文件在`src/main/mr-out-0`，文件中每一行标明了单词和出现次数。

`go build -buildmode=plugin ../mrapps/wc.go`该命令的作用是构建 MR APP 的动态链接库，使用了 Golang 的 Plugin 来构建 MR APP，使得 MR 框架的代码可以和 MR APP 的代码分开编译，而后 MR 框架再通过动态链接的方式载入指定的 MR APP 运行。
> 具体说：`wc.go` 实现了`Ma`p函数和`Reduce`函数，但`Map`和`Reduce`相当于基础性代码，我们可能会随时改变它，采用上述的动态链接，我们只需要改变`wc.go`, 然后编译成`wc.so`, `go run mrsequential.go wc.so pg*.txt`这样就可以载入我们整体性架构了，其他架构代码不需要重新部署和运行。
`mrsequential.go`中载入动态库(如`wc.so`)代码为：`mapf, reducef := loadPlugin(os.Args[1])`

OK，进入正文。
### 实验准备
[MapReduce论文](http://static.googleusercontent.com/media/research.google.com/zh-CN//archive/mapreduce-osdi04.pdf)
[lab1实验文档](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
[lab1实验文档译文](https://github.com/he2121/MIT6.824-2021/blob/main/docs-cn/lab-01.md)

### 实验需求
实验要求我们在分布式存储系统(实验为单机模拟分布式系统)中，实现Coordinator(Matser)和Worker，同一时刻有1个Coordinator process、多个 Worker process 运行，彼此间使用 RPC 进行通信。

**Coordinator(Master)**
- 负责为每个 Worker 分配 Map 或者 Reduce 任务
- 确保每个文件只被处理一次，且最终输出结果正确
- 容错机制，当 Worker 超时或崩溃时，将任务重新分配到其他 Worker

**Worker**
- 向 Coordinator 请求任务询问 Coordinator 以从文件中读取任务输入，并将任务输出写入到文件中
- 对于 map 任务，将输入为 pg-xxx.txt 的文档处理为一组键值对，并按 reduce 任务的数量分桶，得到一组中间文件 mr-X-Y
- 对于 reduce 任务，将属于当前任务的中间文件全部读入，排序后输出到结果文件 mr-out-n 中