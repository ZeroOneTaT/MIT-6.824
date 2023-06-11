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

> `mrsequential.go`中载入动态库(如，wc.so)代码为：`mapf, reducef := loadPlugin(os.Args[1])`