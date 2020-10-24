package main

import (
	"GoScheduler/master"
	"flag"
	"fmt"
	"runtime"
	"time"
)

var (
	confFile string // 配置文件路径
)

// 解析命令行参数
func initArgs() {
	flag.StringVar(&confFile, "config", "./master.json", "指定master.json")
	flag.Parse()
}

// 初始化线程数量
func initEnv() {
	// 线程数等于计算机CPU核心数
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var err error

	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()

	// 加载配置
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 任务管理器
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	// 	服务发现
	if err = master.InitWorkerMgr(); err != nil {
		goto ERR
	}

	// 日志管理器
	if err = master.InitLogMgr(); err != nil {
		goto ERR
	}

	// 启动Api HTTP服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}

	// 正常退出
	return

ERR:
	fmt.Println(err)
}
