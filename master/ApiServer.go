package master

import (
	"GoScheduler/common"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"
)

var (
	G_apiServer *ApiServer
)

// 任务的HTTP接口
type ApiServer struct {
	httpServer *http.Server
}

// 保存任务接口
func handleJobSave(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		postJob string
		job     common.Job
		oldjob  *common.Job
		bytes   []byte
	)

	// 解析post表单
	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	// 获取job字段
	postJob = r.PostForm.Get("job")

	// 反序列化
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}

	// 保存到etcd
	if oldjob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}

	// 返回正常应答
	if bytes, err = common.BuildResponse(0, "success", oldjob); err == nil {
		w.Write(bytes)
	}
	return

	// 返回异常应答
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
	return
}

// 删除任务接口
func handleJobDelete(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		name   string
		oldjob *common.Job
		bytes  []byte
	)
	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	name = r.PostForm.Get("name")

	if oldjob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", oldjob); err == nil {
		w.Write(bytes)
	}

	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
	return
}

// 列举任务接口
func handleJobList(w http.ResponseWriter, r *http.Request) {
	var (
		jobList []*common.Job
		err     error
		bytes   []byte
	)

	if jobList, err = G_jobMgr.ListJobs(); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", jobList); err == nil {
		w.Write(bytes)
	}

	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
	return
}

// 杀死任务接口
func handleJobKill(w http.ResponseWriter, r *http.Request) {
	var (
		name  string
		err   error
		bytes []byte
	)

	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	name = r.PostForm.Get("name")

	if err = G_jobMgr.KillJob(name); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		w.Write(bytes)
	}

	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}

	return
}

// 任务日志接口
func handleJobLog(w http.ResponseWriter, r *http.Request) {
	var (
		name       string
		err        error
		skipParam  string
		limitParam string
		skip       int
		limit      int
		logArr     []*common.JobLog
		bytes      []byte
	)

	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	name = r.Form.Get("name")

	skipParam = r.Form.Get("skip")

	limitParam = r.Form.Get("limit")

	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}

	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = 20
	}

	if logArr, err = G_logMgr.ListLog(name, skip, limit); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", logArr); err == nil {
		w.Write(bytes)
	}

	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}

	return
}

// 获取健康worker节点列表
func handleWorkerList(w http.ResponseWriter, r *http.Request) {
	var (
		workerArr []string
		err       error
		bytes     []byte
	)

	if workerArr, err = G_workerMgr.ListWorkers(); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", workerArr); err == nil {
		w.Write(bytes)
	}

	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}

	return
}

// 初始化服务
func InitApiServer() (err error) {
	// 配置路由
	mux := http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkerList)

	// 静态文件目录
	staticDir := http.Dir(G_config.WebRoot)
	staticHandler := http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler))

	// 启动TCP监听
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort))
	if err != nil {
		return
	}

	// 创建一个HTTP服务
	httpServer := &http.Server{
		ReadHeaderTimeout: time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout:      time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:           mux,
	}

	// 赋值单例
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	// 启动服务端
	go httpServer.Serve(listener)

	return
}
