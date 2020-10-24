package common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

// 定时任务
type Job struct {
	Name     string `json:"name"`
	Command  string `json:"command"`
	CronExpr string `json:"cronExpr"`
}

// 任务调度计划
type JobSchedulePlan struct {
	Job      *Job                 // 调度的任务信息
	Expr     *cronexpr.Expression // 解析后的cronexpr表达式
	NextTime time.Time            // 任务下次调度时间
}

// http接口应答
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

// 变化事件
type JobEvent struct {
	EventType int
	Job       *Job
}

// 任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo // 执行状态
	Output      []byte          // 脚本输出
	Err         error           // 脚本错误原因
	StartTime   time.Time       // 开始时间
	EndTime     time.Time       // 结束时间
}

// 应答方法
func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	var (
		response Response
	)

	response.Errno = errno
	response.Msg = msg
	response.Data = data

	resp, err = json.Marshal(response)

	return
}

// 反序列化job
func UnpackJob(value []byte) (res *Job, err error) {
	job := &Job{}

	if err = json.Unmarshal(value, job); err != nil {
		return
	}

	res = job

	return
}

// 从etcd的key中提取任务名

// 抹掉/cron/jobs/前缀
func ExtractJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

// 抹掉/cron/killer/前缀
func ExtractKillerName(killerKey string) string {
	return strings.TrimPrefix(killerKey, JOB_KILLER_DIR)
}

func ExtractWorkerName(WorkerKey string) string {
	return strings.TrimPrefix(WorkerKey, JOB_WORKER_DIR)
}

// 任务变化事件，（1）更新任务 （2）删除任务
func BuildJobEvent(eventType int, job *Job) *JobEvent {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

// 构造任务执行计划
func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		expr *cronexpr.Expression
	)

	// 解析job的cron表达式
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}

	// 生成任务调度计划
	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}

	return
}

// 任务执行状态
type JobExecuteInfo struct {
	Job        *Job               // 任务信息
	PlanTime   time.Time          // 理论的调度时间
	RealTime   time.Time          // 实际的调度时间
	CancelCtx  context.Context    // 用于取消任务的上下文
	CancelFunc context.CancelFunc // 用于取消任务的取消函数
}

// 构造执行状态信息
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo) {
	jobExecuteInfo = &JobExecuteInfo{
		Job:      jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime, // 计算调度时间
		RealTime: time.Now(),               // 实际调度时间
	}

	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())

	return
}

// 任务执行日志
type JobLog struct {
	JobName      string `json:"jobName" bson:"jobName"`
	Command      string `json:"command" bson:"command"`
	Err          string `json:"err" bson:"err"`
	Output       string `json:"output" bson:"output"`
	PlanTime     int64  `json:"planTime" bson:"planTime"`
	ScheduleTime int64  `json:"scheduleTime" bson:"scheduleTime"`
	StartTime    int64  `json:"startTime" bson:"startTime"`
	EndTime      int64  `json:"endTime" bson:"endTime"`
}

// 日志批次
type LogBatch struct {
	Logs []interface{}
}

// 任务日志过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

// 任务日志排序规则
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"`
}
