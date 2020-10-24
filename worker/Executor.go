package worker

import (
	"GoScheduler/common"
	"math/rand"
	"os/exec"
	"time"
)

// 任务执行器
type Executor struct {
}

var (
	G_executor *Executor
)

// 初始化执行器
func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}

// 执行任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd     *exec.Cmd
			err     error
			output  []byte
			result  *common.JobExecuteResult
			jobLock *JobLock
		)

		// 任务结果
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}

		// 创建分布式锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		// 记录任务开始时间，可能执行失败
		result.StartTime = time.Now()

		// 随机睡眠，防止时钟不一致以致总是同一台机器先抢到锁
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		// 抢锁
		err = jobLock.TryLock()

		// 延迟释放锁
		defer jobLock.Unlock()

		if err != nil {
			// 抢锁失败
			result.Err = err
			// 记录任务结束时间
			result.EndTime = time.Now()
		} else {
			// 上锁后，更新任务开始时间
			result.StartTime = time.Now()

			// 执行shell命令
			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)

			// 执行并捕获输出
			output, err = cmd.CombinedOutput()

			result.Output = output
			result.Err = err
			// 执行成功，记录任务结束时间
			result.EndTime = time.Now()

		}

		// 推送任务执行结果
		G_scheduler.PushJobResult(result)
	}()
}
