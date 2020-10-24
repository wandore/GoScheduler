package worker

import (
	"GoScheduler/common"
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// mongodb存储日志
type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch // 通知chan
}

var (
	G_logSink *LogSink
)

// 初始化
func InitLogSink() (err error) {
	var (
		client        *mongo.Client
		clientOptions *options.ClientOptions
	)

	// 连接mongodb
	clientOptions = options.Client().ApplyURI(G_config.MongodbUri)
	clientOptions.SetConnectTimeout(time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond)

	if client, err = mongo.Connect(context.TODO(), clientOptions); err != nil {
		return
	}

	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	go G_logSink.writeLoop()

	return
}

// 日志存储协程
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch
		commitTimer  *time.Timer
		timeoutBatch *common.LogBatch
	)

	for {
		select {
		case log = <-logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				// 超时自动提交
				commitTimer = time.AfterFunc(
					time.Duration(1000)*time.Millisecond,
					func(batch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- batch
						}
					}(logBatch),
				)
			}

			logBatch.Logs = append(logBatch.Logs, log)

			// 日志最大批次100
			if len(logBatch.Logs) >= 100 {
				// 写入日志
				logSink.saveLogs(logBatch)
				// 清空logBatch
				logBatch = nil
				// 取消定时器
				commitTimer.Stop()
			}
		// 提交过期批次
		case timeoutBatch = <-logSink.autoCommitChan:
			// 判断过期批次是否仍是当前批次
			if timeoutBatch != logBatch {
				continue
			}
			logSink.saveLogs(timeoutBatch)
			logBatch = nil
		}
	}
}

// 批量写入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// 发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		// 队列满了就丢弃
	}
}
