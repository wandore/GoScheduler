package master

import (
	"GoScheduler/common"
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

var (
	G_logMgr *LogMgr
)

// mongodb日志管理
type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

func InitLogMgr() (err error) {
	var (
		client        *mongo.Client
		clientOptions *options.ClientOptions
	)

	clientOptions = options.Client().ApplyURI(G_config.MongodbUri)
	clientOptions.SetConnectTimeout(time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond)

	if client, err = mongo.Connect(context.TODO(), clientOptions); err != nil {
		return
	}

	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}

	return
}

// 查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error) {
	var (
		filter      *common.JobLogFilter
		logSort     *common.SortLogByStartTime
		findOptions *options.FindOptions
		cursor      *mongo.Cursor
		jobLog      *common.JobLog
	)

	logArr = make([]*common.JobLog, 0)

	filter = &common.JobLogFilter{JobName: name}

	logSort = &common.SortLogByStartTime{SortOrder: -1}

	findOptions = options.Find()
	findOptions.SetSort(logSort)
	findOptions.SetSkip(int64(skip))
	findOptions.SetLimit(int64(limit))

	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, findOptions); err != nil {
		return
	}

	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}

		if err = cursor.Decode(jobLog); err != nil {
			continue
		}

		logArr = append(logArr, jobLog)
	}

	return
}
