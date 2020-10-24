package master

import (
	"encoding/json"
	"io/ioutil"
)

var (
	G_config *Config
)

// 程序配置
type Config struct {
	ApiPort               int      `json:"apiPort"`
	ApiReadTimeout        int      `json:"apiReadTimeout"`
	ApiWriteTimeout       int      `json:"apiWriteTimeout"`
	EtcdEndpoints         []string `json:"etcdEndpoints"`
	EtcdDialTimeout       int      `json:"etcdDialTimeout"`
	WebRoot               string   `json:"webroot"`
	MongodbUri            string   `json:"mongodbUri"`
	MongodbConnectTimeout int      `json:"mongodbConnectTimeout"`
}

// 加载配置
func InitConfig(filename string) (err error) {
	var conf Config

	// 读取配置文件
	context, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}

	// json反序列化
	if err = json.Unmarshal(context, &conf); err != nil {
		return
	}

	// 赋值单例
	G_config = &conf

	return
}
