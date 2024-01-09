package consumer

import "encoding/json"

type Task struct {
	// basic
	TraceId      string `json:"trace_id"`      // trace_id
	SpanId       string `json:"span_id"`       // span_id
	Sampled      bool   `json:"sampled"`       // sampled
	CreateTime   int64  `json:"create_time"`   // 创建时间戳,毫秒级
	JobId        string `json:"job_id"`        // 任务唯一 id
	TaskName     string `json:"task_name"`     // 任务名称
	Tag          string `json:"tag"`           // 任务标签
	From         string `json:"from"`          // 来源
	Priority     int    `json:"priority"`      // 优先级
	Principal    string `json:"principal"`     // 责任人
	GroupName    string `json:"group_name"`    // 任务组名
	ExecTime     int64  `json:"exec_time"`     // 执行时间戳,毫秒级
	DeadlineTime int64  `json:"deadline_time"` // 最后期限时间戳,毫秒级
	Timeout      int    `json:"timeout"`       // 超时时间,毫秒数
	Topic        string `json:"topic"`         // 发送的 topic
	DelayTime    int64  `json:"delay_time"`    // 延时时间,毫秒级
	MQName       string `json:"-"`             // mq 集群名称

	// http
	Url         string `json:"url"`          // 回调 url
	ContentType string `json:"content_type"` // 回调类型 form,json,jsonstr,
	Content     string `json:"content"`      // 回调内容
	Headers     string `json:"headers"`      // 回调header
	ClientACK   bool   `json:"client_ack"`   // 消息确认
}

type TaskContent struct {
	Topic   string `json:"topic"`
	Channel string `json:"channel"`
	Flag    string `json:"flag"`
	Param   string `json:"param"`
}

func (c *TaskContent) ToString() string {
	b, err := json.Marshal(c)
	if err != nil {
		return ""
	}

	return string(b)
}
