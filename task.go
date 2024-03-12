package consumer

import "encoding/json"

type Task struct {
	JobId   string `json:"job_id"`
	TaskName string `json:"task_name"`	
	Content string `json:"content"` // 回调内容
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
