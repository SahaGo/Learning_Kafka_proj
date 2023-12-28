package model

import "time"

type Event struct {
	Id        string
	Message   string
	Success   bool
	Variables []int
	Time      time.Time
}
