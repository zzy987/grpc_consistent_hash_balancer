package balancer

import "time"

const (
	Policy             = "consistent_hash_policy"
	Key                = "task_id"
	connectionLifetime = time.Second * 5
)
