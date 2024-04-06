package server

import "time"

type TConfig struct {
	Propagation bool

	ServerStopTimeout time.Duration // 调用server.Close之后的等待时间

}
