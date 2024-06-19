package server

// ProactorServer 自实现的proactor网络模式服务器
// 或许后续可以实现个windows版本
type ProactorServer struct {
}

func NewProactorServer() *ProactorServer {
	return &ProactorServer{}
}

func (ps *ProactorServer) Serve() {

}

func (ps *ProactorServer) Close() error {
	return nil
}
