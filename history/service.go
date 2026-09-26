package history

// Service 是对外的门面，组合存储、时间点查询、压缩与回放能力。
type Service struct {
	store *Store
}

// Open 打开（或创建）目录下的历史服务，并恢复已提交历史。
// 崩溃后重开会自动截断 WAL 尾部未完成写入，从最近一次已提交
// 检查点及之后的已确认事件完整恢复。
func Open(dir string) (*Service, error) {
	st, err := openStore(dir)
	if err != nil {
		return nil, err
	}
	return &Service{store: st}, nil
}

// Store 返回底层存储，提供提交、查询、检查点、压缩与回放接口。
func (s *Service) Store() *Store {
	return s.store
}

// Close 关闭服务。
func (s *Service) Close() error {
	return s.store.Close()
}
