package data

type Manager struct {
}

func NewManager() *Manager {
	return nil
}

func (m *Manager) Write(data []byte) (int64, error) {
	return 0, nil
}

func (m *Manager) MultiWrite(dataList [][]byte) ([]int64, error) {
	return nil, nil
}

func (m *Manager) Remove(idx int64) error {
	return nil
}

func (m *Manager) MultiRemove(idxList []int64) error {
	return nil
}

func (m *Manager) Modify(idx int64, data []byte) error {
	return nil
}

func (m *Manager) MultiModify(map[int64][]byte) error {
	return nil
}

func (m *Manager) Read(idx int64) ([]byte, error) {
	return nil, nil
}

func (m *Manager) MultiRead(idxList []int64) (map[int64][]byte, error) {
	return nil, nil
}
