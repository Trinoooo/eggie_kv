package wal

import "fmt"

type segment struct {
}

func buildPath(blockId int64) string {
	return fmt.Sprintf("%020d", blockId)
}

func baseToBlockId(base string) (int64, error) {
	var firstBlockIdOfSegment int64
	_, err := fmt.Sscanf(base, "%020d", &firstBlockIdOfSegment)
	if err != nil {
		return 0, err
	}

	return firstBlockIdOfSegment, nil
}
