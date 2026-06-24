package store

import (
	"cmp"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"

	json "github.com/goccy/go-json"
	renameio "github.com/google/renameio/v2/maybe"
)

const segmentManifestFile = "topics.json"

type segmentManifest struct {
	Topics []TopicConfig `json:"topics"`
}

func (s *StorxLogStore) loadLogManifest() error {
	root, err := os.OpenRoot(s.rootDir)
	if err != nil {
		return wrapExternal(err, "open segment log root")
	}
	file, err := root.Open(segmentManifestFile)
	closeRootErr := root.Close()
	if errors.Is(err, os.ErrNotExist) {
		return wrapExternal(closeRootErr, "close segment log root")
	}
	if err != nil {
		return errors.Join(wrapExternal(err, "open segment log manifest"), wrapExternal(closeRootErr, "close segment log root"))
	}
	data, readErr := io.ReadAll(file)
	closeFileErr := file.Close()
	if readErr != nil {
		return errors.Join(wrapExternal(readErr, "read segment log manifest"), wrapExternal(closeFileErr, "close segment log manifest"))
	}
	if closeFileErr != nil {
		return wrapExternal(closeFileErr, "close segment log manifest")
	}
	if closeRootErr != nil {
		return wrapExternal(closeRootErr, "close segment log root")
	}
	var manifest segmentManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return wrapExternal(err, "decode segment log manifest")
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	for i := range manifest.Topics {
		topic := manifest.Topics[i]
		normalizeTopic(&topic)
		s.topics.Set(topic.Name, cloneTopic(topic))
		s.ensureTopicPartitionsLocked(topic)
	}
	return nil
}

func (s *StorxLogStore) persistLogManifest() error {
	s.indexMu.RLock()
	topics := make([]TopicConfig, 0, s.topics.Len())
	s.topics.Range(func(_ string, topic TopicConfig) bool {
		topics = append(topics, cloneTopic(topic))
		return true
	})
	s.indexMu.RUnlock()
	manifest := segmentManifest{Topics: sortTopics(topics)}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return wrapExternal(err, "encode segment log manifest")
	}
	if err := os.MkdirAll(s.rootDir, 0o750); err != nil {
		return wrapExternal(err, "create segment log manifest directory")
	}
	if err := renameio.WriteFile(s.segmentManifestPath(), data, 0o600); err != nil {
		return wrapExternal(err, "write segment log manifest")
	}
	return nil
}

func (s *StorxLogStore) segmentManifestPath() string {
	return filepath.Join(s.rootDir, segmentManifestFile)
}

func (s *StorxLogStore) ensureTopicPartitionsLocked(topic TopicConfig) {
	for partition := range topic.Partitions {
		tp := NewTopicPartition(topic.Name, partition)
		if _, ok := s.records.Get(tp); !ok {
			s.records.Set(tp, nil)
		}
		if _, ok := s.nextOffsets.Get(tp); !ok {
			s.nextOffsets.Set(tp, nextOffsetFromPointers(s.records.GetOrDefault(tp, nil)))
		}
	}
}

func sortTopics(topics []TopicConfig) []TopicConfig {
	if len(topics) == 0 {
		return nil
	}
	sorted := slices.Clone(topics)
	slices.SortFunc(sorted, func(left, right TopicConfig) int {
		return cmp.Compare(left.Name, right.Name)
	})
	return sorted
}
