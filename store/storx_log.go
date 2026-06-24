package store

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
)

type StorxLogStore struct {
	mu               sync.Mutex
	indexMu          sync.RWMutex
	partitionLocksMu sync.Mutex
	writersMu        sync.Mutex
	indexWritersMu   sync.Mutex
	appendPipesMu    sync.Mutex
	readersMu        sync.Mutex
	rootDir          string
	segmentsDir      string
	compression      segmentFrameCompression
	readMode         SegmentReadMode
	topics           *collectionmapping.Map[string, TopicConfig]
	records          *collectionmapping.Map[TopicPartition, []segmentRecordPointer]
	timestampRecords *collectionmapping.Map[TopicPartition, []segmentRecordPointer]
	keyRecords       *collectionmapping.Map[TopicPartition, *collectionmapping.Map[string, segmentRecordPointer]]
	keyIndexReady    *collectionset.Set[TopicPartition]
	nextOffsets      *collectionmapping.Map[TopicPartition, uint64]
	metrics          StoreMetrics
	partitionLocks   *collectionmapping.Map[TopicPartition, *sync.Mutex]
	writers          *collectionmapping.Map[string, *segmentWriter]
	indexWriters     *collectionmapping.Map[string, *segmentIndexWriter]
	readers          *collectionmapping.Map[string, *segmentReader]
	appendPipelines  *collectionmapping.Map[TopicPartition, *appendPipeline]
	appendSyncer     *appendDurabilityCoordinator
}

type segmentRecordPointer struct {
	Topic       string `json:"topic"`
	Partition   uint32 `json:"partition"`
	Offset      uint64 `json:"offset"`
	SegmentID   uint64 `json:"segment_id"`
	Position    int64  `json:"position"`
	Length      int    `json:"length"`
	TimestampMS uint64 `json:"timestamp_ms"`
	Attributes  uint16 `json:"attributes"`
}

type segmentStorageFrameKey struct {
	partition uint32
	segmentID uint64
	position  int64
	length    int
}

func OpenStorxLogStore(path string) (*StorxLogStore, error) {
	return OpenStorxLogStoreWithOptions(path, StorxLogOptions{})
}

func OpenStorxLogStoreWithOptions(path string, options StorxLogOptions) (*StorxLogStore, error) {
	options, optionsErr := options.normalize()
	if optionsErr != nil {
		return nil, optionsErr
	}
	compression, compressionErr := newSegmentFrameCompression(options.Compression)
	if compressionErr != nil {
		return nil, compressionErr
	}
	rootDir, normalizeErr := normalizeSegmentRoot(path)
	if normalizeErr != nil {
		return nil, errors.Join(normalizeErr, compression.close())
	}
	segmentsDir := filepath.Join(rootDir, "segments")
	if mkdirErr := os.MkdirAll(segmentsDir, 0o750); mkdirErr != nil {
		return nil, errors.Join(wrapExternal(mkdirErr, "create segment log directory"), compression.close())
	}
	store := &StorxLogStore{
		rootDir:          rootDir,
		segmentsDir:      segmentsDir,
		compression:      compression,
		readMode:         options.ReadMode,
		topics:           collectionmapping.NewMap[string, TopicConfig](),
		records:          collectionmapping.NewMap[TopicPartition, []segmentRecordPointer](),
		timestampRecords: collectionmapping.NewMap[TopicPartition, []segmentRecordPointer](),
		keyRecords:       collectionmapping.NewMap[TopicPartition, *collectionmapping.Map[string, segmentRecordPointer]](),
		keyIndexReady:    collectionset.NewSet[TopicPartition](),
		nextOffsets:      collectionmapping.NewMap[TopicPartition, uint64](),
		metrics:          options.Metrics,
		partitionLocks:   collectionmapping.NewMap[TopicPartition, *sync.Mutex](),
		writers:          collectionmapping.NewMap[string, *segmentWriter](),
		indexWriters:     collectionmapping.NewMap[string, *segmentIndexWriter](),
		readers:          collectionmapping.NewMap[string, *segmentReader](),
		appendPipelines:  collectionmapping.NewMap[TopicPartition, *appendPipeline](),
		appendSyncer:     newAppendDurabilityCoordinator(),
	}
	if err := store.loadLogManifest(); err != nil {
		return nil, errors.Join(err, compression.close())
	}
	if err := store.loadSegmentIndexes(); err != nil {
		return nil, errors.Join(err, compression.close())
	}
	return store, nil
}

func (s *StorxLogStore) Close() error {
	if s == nil {
		return nil
	}
	s.closeAppendPipelines()
	return errors.Join(
		s.appendSyncer.flushPending(s),
		s.closeSegmentWriters(),
		s.closeSegmentIndexWriters(),
		s.closeSegmentReaders(),
		s.compression.close(),
	)
}

func normalizeSegmentRoot(path string) (string, error) {
	if path == "" {
		return "", E(CodeInvalidArgument, "segment log path is required")
	}
	root := path
	if filepath.Ext(path) != "" {
		root = filepath.Dir(path)
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return "", wrapExternal(err, "create segment log root")
	}
	return root, nil
}

func (s *StorxLogStore) CreateTopic(topic TopicConfig) error {
	if err := validateLogTopicForCreate(&topic); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.registerLogTopic(topic); err != nil {
		return err
	}
	if err := s.persistLogManifest(); err != nil {
		return err
	}
	return s.createLogTopicDirs(topic)
}

func validateLogTopicForCreate(topic *TopicConfig) error {
	if topic.Name == "" {
		return E(CodeInvalidArgument, "topic name is required")
	}
	if topic.Partitions == 0 {
		return E(CodeInvalidArgument, "topic %s must have at least one partition", topic.Name)
	}
	normalizeTopic(topic)
	return nil
}

func (s *StorxLogStore) registerLogTopic(topic TopicConfig) error {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if _, exists := s.topics.Get(topic.Name); exists {
		return E(CodeTopicExists, "topic %s already exists", topic.Name)
	}
	s.topics.Set(topic.Name, cloneTopic(topic))
	s.ensureTopicPartitionsLocked(topic)
	return nil
}

func (s *StorxLogStore) createLogTopicDirs(topic TopicConfig) error {
	for partition := range topic.Partitions {
		if err := os.MkdirAll(s.partitionDir(NewTopicPartition(topic.Name, partition)), 0o750); err != nil {
			return wrapExternal(err, "create segment partition directory")
		}
	}
	return nil
}

func (s *StorxLogStore) TopicExists(topic string) (bool, error) {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	_, exists := s.topics.Get(topic)
	return exists, nil
}

func (s *StorxLogStore) StorageUsage(topic string) (uint64, error) {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	cfg, ok := s.topics.Get(topic)
	if !ok {
		return 0, E(CodeTopicNotFound, "topic %s not found", topic)
	}
	seen := collectionset.NewSet[segmentStorageFrameKey]()
	var total uint64
	for partition := range cfg.Partitions {
		tp := NewTopicPartition(topic, partition)
		for _, pointer := range s.records.GetOrDefault(tp, nil) {
			key := segmentStorageFrameKey{
				partition: pointer.Partition,
				segmentID: pointer.SegmentID,
				position:  pointer.Position,
				length:    pointer.Length,
			}
			if seen.Contains(key) {
				continue
			}
			length, err := nonNegativeIntToUint64(pointer.Length, "segment pointer length")
			if err != nil {
				return 0, err
			}
			total += length
			seen.Add(key)
		}
	}
	return total, nil
}

func (s *StorxLogStore) Append(topicPartition TopicPartition, payload []byte) (Record, error) {
	return s.AppendRecord(topicPartition, NewRecordAppend(payload))
}

func (s *StorxLogStore) AppendRecord(topicPartition TopicPartition, appendRecord RecordAppend) (out Record, err error) {
	records, err := s.AppendRecordsBatch(topicPartition, []RecordAppend{appendRecord})
	if err != nil {
		return Record{}, err
	}
	if len(records) == 0 {
		return Record{}, E(CodeCodec, "append record returned no records")
	}
	return records[0], nil
}

func (s *StorxLogStore) partitionLock(topicPartition TopicPartition) *sync.Mutex {
	s.partitionLocksMu.Lock()
	defer s.partitionLocksMu.Unlock()
	lock, _ := s.partitionLocks.GetOrCompute(topicPartition, func() *sync.Mutex {
		return &sync.Mutex{}
	})
	return lock
}

func newStoredRecord(offset uint64, topic TopicConfig, appendRecord RecordAppend) (Record, error) {
	return recordFromAppend(offset, topic, appendRecord)
}
