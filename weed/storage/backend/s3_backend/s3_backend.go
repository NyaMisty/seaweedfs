package s3_backend

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/google/uuid"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
)

func init() {
	backend.BackendStorageFactories["s3"] = &S3BackendFactory{}
}

type S3BackendFactory struct {
}

func (factory *S3BackendFactory) StorageType() backend.StorageType {
	return backend.StorageType("s3")
}
func (factory *S3BackendFactory) BuildStorage(configuration backend.StringProperties, configPrefix string, id string) (backend.BackendStorage, error) {
	return newS3BackendStorage(configuration, configPrefix, id)
}

type S3BackendStorage struct {
	id                    string
	aws_access_key_id     string
	aws_secret_access_key string
	region                string
	bucket                string
	endpoint              string
	cache_dir             string
	storageClass          string
	conn                  s3iface.S3API
}

func newS3BackendStorage(configuration backend.StringProperties, configPrefix string, id string) (s *S3BackendStorage, err error) {
	s = &S3BackendStorage{}
	s.id = id
	s.aws_access_key_id = configuration.GetString(configPrefix + "aws_access_key_id")
	s.aws_secret_access_key = configuration.GetString(configPrefix + "aws_secret_access_key")
	s.region = configuration.GetString(configPrefix + "region")
	s.bucket = configuration.GetString(configPrefix + "bucket")
	s.endpoint = configuration.GetString(configPrefix + "endpoint")
	s.cache_dir = configuration.GetString(configPrefix + "cache_dir")
	s.storageClass = configuration.GetString(configPrefix + "storage_class")
	if s.storageClass == "" {
		s.storageClass = "STANDARD_IA"
	}

	s.conn, err = createSession(s.aws_access_key_id, s.aws_secret_access_key, s.region, s.endpoint)

	glog.V(0).Infof("created backend storage s3.%s for region %s bucket %s", s.id, s.region, s.bucket)
	return
}

func (s *S3BackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["aws_access_key_id"] = s.aws_access_key_id
	m["aws_secret_access_key"] = s.aws_secret_access_key
	m["region"] = s.region
	m["bucket"] = s.bucket
	m["endpoint"] = s.endpoint
	m["cache_dir"] = s.cache_dir
	m["storage_class"] = s.storageClass
	return m
}

func (s *S3BackendStorage) NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}

	f := &S3BackendStorageFile{
		backendStorage: s,
		key:            key,
		tierInfo:       tierInfo,
	}

	if f.backendStorage.cache_dir != "" {
		// has cache implementation
		cacheFilePath := path.Join(f.backendStorage.cache_dir, f.key)
		f.dbRef = NewLevelDBWeakref(cacheFilePath, &opt.Options{
			//AltFilters:                            nil,
			BlockCacher:        opt.NoCacher,
			BlockCacheCapacity: -1,
			//BlockCacheEvictRemoved:                false,
			//BlockRestartInterval:                  0,
			//BlockSize:                             0,
			//CompactionExpandLimitFactor:           0,
			//CompactionGPOverlapsFactor:            0,
			//CompactionL0Trigger:                   0,
			//CompactionSourceLimitFactor:           0,
			//CompactionTableSize:                   0,
			//CompactionTableSizeMultiplier:         0,
			//CompactionTableSizeMultiplierPerLevel: nil,
			//CompactionTotalSize:                   0,
			//CompactionTotalSizeMultiplier:         0,
			//CompactionTotalSizeMultiplierPerLevel: nil,
			//Comparer:                              nil,
			//Compression:                           0,
			DisableBufferPool:            true,
			DisableBlockCache:            true,
			DisableCompactionBackoff:     false,
			DisableLargeBatchTransaction: false,
			//ErrorIfExist:                          false,
			//ErrorIfMissing:                        false,
			//Filter:                                nil,
			//IteratorSamplingRate:                  0,
			//NoSync:                                false,
			//NoWriteMerge:                          false,
			OpenFilesCacher: opt.NoCacher,
			//OpenFilesCacheCapacity:                0,
			//ReadOnly:                              false,
			//Strict:                                0,
			WriteBuffer: 1 * 1024 * 1024, // 1MB
			//WriteL0PauseTrigger:                   0,
			//WriteL0SlowdownTrigger:               0,
		})
	}
	return f
}

func (s *S3BackendStorage) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	randomUuid, _ := uuid.NewRandom()
	key = randomUuid.String()

	glog.V(0).Infof("copying dat file of %s to remote s3.%s as %s", f.Name(), s.id, key)

	util.Retry("upload to S3", func() error {
		size, err = uploadToS3(s.conn, f.Name(), s.bucket, key, s.storageClass, fn)
		return err
	})

	return
}

func (s *S3BackendStorage) DownloadFile(fileName string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {

	glog.V(0).Infof("download dat file of %s from remote s3.%s as %s", fileName, s.id, key)

	size, err = downloadFromS3(s.conn, fileName, s.bucket, key, fn)

	return
}

func (s *S3BackendStorage) DeleteFile(key string) (err error) {

	glog.V(0).Infof("delete dat file %s from remote", key)

	err = deleteFromS3(s.conn, s.bucket, key)

	return
}

type S3BackendStorageFile struct {
	backendStorage *S3BackendStorage
	key            string
	tierInfo       *volume_server_pb.VolumeInfo

	dbRef *LevelDBWeakref
}

type LevelDBWeakref struct {
	ldbPath    string
	ldbOptions *opt.Options

	dbMu sync.Mutex
	db   *leveldb.DB
}

func NewLevelDBWeakref(path string, options *opt.Options) *LevelDBWeakref {
	return &LevelDBWeakref{
		ldbPath:    path,
		ldbOptions: options,
	}
}

func (l *LevelDBWeakref) Lock() *leveldb.DB {
	db := l.ensureDB()
	if db != nil {
		l.dbMu.Lock()
	}
	return db
}

func (l *LevelDBWeakref) Unlock() {
	l.dbMu.Unlock()
}

func (l *LevelDBWeakref) TryRelease() (released bool) {
	if !l.dbMu.TryLock() {
		return false
	}
	defer l.dbMu.Unlock()

	db := l.db
	if db == nil {
		return false // already closed
	}
	l.db = nil
	glog.V(0).Infof("destroying s3 backend cache leveldb %s after timeout", l.ldbPath)
	err := db.Close()
	if err != nil {
		glog.V(0).Infof("failed to close leveldb: %v", err)
	}
	return true
}

func (l *LevelDBWeakref) ensureDB() *leveldb.DB {
	l.dbMu.Lock()
	defer l.dbMu.Unlock()
	if l.db != nil {
		return l.db
	}
	glog.V(0).Infof("opening s3 backend cache leveldb: %v", l.ldbPath)
	db, err := leveldb.OpenFile(l.ldbPath, l.ldbOptions)
	if err != nil {
		return nil
	}
	l.db = db
	go func() {
		time.Sleep(30 * time.Second)
		for l.db != nil {
			l.TryRelease()
			time.Sleep(30 * time.Second)
		}
	}()
	return l.db
}

func (s3backendStorageFile *S3BackendStorageFile) cacheGet(p []byte, off int64) (ok bool, n int, err error) {
	if len(p) > 2048 {
		return false, 0, nil
	}
	// only use cache for small chunk reading

	if s3backendStorageFile.dbRef == nil {
		return false, 0, nil
	}

	cachedb := s3backendStorageFile.dbRef.Lock()
	if cachedb == nil {
		return false, 0, nil
	}
	defer s3backendStorageFile.dbRef.Unlock()
	buffer_key := fmt.Sprintf("%d-%d", off, off+int64(len(p)))
	ret, err := cachedb.Get([]byte(buffer_key), nil)
	if err == nil {
		if len(ret) != len(p) {
			return true, 0, fmt.Errorf("invalid leveldb entry, should have length %d, got %d", len(p), len(ret))
		}
		copy(p, ret)
		return true, len(p), nil
	} else if err == leveldb.ErrNotFound {
		return false, 0, nil
	} else {
		return true, 0, fmt.Errorf("unexpected error during leveldb query: %w", err)
	}
}

func (s3backendStorageFile *S3BackendStorageFile) cacheSet(p []byte, off int64) {
	if len(p) > 2048 {
		return
	}
	if s3backendStorageFile.dbRef != nil {
		cachedb := s3backendStorageFile.dbRef.Lock()
		if cachedb == nil {
			return
		}
		defer s3backendStorageFile.dbRef.Unlock()

		buffer_key := fmt.Sprintf("%d-%d", off, off+int64(len(p)))
		err := cachedb.Put([]byte(buffer_key), p, nil)
		if err != nil {
			glog.V(0).Infof("cannot store new s3 cache entry: %w", err)
		}
	}
}

func (s3backendStorageFile S3BackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	glog.V(1).Infof("s3backendStorageFile ReadAt: off %d len %d", off, len(p))
	var useCache bool
	useCache, n, err = s3backendStorageFile.cacheGet(p, off)
	if useCache {
		return n, err
	}
	defer func() {
		if n > 0 && err == nil {
			s3backendStorageFile.cacheSet(p, off)
		}
	}()

	bytesRange := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)

	getObjectOutput, getObjectErr := s3backendStorageFile.backendStorage.conn.GetObject(&s3.GetObjectInput{
		Bucket: &s3backendStorageFile.backendStorage.bucket,
		Key:    &s3backendStorageFile.key,
		Range:  &bytesRange,
	})

	if getObjectErr != nil {
		return 0, fmt.Errorf("bucket %s GetObject %s: %v", s3backendStorageFile.backendStorage.bucket, s3backendStorageFile.key, getObjectErr)
	}
	defer getObjectOutput.Body.Close()

	// glog.V(3).Infof("read %s %s", s3backendStorageFile.key, bytesRange)
	// glog.V(3).Infof("content range: %s, contentLength: %d", *getObjectOutput.ContentRange, *getObjectOutput.ContentLength)

	var readCount int
	for {
		p = p[readCount:]
		readCount, err = getObjectOutput.Body.Read(p)
		n += readCount

		if err != nil {
			break
		}
	}

	if err == io.EOF {
		err = nil
	}

	return
}

func (s3backendStorageFile S3BackendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	panic("not implemented")
}

func (s3backendStorageFile S3BackendStorageFile) Truncate(off int64) error {
	panic("not implemented")
}

func (s3backendStorageFile S3BackendStorageFile) Close() error {
	return nil
}

func (s3backendStorageFile S3BackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {

	files := s3backendStorageFile.tierInfo.GetFiles()

	if len(files) == 0 {
		err = fmt.Errorf("remote file info not found")
		return
	}

	datSize = int64(files[0].FileSize)
	modTime = time.Unix(int64(files[0].ModifiedTime), 0)

	return
}

func (s3backendStorageFile S3BackendStorageFile) Name() string {
	return s3backendStorageFile.key
}

func (s3backendStorageFile S3BackendStorageFile) Sync() error {
	return nil
}
