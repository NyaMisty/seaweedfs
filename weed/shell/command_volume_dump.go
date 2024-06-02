package shell

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"golang.org/x/sync/errgroup"
	"io"
	"math"
	"os"
	"path"
	"sync"
	"time"
)

func init() {
	Commands = append(Commands, &commandVolumeDump{})
}

type commandVolumeDump struct {
	env        *CommandEnv
	writer     io.Writer
	collection *string
	volumeIds  map[uint32]bool
	tempFolder string

	verbose *bool
}

func (c *commandVolumeDump) Name() string {
	return "volume.dump"
}

func (c *commandVolumeDump) Help() string {
	return `check all volumes to find entries not used by the filer

	Important assumption!!!
		the system is all used by one filer.

	This command works this way:
	1. collect all file ids from all volumes, as set A
	2. collect all file ids from the filer, as set B
	3. find out the set A subtract B

	If -findMissingChunksInFiler is enabled, this works
	in a reverse way:
	1. collect all file ids from all volumes, as set A
	2. collect all file ids from the filer, as set B
	3. find out the set B subtract A

`
}

func (c *commandVolumeDump) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	dumpCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	c.verbose = dumpCommand.Bool("v", false, "verbose mode")
	concurrency := dumpCommand.Int("concurrency", 150, "concurrency threads")
	c.collection = dumpCommand.String("collection", "", "the collection name")
	tempPath := dumpCommand.String("tempPath", path.Join(os.TempDir()), "path for temporary idx files")
	cutoffTimeAgo := dumpCommand.Duration("cutoffTimeAgo", 5*time.Minute, "only include entries  on volume servers before this cutoff time to check orphan chunks")
	modifyTimeAgo := dumpCommand.Duration("modifyTimeAgo", 0, "only include entries after this modify time to check orphan chunks")

	if err = dumpCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}
	c.volumeIds = make(map[uint32]bool)

	c.env = commandEnv
	c.writer = writer

	// create a temp folder
	c.tempFolder, err = os.MkdirTemp(*tempPath, "sw_fsck")
	if err != nil {
		return fmt.Errorf("failed to create temp folder: %v", err)
	}
	if *c.verbose {
		fmt.Fprintf(c.writer, "working directory: %s\n", c.tempFolder)
	}
	defer os.RemoveAll(c.tempFolder)

	// collect all volume id locations
	dataNodeVolumeIdToVInfo, err := c.collectVolumeIds()
	if err != nil {
		return fmt.Errorf("failed to collect all volume locations: %v", err)
	}

	collectCutoffFromAtNs := time.Now().Add(-*cutoffTimeAgo).UnixNano()
	var collectModifyFromAtNs int64 = 0
	if modifyTimeAgo.Seconds() != 0 {
		collectModifyFromAtNs = time.Now().Add(-*modifyTimeAgo).UnixNano()
	}

	// collect each volume file ids
	fidMap := &sync.Map{}
	_ = fidMap

	eg, gCtx := errgroup.WithContext(context.Background())
	_ = gCtx
	eg.SetLimit(*concurrency)
	maps := &sync.Map{}
	for _dataNodeId, _volumeIdToVInfo := range dataNodeVolumeIdToVInfo {
		dataNodeId, volumeIdToVInfo := _dataNodeId, _volumeIdToVInfo
		for _volumeId, _vinfo := range volumeIdToVInfo {
			volumeId, vinfo := _volumeId, _vinfo
			if len(c.volumeIds) > 0 {
				if _, ok := c.volumeIds[volumeId]; !ok {
					delete(volumeIdToVInfo, volumeId)
					continue
				}
			}
			if *c.collection != "" && vinfo.collection != *c.collection {
				delete(volumeIdToVInfo, volumeId)
				continue
			}
			eg.Go(func() error {
				err = c.collectOneVolumeFileIds(dataNodeId, volumeId, vinfo, uint64(collectModifyFromAtNs), uint64(collectCutoffFromAtNs))
				if err != nil {
					return fmt.Errorf("failed to collect file ids from volume %d on %s: %v", volumeId, vinfo.server, err)
				}
				curMap := &sync.Map{}
				err = c.readVolumeNeedles(curMap, dataNodeId, volumeId)
				if err != nil {
					return fmt.Errorf("failed to read file ids from volume %d on %s: %v", volumeId, vinfo.server, err)
				}
				maps.Store(fmt.Sprintf("%v-%v", dataNodeId, volumeId), curMap)
				return nil
			})
		}
		if *c.verbose {
			fmt.Fprintf(c.writer, "dn %+v filtred %d volumes and locations.\n", dataNodeId, len(dataNodeVolumeIdToVInfo[dataNodeId]))
		}
	}

	err = eg.Wait()
	if err != nil {
		fmt.Fprintf(c.writer, "cannot collect fid map: %v", err)
		return err
	}

	fidJsonMap := map[string]FidInfo{}
	maps.Range(func(key, value any) bool {
		mm := value.(*sync.Map)
		mm.Range(func(key, value any) bool {
			fidJsonMap[key.(string)] = value.(FidInfo)
			return true
		})
		return true
	})
	//fidMap.Range(func(key, value any) bool {
	//	fidJsonMap[key.(uint64)] = value.(FidInfo)
	//	return true
	//})
	fidJson, err := json.Marshal(fidJsonMap)
	if err != nil {
		fmt.Fprintf(c.writer, "cannot marshal fid map: %v", err)
		return err
	}
	fmt.Fprintln(c.writer, string(fidJson))
	return nil
}

type FidInfo struct {
	DataNode string
	Vid      uint32
	//FileKey  types.NeedleId
	//Size     types.Size
	//Offset   types.Offset
	FileKey uint64
	Size    int32
	Offset  int64
}

func (f *FidInfo) String() string {
	return fmt.Sprintf("[Fid %v %v %x %v]", f.DataNode, f.Vid, f.FileKey, f.Size)
}

func (c *commandVolumeDump) readVolumeNeedles(m *sync.Map, dataNodeId string, volumeId uint32) (err error) {
	volumeFileIdDb := needle_map.NewMemDb()
	defer volumeFileIdDb.Close()

	if err = volumeFileIdDb.LoadFromIdx(getVolumeFileIdFile(c.tempFolder, dataNodeId, volumeId)); err != nil {
		err = fmt.Errorf("failed to LoadFromIdx %+v", err)
		return
	}
	return volumeFileIdDb.AscendingVisit(func(value needle_map.NeedleValue) error {
		curInfo := FidInfo{
			DataNode: dataNodeId,
			Vid:      volumeId,
			FileKey:  uint64(value.Key),
			Size:     int32(value.Size),
			Offset:   value.Offset.ToActualOffset(),
		}
		key := fmt.Sprintf("%v-%v", curInfo.FileKey, curInfo.Size)
		//if _existingInfo, loaded := m.LoadOrStore(key, curInfo); loaded {
		//	existingInfo := _existingInfo.(FidInfo)
		//	if existingInfo.Vid != curInfo.Vid {
		//		fmt.Fprintf(c.writer, "got duplicate fid: %s vs %s\n", curInfo.String(), existingInfo.String())
		//		return nil
		//	}
		//	return nil
		//}
		m.Store(key, curInfo)
		return nil
	})
}

func (c *commandVolumeDump) collectOneVolumeFileIds(dataNodeId string, volumeId uint32, vinfo VInfo, modifyFrom uint64, cutoffFrom uint64) error {

	if *c.verbose {
		fmt.Fprintf(c.writer, "collecting volume %d file ids from %s ...\n", volumeId, vinfo.server)
	}

	return operation.WithVolumeServerClient(false, vinfo.server, c.env.option.GrpcDialOption,
		func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			ext := ".idx"
			if vinfo.isEcVolume {
				ext = ".ecx"
			}

			copyFileClient, err := volumeServerClient.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
				VolumeId:                 volumeId,
				Ext:                      ext,
				CompactionRevision:       math.MaxUint32,
				StopOffset:               math.MaxInt64,
				Collection:               vinfo.collection,
				IsEcVolume:               vinfo.isEcVolume,
				IgnoreSourceFileNotFound: false,
			})
			if err != nil {
				return fmt.Errorf("failed to start copying volume %d%s: %v", volumeId, ext, err)
			}

			var buf bytes.Buffer
			for {
				resp, err := copyFileClient.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return err
				}
				buf.Write(resp.FileContent)
			}
			if false && !vinfo.isReadOnly {
				index, err := idx.FirstInvalidIndex(buf.Bytes(),
					func(key types.NeedleId, offset types.Offset, size types.Size) (bool, error) {
						resp, err := volumeServerClient.ReadNeedleMeta(context.Background(), &volume_server_pb.ReadNeedleMetaRequest{
							VolumeId: volumeId,
							NeedleId: uint64(key),
							Offset:   offset.ToActualOffset(),
							Size:     int32(size),
						})
						if err != nil {
							return false, fmt.Errorf("read needle meta with id %d from volume %d: %v", key, volumeId, err)
						}
						if (modifyFrom == 0 || modifyFrom <= resp.AppendAtNs) && (resp.AppendAtNs <= cutoffFrom) {
							return true, nil
						}
						return false, nil
					})
				if err != nil {
					fmt.Fprintf(c.writer, "Failed to search for last valid index on volume %d with error %v\n", volumeId, err)
				} else {
					buf.Truncate(index * types.NeedleMapEntrySize)
				}
			}
			idxFilename := getVolumeFileIdFile(c.tempFolder, dataNodeId, volumeId)
			err = writeToFile(buf.Bytes(), idxFilename)
			if err != nil {
				return fmt.Errorf("failed to copy %d%s from %s: %v", volumeId, ext, vinfo.server, err)
			}

			return nil
		})

}

func (c *commandVolumeDump) readFilerFileIdFile(volumeId uint32, fn func(needleId types.NeedleId, itemPath util.FullPath)) error {
	fp, err := os.Open(getFilerFileIdFile(c.tempFolder, volumeId))
	if err != nil {
		return err
	}
	defer fp.Close()

	br := bufio.NewReader(fp)
	buffer := make([]byte, readbufferSize)
	var readSize int
	var readErr error
	item := &Item{vid: volumeId}
	for {
		readSize, readErr = io.ReadFull(br, buffer)
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
		if readSize != readbufferSize {
			return fmt.Errorf("readSize mismatch")
		}
		item.fileKey = util.BytesToUint64(buffer[:8])
		item.cookie = util.BytesToUint32(buffer[8:12])
		pathSize := util.BytesToUint32(buffer[12:16])
		pathBytes := make([]byte, int(pathSize))
		n, err := io.ReadFull(br, pathBytes)
		if err != nil {
			fmt.Fprintf(c.writer, "%d,%x%08x in unexpected error: %v\n", volumeId, item.fileKey, item.cookie, err)
		}
		if n != int(pathSize) {
			fmt.Fprintf(c.writer, "%d,%x%08x %d unexpected file name size %d\n", volumeId, item.fileKey, item.cookie, pathSize, n)
		}
		item.path = util.FullPath(pathBytes)
		needleId := types.NeedleId(item.fileKey)
		fn(needleId, item.path)
	}
	return nil
}

func (c *commandVolumeDump) collectVolumeIds() (volumeIdToServer map[string]map[uint32]VInfo, err error) {

	if *c.verbose {
		fmt.Fprintf(c.writer, "collecting volume id and locations from master ...\n")
	}

	volumeIdToServer = make(map[string]map[uint32]VInfo)
	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(c.env, 0)
	if err != nil {
		return
	}

	eachDataNode(topologyInfo, func(dc string, rack RackId, t *master_pb.DataNodeInfo) {
		for _, diskInfo := range t.DiskInfos {
			dataNodeId := t.GetId()
			volumeIdToServer[dataNodeId] = make(map[uint32]VInfo)
			for _, vi := range diskInfo.VolumeInfos {
				volumeIdToServer[dataNodeId][vi.Id] = VInfo{
					server:     pb.NewServerAddressFromDataNode(t),
					collection: vi.Collection,
					isEcVolume: false,
					isReadOnly: vi.ReadOnly,
				}
			}
			for _, ecShardInfo := range diskInfo.EcShardInfos {
				volumeIdToServer[dataNodeId][ecShardInfo.Id] = VInfo{
					server:     pb.NewServerAddressFromDataNode(t),
					collection: ecShardInfo.Collection,
					isEcVolume: true,
					isReadOnly: true,
				}
			}
			if *c.verbose {
				fmt.Fprintf(c.writer, "dn %+v collected %d volumes and locations.\n", dataNodeId, len(volumeIdToServer[dataNodeId]))
			}
		}
	})
	return
}
