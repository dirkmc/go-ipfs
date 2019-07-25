package coreunix

import (
	"context"
	"flag"
	filepath "path"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/repo"

	datastore "github.com/ipfs/go-datastore"
	// syncds "github.com/ipfs/go-datastore/sync"
	config "github.com/ipfs/go-ipfs-config"
	files "github.com/ipfs/go-ipfs-files"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	badgerds "github.com/ipfs/go-ds-badger"
	"github.com/pkg/profile"
)

const k = 1024
const M = k * k
const tmpPackageDir = "/tmp/sample-package"

// func TestAddDSCalls(t *testing.T) {
// 	fileSizes := []int{
// 		100 * M,
// 		10 * M,
// 		1 * M,
// 		100 * k,
// 		10 * k,
// 		5 * k,
// 		2 * k,
// 		1.5 * k,
// 		1.25 * k,
// 		k,
// 	}
// 	max := fileSizes[0]

// 	// itemsPerDirs := []int{ 2, 4, 8, 16, 32, 128, 256 }
// 	itemsPerDirs := []int{ 128 }

// 	fmt.Println("name\tHas\tGet\tPut\tObjects\tDuration")
// 	for _, fileSize := range fileSizes {
// 		fileCount := int(max / fileSize)
// 		// fmt.Println(formatName(fileSize, fileCount))
// 		for _, itemsPerDir := range itemsPerDirs {
// 			testAddDSCalls(t, fileSize, fileCount, itemsPerDir)
// 		}
// 		// fmt.Println()
// 	}
// }

// GO111MODULE=on go test -timeout 120m -count=1 -v ./core/coreunix/... -run TestAddDSCalls -args -itemsPerDir=32
var itemsPerDir *int = flag.Int("itemsPerDir", 32, "the items per dir")

func getTmpDirForFileCount(fileCount int) string {
	return tmpPackageDir + "/" + fmt.Sprintf("%dk", fileCount / k)
}

func TestAddDSCalls(t *testing.T) {
	fileCounts := []int{
		1 * k,
		// 256 * k,
		// 512 * k,
		// M,
		// 1.5 * M,
		// 2 * M,
		// 2.5 * M,
		// 3 * M,
		// 3.5 * M,
		// 4 * M,
		// 8 * M,
		// 16 * M,
	}

	// itemsPerDirs := []int{ 2, 4, 8, 16, 32, 128, 256 }
	// itemsPerDir := 64

	fmt.Printf("Running test with %d items per directory\n", *itemsPerDir)
	fmt.Println("File Count\tDuration")
	for _, fileCount := range fileCounts {
		// fmt.Println(formatName(fileSize, fileCount))
		tmpDirPath := getTmpDirForFileCount(fileCount)
		err := createFileTree(tmpDirPath, fileCount, *itemsPerDir)
		if err != nil {
			t.Fatal(err)
		}
		testAddDSCalls(t, fileCount, *itemsPerDir)
	}
}

func testAddDSCalls(t *testing.T, fileCount int, itemsPerDir int) {
	// name := formatName(fileSize, fileCount)
	// fmt.Println("	   Name		  Has	  Get	  Put   Objects")
	// for _, fileSize := range profiles {
		// stats := getDSCalls(t, fileSize, fileCount, fileCount)
		stats := getDSCalls(t, fileCount, itemsPerDir)
		// fmt.Printf("%14s:  %7d  %7d  %7d   %7d	%s\n", name, stats.Has, stats.Get, stats.Put, stats.objects, stats.elapsed)
		// fmt.Printf("%s\t%d\t%d\t%d\t%d\t%d\t%d\n", name, itemsPerDir, stats.Has, stats.Get, stats.Put, stats.objects, stats.elapsed)
		fmt.Printf("%d\t%d\n", fileCount, stats.elapsed / 1000000)
	// }
}

func formatName(fileSize int, fileCount int) string {
	sizeName := fmt.Sprintf("%.2fk", float64(fileSize) / k)
	if (fileSize >= M) {
		sizeName = fmt.Sprintf("%dM", fileSize / M)
	} else if (fileSize >= 2 * k) {
		sizeName = fmt.Sprintf("%dk", fileSize / k)
	}

	return fmt.Sprintf("%d x %s", fileCount, sizeName)
}

func getDSCalls(t *testing.T, fileCount int, itemsPerDir int) *addPerfStats {
	var stats = addPerfStats{}
	// apds := &addperfDatastore{MapDatastore: datastore.NewMapDatastore(), stats: &stats}
	// wrpds := syncds.MutexWrap(apds)
	path := fmt.Sprintf("/tmp/perf-test/badger-test-%s", time.Now())
	bdgds, err := badgerds.NewDatastore(path, &badgerds.DefaultOptions)
	if err != nil {
		t.Fatal(err)
	}

	r := &repo.Mock{
		C: config.Config{
			Identity: config.Identity{
				PeerID: testPeerID, // required by offline node
			},
		},
		// D: wrpds,
		D: bdgds,
	}
	node, err := core.NewNode(context.Background(), &core.BuildCfg{Repo: r})
	if err != nil {
		t.Fatal(err)
	}

	out := make(chan interface{})
	adder, err := NewAdder(context.Background(), node.Pinning, node.Blockstore, node.DAG)
	// adder, err := NewAdder(context.Background(), node.Pinning, node.Blockstore, dsrv)
	if err != nil {
		t.Fatal(err)
	}
	adder.Out = out

	tmpDirPath := getTmpDirForFileCount(fileCount)
	stat, err := os.Stat(tmpDirPath)
	if err != nil {
		t.Fatal(err)
	}
	slf, err := files.NewSerialFile(tmpDirPath, false, stat)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		defer close(out)
		start := time.Now()
		// p := profile.Start(profile.MemProfile)
		p := profile.Start()
		_, err := adder.AddAllAndPin(slf)
		p.Stop()
		stats.elapsed = time.Since(start)

		if err != nil {
			t.Error(err)
		}

	}()

	count := 0
	for o := range out {
		str := o.(*coreiface.AddEvent).Path.Cid().String()
		if (str != "") {
			count++
		}
		fmt.Println(str)
	}
	stats.objects = count

	return &stats
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func createFileTree(tmpDirPath string, fileCount int, itemsPerDir int) error {
	exists, err := fileExists(tmpDirPath)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	fmt.Println("Generating files for test " + tmpDirPath)

	root := makeTmpDir(tmpDirPath, nil, nil, "")
	current := root
	height := 1
	curdepth := 1

	for processed := 0; processed < fileCount; {
		// if this node is not yet full
		if current.Length() < itemsPerDir {
			// if we're at maximum depth
			if curdepth == height {
				// Create files in directory
				for i := 0; i < itemsPerDir && processed < fileCount; i++ {
					name := strconv.Itoa(processed)
					size := k
					bucket := rand.Float32()
					if bucket < 0.5 {
						size = (int)(k + rand.Int31n(M))
					} else if bucket < 0.55 {
						size = (int)(M + rand.Int31n(100 * M))
					}

					childpath := filepath.Join(current.path, name)
					data := randContent(size)
					err := ioutil.WriteFile(childpath, data, 0644)
					if err != nil {
						panic(err)
					}

					processed++
				}
			} else {
				// Add a child directory and recurse over child
				name := strconv.Itoa(current.Length())
				current = current.CreateChildDir(name)
				curdepth++
			}
		} else {
			// The node is full

			// If we're at the root
			if current == root {
				// Create a parent node and add the root as a child of it
				newRoot := makeTmpDir(tmpDirPath, nil, files.FileEntry("0", root), root.path)
				root = newRoot
				current = root
				height++
			} else {
				// Go up a level
				current = current.Parent()
				curdepth--
			}
		}
	}

	return root.Commit(tmpDirPath)
}

var randReader *rand.Rand

func newRandContentFile(len int) files.Node {
	return files.NewBytesFile(randContent(len))
}

func randContent(len int) []byte {
	if randReader == nil {
		randReader = rand.New(rand.NewSource(2))
	}
	data := make([]byte, len)
	randReader.Read(data)
	return data
}

type addPerfStats struct {
	Get uint
	Has uint
	Put uint
	elapsed time.Duration
	objects int
}

type addperfDatastore struct {
	*datastore.MapDatastore
	// *badgerds.Datastore
	stats *addPerfStats
}

// func (ds *addperfDatastore) Get(key datastore.Key) (value []byte, err error) {
// 	// fmt.Println("get")
// 	ds.stats.Get++
// 	return ds.Datastore.Get(key)
// }

// func (ds *addperfDatastore) Has(key datastore.Key) (exists bool, err error) {
// 	// fmt.Println("has")
// 	ds.stats.Has++
// 	return ds.Datastore.Has(key)
// }

// func (ds *addperfDatastore) Put(key datastore.Key, value []byte) error {
// 	// fmt.Printf("put %d\n", len(value))
// 	ds.stats.Put++
// 	return ds.Datastore.Put(key, value)
// }

func (ds *addperfDatastore) Batch() (datastore.Batch, error) {
	return datastore.NewBasicBatch(ds), nil
}

func (ds *addperfDatastore) Get(key datastore.Key) (value []byte, err error) {
	// fmt.Println("get")
	ds.stats.Get++
	return ds.MapDatastore.Get(key)
}

func (ds *addperfDatastore) Has(key datastore.Key) (exists bool, err error) {
	// fmt.Println("has")
	ds.stats.Has++
	return ds.MapDatastore.Has(key)
}

func (ds *addperfDatastore) Put(key datastore.Key, value []byte) error {
	// fmt.Printf("put %d\n", len(value))
	ds.stats.Put++
	return ds.MapDatastore.Put(key, value)
}

func writeTreeToDisk(root files.Directory, depth int, path string) {
	it := root.Entries()

	_ = os.Mkdir(path, 0700)

	for it.Next() {
		for i := 0; i < depth * 2; i++ {
			fmt.Printf(" ")
		}

		fmt.Println(it.Name())
		childpath := filepath.Join(path, it.Name())

		if d, ok := it.Node().(files.Directory); ok {
			writeTreeToDisk(d, depth + 1, childpath)
		} else if f, ok := it.Node().(files.File); ok {
			data, err := ioutil.ReadAll(f)
			if err != nil {
				panic(err)
			}
			err = ioutil.WriteFile(childpath, data, 0644)
			if err != nil {
				panic(err)
			}
		}
	}
}

type directory struct {
	parent *directory
	// files []files.DirEntry
	path string
}

func makeDir(path string, parent *directory) *directory {
	err := os.Mkdir(path, 0700)
	if err != nil {
		panic(err)
	}
	return &directory{ parent: parent, path: path }
}

func makeTmpDir(path string, parent *directory, firstChild files.DirEntry, oldChildPath string) *directory {
	os.Mkdir(path, 0700)
	tmpPath := filepath.Join(path, strconv.Itoa(rand.Intn(k * M)))
	err := os.Mkdir(tmpPath, 0700)
	if err != nil {
		panic(err)
	}
	if firstChild != nil {
		childPath := filepath.Join(tmpPath, firstChild.Name())
		err := os.Rename(oldChildPath, childPath)
		if err != nil {
			panic(err)
		}
	}
	return &directory{ parent, tmpPath }
}

func (d *directory) Commit(path string) error {
	tmpPath := path + ".tmp"
	err := os.Rename(path, tmpPath)
	if err != nil {
		return err
	}
	err = os.Rename(strings.Replace(d.path, path, tmpPath, 1), path)
	if err != nil {
		return err
	}
	return os.Remove(tmpPath)
}

func (d *directory) CreateChildDir(name string) *directory {
	return makeDir(filepath.Join(d.path, name), d)
}

func (d *directory) Append(file files.DirEntry) {
	// d.files = append(d.files, file)
	// if subdir, ok := file.Node().(*directory); ok {
	// 	subdir.parent = d
	// }
}

// func (d *directory) CreateChildDir(name string) *directory {
// 	child := makeDir(d)
// 	d.Append(files.FileEntry(name, child))
// 	return child
// }

func (d *directory) Parent() *directory {
	return d.parent
}

func (d *directory) Close() error {
	return nil
}

func (d *directory) Length() int {
	files, err := ioutil.ReadDir(d.path)
	if err != nil {
		panic(err)
	}
	return len(files)
	// return len(d.files)
}

func (d *directory) Entries() files.DirIterator {
	// return &sliceIterator{files: d.files, n: -1}
	return &sliceIterator{files: []files.DirEntry{}, n: -1}
}

func (d *directory) Size() (int64, error) {
	return 0, nil
}

type sliceIterator struct {
	files []files.DirEntry
	n	 int
}

func (it *sliceIterator) Name() string {
	return it.files[it.n].Name()
}

func (it *sliceIterator) Node() files.Node {
	return it.files[it.n].Node()
}

func (it *sliceIterator) Next() bool {
	it.n++
	return it.n < len(it.files)
}

func (it *sliceIterator) Err() error {
	return nil
}

var _ files.Directory = &directory{}
