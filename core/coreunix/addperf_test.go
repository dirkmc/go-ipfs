package coreunix

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/repo"

	datastore "github.com/ipfs/go-datastore"
	syncds "github.com/ipfs/go-datastore/sync"
	config "github.com/ipfs/go-ipfs-config"
	files "github.com/ipfs/go-ipfs-files"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	// badgerds "github.com/ipfs/go-ds-badger"
)

const k = 1024
const M = k * k

func TestAddDSCalls(t *testing.T) {
	fileSizes := []int{
		100 * M,
		10 * M,
		1 * M,
		100 * k,
		10 * k,
		5 * k,
		2 * k,
		1.5 * k,
		1.25 * k,
		k,
	}
	max := fileSizes[0]

	itemsPerDirs := []int{ 2, 4, 8, 16, 32, 128, 256 }

	fmt.Println("items/dir\tHas\tGet\tPut\tObjects\tDuration")
	for _, fileSize := range fileSizes {
		fileCount := int(max / fileSize)
		fmt.Println(formatName(fileSize, fileCount))
		for _, itemsPerDir := range itemsPerDirs {
			testAddDSCalls(t, fileSize, fileCount, itemsPerDir)
		}
		fmt.Println()
	}
}

func testAddDSCalls(t *testing.T, fileSize int, fileCount int, itemsPerDir int) {
	// fmt.Println("       Name          Has      Get      Put   Objects")
	// for _, fileSize := range profiles {
		// stats := getDSCalls(t, fileSize, fileCount, fileCount)
		stats := getDSCalls(t, fileSize, fileCount, itemsPerDir)
		// fmt.Printf("%14s:  %7d  %7d  %7d   %7d    %s\n", name, stats.Has, stats.Get, stats.Put, stats.objects, stats.elapsed)
		fmt.Printf("%d\t%d\t%d\t%d\t%d\t%d\n", itemsPerDir, stats.Has, stats.Get, stats.Put, stats.objects, stats.elapsed)
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

func getDSCalls(t *testing.T, fileSize int, fileCount int, itemsPerDir int) *addPerfStats {
	var stats = addPerfStats{}
	apds := &addperfDatastore{MapDatastore: datastore.NewMapDatastore(), stats: &stats}
	wrpds := syncds.MutexWrap(apds)
	// path := fmt.Sprintf("/tmp/perf-test/badger-test-%s", time.Now())
	// bdgds, err := badgerds.NewDatastore(path, &badgerds.DefaultOptions)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// wrpds := &addperfDatastore{MapDatastore: apds, stats: &stats}

	r := &repo.Mock{
		C: config.Config{
			Identity: config.Identity{
				PeerID: testPeerID, // required by offline node
			},
		},
		D: wrpds,
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

	slf := createInMemFileTree(fileSize, fileCount, itemsPerDir)

	// printTree(slf, 0)

	go func() {
		defer close(out)
		start := time.Now()
		_, err := adder.AddAllAndPin(slf)
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
		// fmt.Println(o.(*coreiface.AddEvent).Path.Cid().String())
	}
	stats.objects = count

	return &stats
}

func createInMemFileTree(fileSize int, fileCount int, itemsPerDir int) files.Directory {
	root := makeDir(nil)
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
					file := newRandContentFile(fileSize)
					current.Append(files.FileEntry(name, file))
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
				newRoot := makeDir(nil)
				newRoot.Append(files.FileEntry("0", root))
				root = newRoot
				height++
			} else {
				curdepth--
			}

			// Go up a level
			current = current.Parent()
		}
	}

	return root
}

func newRandContentFile(len int) files.Node {
	data := make([]byte, len)
	rand.Read(data)
	return files.NewBytesFile(data)
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

func printTree(root files.Directory, depth int) {
	it := root.Entries()

	for it.Next() {
		for i := 0; i < depth * 2; i++ {
			fmt.Printf(" ")
		}
		fmt.Println(it.Name())
		if d, ok := it.Node().(files.Directory); ok {
			printTree(d, depth + 1)
		}
	}
}

type directory struct {
	parent *directory
	files []files.DirEntry
}

func makeDir(parent *directory) *directory {
	return &directory{ parent: parent }
}

func (d *directory) Append(file files.DirEntry) {
	d.files = append(d.files, file)
	if subdir, ok := file.Node().(*directory); ok {
		subdir.parent = d
	}
}

func (d *directory) CreateChildDir(name string) *directory {
	child := makeDir(d)
	d.Append(files.FileEntry(name, child))
	return child
}

func (d *directory) Parent() *directory {
	return d.parent
}

func (d *directory) Close() error {
	return nil
}

func (d *directory) Length() int {
	return len(d.files)
}

func (d *directory) Entries() files.DirIterator {
	return &sliceIterator{files: d.files, n: -1}
}

func (d *directory) Size() (int64, error) {
	var size int64

	for _, file := range d.files {
		s, err := file.Node().Size()
		if err != nil {
			return 0, err
		}
		size += s
	}

	return size, nil
}

type sliceIterator struct {
	files []files.DirEntry
	n     int
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
