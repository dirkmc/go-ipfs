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
)

const k = 1024
const M = k * k

func TestAddDSCalls(t *testing.T) {
	profiles := []int{
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

	max := 100 * M
	fmt.Println("       Name          Has      Get      Put   Objects")
	for _, fileSize := range profiles {
		dirSize := int(max / fileSize)
		calls := getDSCalls(t, fileSize, dirSize)
		name := formatName(fileSize, dirSize)
		fmt.Printf("%14s:  %7d  %7d  %7d   %7d    %s\n", name, calls.Has, calls.Get, calls.Put, calls.objects, calls.elapsed)
	}
}

func formatName(fileSize int, dirSize int) string {
	sizeName := fmt.Sprintf("%.1fk", float64(fileSize) / k)
	if (fileSize >= M) {
		sizeName = fmt.Sprintf("%dM", fileSize / M)
	} else if (fileSize >= k) {
		sizeName = fmt.Sprintf("%dk", fileSize / k)
	}

	return fmt.Sprintf("%d x %s", dirSize, sizeName)
}

func getDSCalls(t *testing.T, fileSize int, dirSize int) *addPerfCalls {
	apds := &addperfDatastore{MapDatastore: datastore.NewMapDatastore()}
	wrpds := syncds.MutexWrap(apds)
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

	mapDir := make(map[string]files.Node)
	for i := 0; i < dirSize; i++ {
		mapDir[strconv.Itoa(len(mapDir))] = newRandContentFile(fileSize)
	}

	slf := files.NewMapDirectory(mapDir)

	go func() {
		defer close(out)
		start := time.Now()
		_, err := adder.AddAllAndPin(slf)
		apds.calls.elapsed = time.Since(start)

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
	apds.calls.objects = count

	return &apds.calls
}

func newRandContentFile(len int) files.Node {
	data := make([]byte, len)
	rand.Read(data)
	return files.NewBytesFile(data)
}

type addPerfCalls struct {
	Get uint
	Has uint
	Put uint
	elapsed time.Duration
	objects int
}

type addperfDatastore struct {
	*datastore.MapDatastore
	calls addPerfCalls
}

func (ds *addperfDatastore) Batch() (datastore.Batch, error) {
	return datastore.NewBasicBatch(ds), nil
}

func (ds *addperfDatastore) Get(key datastore.Key) (value []byte, err error) {
	// fmt.Println("get")
	ds.calls.Get++
	return ds.MapDatastore.Get(key)
}

func (ds *addperfDatastore) Has(key datastore.Key) (exists bool, err error) {
	// fmt.Println("has")
	ds.calls.Has++
	return ds.MapDatastore.Has(key)
}

func (ds *addperfDatastore) Put(key datastore.Key, value []byte) error {
	// fmt.Printf("put %d\n", len(value))
	ds.calls.Put++
	return ds.MapDatastore.Put(key, value)
}
