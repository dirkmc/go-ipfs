package coreunix

import (
	"context"
	"errors"
	"fmt"
	"io"
	gopath "path"
	"strconv"
	"time"

	"github.com/ipfs/go-ipfs/pin"

	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	"github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/go-ipfs-posinfo"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	dag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-mfs"
	"github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipfs/go-unixfs/importer/trickle"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	ipfspath "github.com/ipfs/interface-go-ipfs-core/path"
	uio "github.com/ipfs/go-unixfs/io"
)

var log = logging.Logger("coreunix")

// how many bytes of progress to wait before sending a progress update message
const progressReaderIncrement = 1024 * 256

const fileQueueConcurrency = 8

var liveCacheSize = uint64(256 << 10)

type Link struct {
	Name, Hash string
	Size       uint64
}

// NewAdder Returns a new Adder used for a file add operation.
func NewAdder(ctx context.Context, p pin.Pinner, bs bstore.GCLocker, ds ipld.DAGService) (*Adder, error) {
	bufferedDS := ipld.NewBufferedDAG(ctx, ds)

	a := &Adder{
		ctx:        ctx,
		pinning:    p,
		gcLocker:   bs,
		dagService: ds,
		bufferedDS: bufferedDS,
		Progress:   false,
		Pin:        true,
		Trickle:    false,
		Chunker:    "",
		addt: 0,
	}

	a.fileQueue = newFileQueue(ctx, fileQueueConcurrency, a)

	return a, nil
}

// Adder holds the switches passed to the `add` command.
type Adder struct {
	ctx        context.Context
	pinning    pin.Pinner
	gcLocker   bstore.GCLocker
	dagService ipld.DAGService
	bufferedDS *ipld.BufferedDAG
	Out        chan<- interface{}
	Progress   bool
	Pin        bool
	Trickle    bool
	RawLeaves  bool
	Silent     bool
	NoCopy     bool
	Chunker    string
	mroot      *mfs.Root
	unlocker   bstore.Unlocker
	tempRoot   cid.Cid
	CidBuilder cid.Builder
	liveNodes  uint64
	fileQueue  *fileQueue
	addt int64
}

func (adder *Adder) mfsRoot() (*mfs.Root, error) {
	if adder.mroot != nil {
		return adder.mroot, nil
	}
	rnode := unixfs.EmptyDirNode()
	rnode.SetCidBuilder(adder.CidBuilder)
	mr, err := mfs.NewRoot(adder.ctx, adder.dagService, rnode, nil)
	if err != nil {
		return nil, err
	}
	adder.mroot = mr
	return adder.mroot, nil
}

// SetMfsRoot sets `r` as the root for Adder.
func (adder *Adder) SetMfsRoot(r *mfs.Root) {
	adder.mroot = r
}

// Constructs a node from reader's data, and adds it. Doesn't pin.
func (adder *Adder) add(reader io.Reader) (ipld.Node, error) {
	chnk, err := chunker.FromString(reader, adder.Chunker)
	if err != nil {
		return nil, err
	}

bds := ipld.NewBufferedDAG(adder.ctx, adder.dagService)

	start := time.Now()
	params := ihelper.DagBuilderParams{
		// Dagserv:	adder.bufferedDS,
		Dagserv:	bds,
		RawLeaves:  adder.RawLeaves,
		Maxlinks:   ihelper.DefaultLinksPerBlock,
		NoCopy:     adder.NoCopy,
		CidBuilder: adder.CidBuilder,
	}

	db, err := params.New(chnk)
	if err != nil {
		return nil, err
	}
	var nd ipld.Node
	if adder.Trickle {
		nd, err = trickle.Layout(db)
	} else {
		nd, err = balanced.Layout(db)
	}
	if err != nil {
		return nil, err
	}

	// e := adder.bufferedDS.Commit()
	e := bds.Commit()
	adder.addt += int64(time.Since(start))
	return nd, e
	// return nd, adder.bufferedDS.Commit()
}

// RootNode returns the mfs root node
func (adder *Adder) curRootNode() (ipld.Node, error) {
	mr, err := adder.mfsRoot()
	if err != nil {
		return nil, err
	}
	root, err := mr.GetDirectory().GetNode()
	if err != nil {
		return nil, err
	}

	// if one root file, use that hash as root.
	if len(root.Links()) == 1 {
		nd, err := root.Links()[0].GetNode(adder.ctx, adder.dagService)
		if err != nil {
			return nil, err
		}

		root = nd
	}

	return root, err
}

// Recursively pins the root node of Adder and
// writes the pin state to the backing datastore.
func (adder *Adder) PinRoot(root ipld.Node) error {
	if !adder.Pin {
		return nil
	}

	rnk := root.Cid()

	err := adder.dagService.Add(adder.ctx, root)
	if err != nil {
		return err
	}

	if adder.tempRoot.Defined() {
		err := adder.pinning.Unpin(adder.ctx, adder.tempRoot, true)
		if err != nil {
			return err
		}
		adder.tempRoot = rnk
	}

	adder.pinning.PinWithMode(rnk, pin.Recursive)
	return adder.pinning.Flush()
}

func (adder *Adder) outputDirs(path string, fsn mfs.FSNode) error {
	switch fsn := fsn.(type) {
	case *mfs.File:
		return nil
	case *mfs.Directory:
		names, err := fsn.ListNames(adder.ctx)
		if err != nil {
			return err
		}

		for _, name := range names {
			child, err := fsn.Child(name)
			if err != nil {
				return err
			}

			childpath := gopath.Join(path, name)
			err = adder.outputDirs(childpath, child)
			if err != nil {
				return err
			}

			fsn.Uncache(name)
		}
		nd, err := fsn.GetNode()
		if err != nil {
			return err
		}

		return outputDagnode(adder.Out, path, nd)
	default:
		return fmt.Errorf("unrecognized fsn type: %#v", fsn)
	}
}

func (adder *Adder) addNode(node ipld.Node, path string) error {
	// patch it into the root
	if path == "" {
		path = node.Cid().String()
	}

	if pi, ok := node.(*posinfo.FilestoreNode); ok {
		node = pi.Node
	}

	// mr, err := adder.mfsRoot()
	// if err != nil {
	// 	return err
	// }
	// dir := gopath.Dir(path)
	// if dir != "." {
	// 	opts := mfs.MkdirOpts{
	// 		Mkparents:  true,
	// 		Flush:      false,
	// 		CidBuilder: adder.CidBuilder,
	// 	}
	// 	if err := mfs.Mkdir(mr, dir, opts); err != nil {
	// 		return err
	// 	}
	// }

	// if err := mfs.PutNode(mr, path, node); err != nil {
	// 	return err
	// }

	if !adder.Silent {
		return outputDagnode(adder.Out, path, node)
	}
	return nil
}

// AddAllAndPin adds the given request's files and pin them.
func (adder *Adder) AddAllAndPin(file files.Node) (ipld.Node, error) {
	if adder.Pin {
		adder.unlocker = adder.gcLocker.PinLock()
	}
	defer func() {
		if adder.unlocker != nil {
			adder.unlocker.Unlock()
		}
	}()

	start := time.Now()
	// if err := adder.addFileNode("", file, true); err != nil {
	childrenChan := make(chan *ipldFileNode)
	err := adder.walkFileNode(file, "", "", childrenChan)
	if err != nil {
		return nil, err
	}

	c := <- childrenChan
	close(childrenChan)
	nd := c.Node
	cumulativet := int64(time.Since(start))
fmt.Printf("%d milli-seconds add\n", adder.addt / 1000000)
fmt.Printf("%d milli-seconds cumulative\n", cumulativet / 1000000)

	// // get root
	// mr, err := adder.mfsRoot()
	// if err != nil {
	// 	return nil, err
	// }
	// var root mfs.FSNode
	// rootdir := mr.GetDirectory()
	// root = rootdir

	// err = root.Flush()
	// if err != nil {
	// 	return nil, err
	// }

	// // if adding a file without wrapping, swap the root to it (when adding a
	// // directory, mfs root is the directory)
	// _, dir := file.(files.Directory)
	// var name string
	// if !dir {
	// 	children, err := rootdir.ListNames(adder.ctx)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	if len(children) == 0 {
	// 		return nil, fmt.Errorf("expected at least one child dir, got none")
	// 	}

	// 	// Replace root with the first child
	// 	name = children[0]
	// 	root, err = rootdir.Child(name)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	// err = mr.Close()
	// if err != nil {
	// 	return nil, err
	// }

	// nd, err := root.GetNode()
	// if err != nil {
	// 	return nil, err
	// }

	// // output directory events
	// err = adder.outputDirs(name, root)
	// if err != nil {
	// 	return nil, err
	// }
	if !adder.Pin {
		return nd, nil
	}
	return nd, adder.PinRoot(nd)
}

// func (adder *Adder) addFileNode(path string, file files.Node, toplevel bool) error {
func (adder *Adder) walkFileNode(file files.Node, name string, path string, parentChan chan *ipldFileNode) error {
	// err := adder.maybePauseForGC()
	// if err != nil {
	// 	return err
	// }

	// if adder.liveNodes >= liveCacheSize {
	// 	// TODO: A smarter cache that uses some sort of lru cache with an eviction handler
	// 	mr, err := adder.mfsRoot()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if err := mr.FlushMemFree(adder.ctx); err != nil {
	// 		return err
	// 	}

	// 	adder.liveNodes = 0
	// }
	// adder.liveNodes++

	switch f := file.(type) {
	case files.Directory:
		// return adder.addDir(path, f, toplevel)
		return adder.walkDir(f, name, path, parentChan)
	case *files.Symlink:
		// return adder.addSymlink(path, f)
		adder.fileQueue.enqueue(f, name, path, parentChan)
		return nil
	case files.File:
		// return adder.addFile(path, f)
		adder.fileQueue.enqueue(f, name, path, parentChan)
		return nil
	default:
		return errors.New("unknown file type")
	}
}

type ipldFileNode struct {
	Node ipld.Node
	Name string
	Path string
	IsDir bool
}

type dirPathEntry struct {
	Node files.Node
	Name string
	Path string
}

func (adder *Adder) walkDir(dir files.Directory, name string, path string, parentChan chan<- *ipldFileNode) error {
	// fmt.Printf("walkDir %s\n", path)
	childrenChan := make(chan *ipldFileNode, 1)
	entryCountChan := make(chan int, 1)

	go func () {
		var children []ipldFileNode

		entryCount := -1
		for ; entryCount == -1 || len(children) < entryCount ; {
			select {
				// TODO: Does entryCount need to be an atomic or is it
				// guaranteed to be safe because we're reading from a channel?
		        case entryCount = <- entryCountChan:
		        case c := <- childrenChan:
					// fmt.Printf("c %s\n", c.Path)
					if !c.IsDir {
						outputDagnode(adder.Out, c.Path, c.Node)
					}
					children = append(children, *c)
					// fmt.Printf("%s %d\n", path, i)
	        }
		}
		close(childrenChan)

		d, err := adder.addDir(path, children)
		if err != nil {
			panic(fmt.Sprintf("got error adding dir %s %v", path, err))
		}
		// fmt.Printf("doneDir %s\n", path)
		outputDagnode(adder.Out, path, d)
		parentChan <- &ipldFileNode{ d, name, path, true }
	}()

	count := 0
	it := dir.Entries()
	for it.Next() {
		fpath := gopath.Join(path, it.Name())
		err := adder.walkFileNode(it.Node(), it.Name(), fpath, childrenChan)
		if err != nil {
			return err
		}
		count++
	}
	if err := it.Err(); err != nil {
		panic(fmt.Sprintf("got error reading files from %s: %v", path, err))
	}
	entryCountChan <- count

	return nil
}

type fileQueue struct {
	ctx context.Context
	jobs chan *fileJob
	adder *Adder
}

type fileJob struct {
	File files.Node
	Name string
	Path string
	Dirchan chan *ipldFileNode
}

func newFileQueue(ctx context.Context, size int, adder *Adder) *fileQueue {
	jobs := make(chan *fileJob, size)
	q := &fileQueue{ ctx, jobs, adder }

	for i := 0; i < size; i++ {
		go q.worker()
	}

	return q
}

func (q *fileQueue) worker() {
	for {
		select {
		case <-q.ctx.Done():
			return

		case job := <-q.jobs:
			if q.ctx.Err() != nil {
				return
			}
			q.process(job)
		}
	}
}

func (q *fileQueue) enqueue(file files.Node, name string, path string, dirchan chan *ipldFileNode) {
	// fmt.Printf("enqueue %s\n", path)
	q.jobs <- &fileJob{ file, name, path, dirchan }
	// fmt.Printf("enqueued %s\n", path)
}

func (q *fileQueue) process(job *fileJob) () {
	// fmt.Printf("process file %s\n", job.Path)
	file := job.File
	defer file.Close()

	switch f := file.(type) {
	case *files.Symlink:
		nd, err := q.adder.addSymlink(job.Path, f)
		if err != nil {
			panic(fmt.Sprintf("error adding symlink %s %v", job.Path, err))
			return
		}
		job.Dirchan <- &ipldFileNode{ nd, job.Name, job.Path, false }
		return
	case files.File:
		// fmt.Printf("  add file %s\n", job.Path)
		nd, err := q.adder.addFile(job.Path, f)
		// fmt.Printf("  added file %s\n", job.Path)
		if err != nil {
			panic(fmt.Sprintf("error adding file %s %v", job.Path, err))
			return
		}
		job.Dirchan <- &ipldFileNode{ nd, job.Name, job.Path, false }
		// fmt.Printf("processed file %s\n", job.Path)
		return
	default:
		panic("fileQueue process(): unknown file type")
	}
}

func (adder *Adder) addSymlink(path string, l *files.Symlink) (ipld.Node, error)  {
	sdata, err := unixfs.SymlinkData(l.Target)
	if err != nil {
		return nil, err
	}

	dagnode := dag.NodeWithData(sdata)
	dagnode.SetCidBuilder(adder.CidBuilder)
	err = adder.dagService.Add(adder.ctx, dagnode)
	if err != nil {
		return nil, err
	}

	err = adder.addNode(dagnode, path)
	if err != nil {
		return nil, err
	}
	return dagnode, nil
}

func (adder *Adder) addFile(path string, file files.File) (ipld.Node, error)  {
	// if the progress flag was specified, wrap the file so that we can send
	// progress updates to the client (over the output channel)
	var reader io.Reader = file
	if adder.Progress {
		rdr := &progressReader{file: reader, path: path, out: adder.Out}
		if fi, ok := file.(files.FileInfo); ok {
			reader = &progressReader2{rdr, fi}
		} else {
			reader = rdr
		}
	}

	dagnode, err := adder.add(reader)
	if err != nil {
		return nil, err
	}

	// err = adder.addNode(dagnode, path)
	// if err != nil {
	// 	return nil, err
	// }
	return dagnode, nil
}

func (adder *Adder) addDir(path string, children []ipldFileNode) (ipld.Node, error) {
	log.Infof("adding directory: %s", path)

	// if !(toplevel && path == "") {
	// 	mr, err := adder.mfsRoot()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	err = mfs.Mkdir(mr, path, mfs.MkdirOpts{
	// 		Mkparents:  true,
	// 		Flush:      false,
	// 		CidBuilder: adder.CidBuilder,
	// 	})
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	unixdir := uio.NewDirectory(adder.dagService)

	for _, f := range children {
		err := unixdir.AddChild(adder.ctx, f.Name, f.Node)
		if err != nil {
			return nil, err
		}
	}

	dirnd, err := unixdir.GetNode()
	if err != nil {
		return nil, err
	}

	err = adder.dagService.Add(adder.ctx, dirnd)
	if err != nil {
		return nil, err
	}

	// err = adder.addNode(dirnd, path)
	// if err != nil {
	// 	return nil, err
	// }

	// if it.Err() != nil {
	// 	return nil, it.Err()
	// }

	return dirnd, nil
}

func (adder *Adder) maybePauseForGC() error {
	if adder.unlocker != nil && adder.gcLocker.GCRequested() {
		rn, err := adder.curRootNode()
		if err != nil {
			return err
		}

		err = adder.PinRoot(rn)
		if err != nil {
			return err
		}

		adder.unlocker.Unlock()
		adder.unlocker = adder.gcLocker.PinLock()
	}
	return nil
}

// outputDagnode sends dagnode info over the output channel
func outputDagnode(out chan<- interface{}, name string, dn ipld.Node) error {
	if out == nil {
		return nil
	}

	o, err := getOutput(dn)
	if err != nil {
		return err
	}

	out <- &coreiface.AddEvent{
		Path: o.Path,
		Name: name,
		Size: o.Size,
	}

	return nil
}

// from core/commands/object.go
func getOutput(dagnode ipld.Node) (*coreiface.AddEvent, error) {
	c := dagnode.Cid()
	s, err := dagnode.Size()
	if err != nil {
		return nil, err
	}

	output := &coreiface.AddEvent{
		Path: ipfspath.IpfsPath(c),
		Size: strconv.FormatUint(s, 10),
	}

	return output, nil
}

type progressReader struct {
	file         io.Reader
	path         string
	out          chan<- interface{}
	bytes        int64
	lastProgress int64
}

func (i *progressReader) Read(p []byte) (int, error) {
	n, err := i.file.Read(p)

	i.bytes += int64(n)
	if i.bytes-i.lastProgress >= progressReaderIncrement || err == io.EOF {
		i.lastProgress = i.bytes
		i.out <- &coreiface.AddEvent{
			Name:  i.path,
			Bytes: i.bytes,
		}
	}

	return n, err
}

type progressReader2 struct {
	*progressReader
	files.FileInfo
}

func (i *progressReader2) Read(p []byte) (int, error) {
	return i.progressReader.Read(p)
}
