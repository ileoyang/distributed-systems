package mapreduce

import (
	"github.com/gammazero/deque"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	timeout = 10  // wait timeout for a task
	tempDir = "temp"  // directory of temporary files
)

type Master struct {
	mu sync.Mutex
	nMap int  // number of map tasks
	nReduce int  // number of reduce tasks
	tasks deque.Deque
	assignment map[int]Task  // running task assigned to worker ID
}

func (m *Master) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	m.mu.Lock()
	workerId := args.WorkerId
	// consider the task previously assigned to the worker as completed.
	delete(m.assignment, workerId)
	var task Task
	// have unassigned valid tasks.
	if m.tasks.Len() != 0 && (m.tasks.Len() != m.nReduce || len(m.assignment) == 0) {
		task = m.tasks.Front().(Task)
		m.assignment[workerId] = task
		m.tasks.PopFront()
	} else {
		task.Type = Idle
	}
	reply.Task = task
	m.mu.Unlock()
	go func() {
		// if an assigned task has not finished within a certain time, determine worker failure and reset the task.
		time.Sleep(timeout * time.Second)
		m.mu.Lock()
		defer m.mu.Unlock()
		if task, ok := m.assignment[workerId]; ok {
			m.tasks.PushFront(task)
			delete(m.assignment, workerId)
		}
	}()
	return nil
}

// start a thread that listens for RPCs.
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	unixAddress := unixAddress()
	os.Remove(unixAddress)
	listener, err := net.Listen("unix", unixAddress)
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}
	go http.Serve(listener, nil)
}

// Done returns if the entire job has finished.
func (m *Master) Done() bool {
	return m.tasks.Len() == 0 && len(m.assignment) == 0
}

func unixAddress() string {
	return "/var/tmp/824-mr-" + strconv.Itoa(os.Getuid())
}

func NewMaster(files []string, nReduce int) *Master {
	m := Master{}
	nMap := len(files)
	m.nMap = nMap
	m.nReduce = nReduce
	m.tasks = deque.Deque{}
	m.assignment = make(map[int]Task)
	for i := 0; i < nMap; i++ {
		m.tasks.PushBack(Task{i, Map, files[i]})
	}
	for i := 0; i < nReduce; i++ {
		m.tasks.PushBack(Task{i, Reduce, ""})
	}
	m.server()
	// reset task result files.
	filePaths, err := filepath.Glob("mr-out*")
	if err != nil {
		log.Fatalf("cannot glob: %v", err)
	}
	for _, filePath := range filePaths {
		if err := os.Remove(filePath); err != nil {
			log.Fatalf("cannot remove: %v", err)
		}
	}
	if err := os.RemoveAll(tempDir); err != nil {
		log.Fatalf("cannot remove temp directory: %v", err)
	}
	if err := os.Mkdir(tempDir, 0755); err != nil {
		log.Fatalf("cannot create temp directory: %v", err)
	}
	return &m
}