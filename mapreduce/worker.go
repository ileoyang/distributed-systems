package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const idleTime = 1

type KeyValue struct {
	Key   string
	Value string
}

type MapFunc func(string, string) []KeyValue

type ReduceFunc func(string, []string) string

// KeyValuesByKey defines a collection type that implements sort.Interface.
type KeyValuesByKey []KeyValue

func (kva KeyValuesByKey) Len() int {
	return len(kva)
}

func (kva KeyValuesByKey) Swap(i, j int) {
	kva[i], kva[j] = kva[j], kva[i]
}

func (kva KeyValuesByKey) Less(i, j int) bool {
	return kva[i].Key < kva[j].Key
}

func RunWorker(mapFunc MapFunc, reduceFunc ReduceFunc, nReduce int) {
	for {
		args := AssignTaskArgs{os.Getpid()}
		reply := AssignTaskReply{}
		ok := call("Master.AssignTask", &args, &reply)
		if ok == false {
			fmt.Println("cannot connect to the master, maybe all the tasks are finished")
			break
		}
		task := reply.Task
		if task.Type == Map {
			doMap(mapFunc, task.Id, task.Filename, nReduce)
		} else if task.Type == Reduce {
			doReduce(reduceFunc, task.Id)
		} else {
			time.Sleep(idleTime * time.Second)
		}
	}
}

// generate hash code of a key.
func hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(mapFunc MapFunc, taskId int, filename string, nReduce int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file: %v", err)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file: %v", err)
	}
	kva := mapFunc(filename, string(content))
	// write kva to disk buckets by key.
	filePrefix := fmt.Sprintf("%v/map-%v", tempDir, taskId)
	files := make([]*os.File, nReduce)
	writers := make([]*bufio.Writer, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		// partial file path, use worker's pid to identify in case of failure.
		filePath := fmt.Sprintf("%v-%v-%v", filePrefix, i, os.Getpid())
		files[i], err = os.Create(filePath)
		if err != nil {
			log.Fatalf("cannot create file: %v", err)
		}
		writers[i] = bufio.NewWriter(files[i])
		encoders[i] = json.NewEncoder(writers[i])
	}
	for _, kv := range kva {
		idx := hash(kv.Key) % nReduce
		if err := encoders[idx].Encode(&kv); err != nil {
			log.Fatalf("cannot encode: %v", err)
		}
	}
	for _, writer := range writers {
		if err := writer.Flush(); err != nil {
			log.Fatalf("cannot flush: %v", err)
		}
	}
	// rename partial files to commit.
	for i, file := range files {
		newPath := fmt.Sprintf("%v-%v", filePrefix, i)
		if err := os.Rename(file.Name(), newPath); err != nil {
			log.Fatalf("cannot rename: %v", err)
		}
		file.Close()
	}
}

func doReduce(reduceFunc ReduceFunc, taskId int) {
	filePaths, err := filepath.Glob(fmt.Sprintf("%v/map-%v-%v", tempDir, "*", taskId))
	if err != nil {
		log.Fatalf("cannot glob: %v", err)
	}
	var kva []KeyValue
	for _, filePath := range filePaths {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open file: %v", err)
		}
		var kv KeyValue
		dec := json.NewDecoder(file)
		for dec.More() {
			if err := dec.Decode(&kv); err != nil {
				log.Fatalf("cannot decode: %v", err)
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(KeyValuesByKey(kva))
	// partial file path, use worker's pid to identify in case of failure.
	filePath := fmt.Sprintf("%v/reduce-%v-%v", tempDir, taskId, os.Getpid())
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("cannot create file: %v", err)
	}
	defer file.Close()
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reduceFunc(kva[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		i = j
	}
	// rename partial files to commit.
	newPath := fmt.Sprintf("mr-out-%v", taskId)
	if err = os.Rename(filePath, newPath); err != nil {
		log.Fatalf("cannot rename: %v", err)
	}
}

// send RPC request to the master.
func call(rpcName string, args interface{}, reply interface{}) bool {
	client, err := rpc.DialHTTP("unix", unixAddress())
	if err != nil {
		log.Fatalf("dialing: %v", err)
	}
	defer client.Close()
	if err = client.Call(rpcName, args, reply); err != nil {
		fmt.Println(err)
		return false
	}
	return true
}