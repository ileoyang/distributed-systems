package main

import (
	"distributed-systems/mapreduce"
	"log"
	"os"
	"plugin"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("usage: go run mapreduce.go [nReduce] [nWorker] [func.so] [filename]...")
	}
	nReduce, _ := strconv.Atoi(os.Args[1])
	nWorker, _ := strconv.Atoi(os.Args[2])
	mapFunc, reduceFunc := load(os.Args[3])
	m := mapreduce.NewMaster(os.Args[4:], nReduce)
	time.Sleep(time.Second)
	for i := 0; i < nWorker; i++ {
		go mapreduce.RunWorker(mapFunc, reduceFunc, nReduce)
	}
	for m.Done() == false {
		time.Sleep(time.Second)
	}
}

func load(filename string) (func(string, string) []mapreduce.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin: %v", err)
	}
	mapFunc, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map: %v", err)
	}
	reduceFunc, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce: %v", err)
	}
	return mapFunc.(func(string, string) []mapreduce.KeyValue), reduceFunc.(func(string, []string) string)
}