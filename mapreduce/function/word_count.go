package main

import (
	"distributed-systems/mapreduce"
	"strconv"
	"strings"
	"unicode"
)

func Map(filename string, content string) []mapreduce.KeyValue {
	words := strings.FieldsFunc(content, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	var kva []mapreduce.KeyValue
	for _, word := range words {
		kv := mapreduce.KeyValue{Key: word, Value: "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}