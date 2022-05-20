package rpcutil

/**
A channel-based RPC framework.
*/

import (
	"bytes"
	"log"
	"reflect"
)

type reqMsg struct {
	endName     interface{}
	serviceFunc string
	args        []byte
	argsType    reflect.Type
	replyCh     chan replyMsg
}

type replyMsg struct {
	ok    bool
	reply []byte
}

type ClientEnd struct {
	endName interface{}
	ch      chan reqMsg
	done    chan struct{}
}

func (e *ClientEnd) Call(serviceFunc string, args interface{}, reply interface{}) bool {
	req := reqMsg{}
	req.endName = e.endName
	req.serviceFunc = serviceFunc
	req.argsType = reflect.TypeOf(args)
	req.replyCh = make(chan replyMsg)
	qb := new(bytes.Buffer)
	qe := NewEncoder(qb)
	qe.Encode(args)
	req.args = qb.Bytes()
	select {
	case e.ch <- req:
	case <-e.done:
		return false
	}
	rep := <-req.replyCh
	if rep.ok {
		rb := bytes.NewBuffer(rep.reply)
		rd := NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			log.Fatalf("rpcutil/rpc error: decode reply: %v", err)
		}
	}
	return rep.ok
}
