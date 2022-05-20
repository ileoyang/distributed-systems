package rpcutil

/**
A wrapper around encoding/gob warns about RPC-related errors.
*/

import (
	"encoding/gob"
	"fmt"
	"io"
	"reflect"
	"sync"
	"unicode"
	"unicode/utf8"
)

var mu sync.Mutex
var checked map[reflect.Type]bool
var errorCount int

type Encoder struct {
	gob *gob.Encoder
}

func NewEncoder(w io.Writer) *Encoder {
	enc := &Encoder{}
	enc.gob = gob.NewEncoder(w)
	return enc
}

func (enc *Encoder) Encode(e interface{}) error {
	checkValue(e)
	return enc.gob.Encode(e)
}

func (enc *Encoder) EncodeValue(value reflect.Value) error {
	checkValue(value.Interface())
	return enc.gob.EncodeValue(value)
}

type Decoder struct {
	gob *gob.Decoder
}

func NewDecoder(r io.Reader) *Decoder {
	dec := &Decoder{}
	dec.gob = gob.NewDecoder(r)
	return dec
}

func (dec *Decoder) Decode(e interface{}) error {
	checkValue(e)
	checkDefault(e)
	return dec.gob.Decode(e)
}

func checkValue(value interface{}) {
	checkType(reflect.TypeOf(value))
}

func checkType(t reflect.Type) {
	k := t.Kind()
	mu.Lock()
	if checked == nil {
		checked = map[reflect.Type]bool{}
	}
	if checked[t] {
		mu.Unlock()
		return
	}
	checked[t] = true
	mu.Unlock()
	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			rune, _ := utf8.DecodeRuneInString(f.Name)
			if !unicode.IsUpper(rune) {
				fmt.Printf("rpcutil/gob error: use a lowercase field %v of %v in RPC may cause undefined behaviors", f.Name, t.Name())
				mu.Lock()
				errorCount++
				mu.Unlock()
			}
			checkType(f.Type)
		}
	case reflect.Slice, reflect.Array, reflect.Ptr:
		checkType(t.Elem())
	case reflect.Map:
		checkType(t.Elem())
		checkType(t.Key())
	}
}

func checkDefault(value interface{}) {
	if value != nil {
		checkDefault1(reflect.ValueOf(value), 1, "")
	}
}

func checkDefault1(value reflect.Value, depth int, name string) {
	if depth > 3 {
		return
	}
	t := value.Type()
	k := t.Kind()
	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := value.Field(i)
			name1 := t.Field(i).Name
			if name != "" {
				name1 = name + "." + name1
			}
			checkDefault1(f, depth+1, name1)
		}
	case reflect.Ptr:
		if !value.IsNil() {
			checkDefault1(value.Elem(), depth+1, name)
		}
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8,
		reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64, reflect.String:
		if !reflect.DeepEqual(reflect.Zero(t).Interface(), value.Interface()) {
			mu.Lock()
			if errorCount < 1 {
				name1 := name
				if name1 == "" {
					name1 = t.Name()
				}
				fmt.Printf("rpcutil/gob warning: decode into a non-default variable/field %v may not work", name1)
			}
			errorCount++
			mu.Unlock()
		}
	}
}
