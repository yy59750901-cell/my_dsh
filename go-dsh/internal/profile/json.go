package profile

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

// encoding/json 默认接受重复键、大小写别名和孤立代理项，这里显式拒绝。
func strictDecode(raw []byte, target any) error {
	if len(raw) == 0 || len(raw) > MaxJSONBytes || !utf8.Valid(raw) || !validEscapes(raw) {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	nodes := 0
	var read func(int) (any, error)
	read = func(depth int) (any, error) {
		nodes++
		if depth > 64 || nodes > 20000 {
			return nil, ErrInvalid
		}
		token, err := d.Token()
		if err != nil || token == nil {
			return nil, ErrInvalid
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return token, nil
		}
		switch delim {
		case '{':
			value := map[string]any{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return nil, ErrInvalid
				}
				name, ok := key.(string)
				if !ok {
					return nil, ErrInvalid
				}
				if _, ok := value[name]; ok {
					return nil, ErrInvalid
				}
				child, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				value[name] = child
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return nil, ErrInvalid
			}
			return value, nil
		case '[':
			value := []any{}
			for d.More() {
				child, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				value = append(value, child)
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return nil, ErrInvalid
			}
			return value, nil
		}
		return nil, ErrInvalid
	}
	value, err := read(0)
	if err != nil {
		return ErrInvalid
	}
	if _, err = d.Token(); err != io.EOF {
		return ErrInvalid
	}
	if !exactFields(value, reflect.TypeOf(target).Elem()) {
		return ErrInvalid
	}
	if err = json.Unmarshal(raw, target); err != nil {
		return ErrInvalid
	}
	return nil
}

func exactFields(value any, typ reflect.Type) bool {
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			fields[strings.Split(field.Tag.Get("json"), ",")[0]] = field.Type
		}
		for name, child := range object {
			ft, ok := fields[name]
			if !ok || !exactFields(child, ft) {
				return false
			}
		}
	case reflect.Slice:
		array, ok := value.([]any)
		if !ok {
			return false
		}
		for _, child := range array {
			if !exactFields(child, typ.Elem()) {
				return false
			}
		}
	case reflect.Map:
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for _, child := range object {
			if !exactFields(child, typ.Elem()) {
				return false
			}
		}
	}
	return true
}

func validEscapes(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		n, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n < 0xd800 || n > 0xdbff {
			continue
		}
		if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}
