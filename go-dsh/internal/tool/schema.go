package tool

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"
)

const maxSchemaBytes = 256 << 10

// 支持明确列出的 JSON Schema 子集；未知关键字（包括 $ref、组合和 pattern）拒绝注册。
// JSON 和 schema 都有深度、节点数和数值范围上限，重复键一律拒绝。
type schema struct {
	allow                                                                  bool
	types                                                                  map[string]bool
	properties                                                             map[string]*schema
	required                                                               []string
	additional                                                             *schema
	items                                                                  *schema
	enum                                                                   []any
	minimum, maximum, exclusiveMinimum, exclusiveMaximum                   *big.Rat
	minLength, maxLength, minItems, maxItems, minProperties, maxProperties *int
}

func decodeJSON(raw []byte) (any, error) {
	if !utf8.Valid(raw) {
		return nil, errors.New("invalid UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	nodes := 0
	var read func(int) (any, error)
	read = func(depth int) (any, error) {
		nodes++
		if depth > 64 || nodes > 10000 {
			return nil, errors.New("JSON complexity limit exceeded")
		}
		t, err := d.Token()
		if err != nil {
			return nil, err
		}
		switch v := t.(type) {
		case json.Delim:
			switch v {
			case '{':
				m := make(map[string]any)
				for d.More() {
					key, err := d.Token()
					if err != nil {
						return nil, err
					}
					k, ok := key.(string)
					if !ok {
						return nil, errors.New("invalid object key")
					}
					if _, exists := m[k]; exists {
						return nil, errors.New("duplicate JSON key")
					}
					value, err := read(depth + 1)
					if err != nil {
						return nil, err
					}
					m[k] = value
				}
				end, err := d.Token()
				if err != nil || end != json.Delim('}') {
					return nil, errors.New("unclosed object")
				}
				return m, nil
			case '[':
				a := []any{}
				for d.More() {
					value, err := read(depth + 1)
					if err != nil {
						return nil, err
					}
					a = append(a, value)
				}
				end, err := d.Token()
				if err != nil || end != json.Delim(']') {
					return nil, errors.New("unclosed array")
				}
				return a, nil
			default:
				return nil, errors.New("unexpected delimiter")
			}
		case json.Number:
			if _, err := number(v); err != nil {
				return nil, err
			}
			return v, nil
		default:
			return v, nil
		}
	}
	v, err := read(0)
	if err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, errors.New("trailing JSON data")
	}
	return v, nil
}

func number(v any) (*big.Rat, error) {
	n, ok := v.(json.Number)
	if !ok {
		return nil, errors.New("expected number")
	}
	s := string(n)
	if len(s) > 128 {
		return nil, errors.New("number too long")
	}
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		exp, err := strconv.Atoi(s[i+1:])
		if err != nil || exp < -1000 || exp > 1000 {
			return nil, errors.New("number exponent out of range")
		}
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, errors.New("invalid number")
	}
	return r, nil
}

func compileSchema(raw []byte) (*schema, error) {
	if len(raw) == 0 || len(raw) > maxSchemaBytes {
		return nil, errors.New("schema missing or too large")
	}
	v, err := decodeJSON(raw)
	if err != nil {
		return nil, err
	}
	return buildSchema(v)
}

func buildSchema(v any) (*schema, error) {
	if b, ok := v.(bool); ok {
		return &schema{allow: b}, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("schema must be an object or boolean")
	}
	s := &schema{allow: true}
	for k, v := range m {
		switch k {
		case "title", "description", "$comment":
			if _, ok := v.(string); !ok {
				return nil, fmt.Errorf("%s must be a string", k)
			}
		case "$schema":
			str, ok := v.(string)
			if !ok || (str != "http://json-schema.org/draft-07/schema#" && str != "https://json-schema.org/draft/2020-12/schema" && str != "https://json-schema.org/draft/2019-09/schema") {
				return nil, errors.New("unsupported schema dialect")
			}
		case "default": // 仅注释，不自动填默认值。
		case "examples":
			if _, ok := v.([]any); !ok {
				return nil, errors.New("examples must be an array")
			}
		case "type":
			values, ok := v.([]any)
			if !ok {
				values = []any{v}
			}
			if len(values) == 0 {
				return nil, errors.New("empty type list")
			}
			s.types = make(map[string]bool)
			for _, value := range values {
				t, ok := value.(string)
				if !ok || s.types[t] {
					return nil, errors.New("invalid or duplicate type")
				}
				switch t {
				case "object", "array", "string", "integer", "number", "boolean", "null":
				default:
					return nil, errors.New("unsupported type")
				}
				s.types[t] = true
			}
		case "properties":
			props, ok := v.(map[string]any)
			if !ok {
				return nil, errors.New("properties must be an object")
			}
			s.properties = make(map[string]*schema)
			for name, value := range props {
				child, err := buildSchema(value)
				if err != nil {
					return nil, err
				}
				s.properties[name] = child
			}
		case "required":
			values, ok := v.([]any)
			if !ok {
				return nil, errors.New("required must be an array")
			}
			seen := make(map[string]bool)
			for _, value := range values {
				name, ok := value.(string)
				if !ok || seen[name] {
					return nil, errors.New("invalid required property")
				}
				seen[name] = true
				s.required = append(s.required, name)
			}
		case "additionalProperties":
			child, err := buildSchema(v)
			if err != nil {
				return nil, err
			}
			s.additional = child
		case "items":
			child, err := buildSchema(v)
			if err != nil {
				return nil, err
			}
			s.items = child
		case "enum":
			values, ok := v.([]any)
			if !ok || len(values) == 0 || len(values) > 256 {
				return nil, errors.New("invalid enum")
			}
			for i, value := range values {
				for _, prior := range values[:i] {
					if jsonEqual(value, prior) {
						return nil, errors.New("duplicate enum value")
					}
				}
			}
			s.enum = values
		case "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum":
			n, err := number(v)
			if err != nil {
				return nil, err
			}
			switch k {
			case "minimum":
				s.minimum = n
			case "maximum":
				s.maximum = n
			case "exclusiveMinimum":
				s.exclusiveMinimum = n
			case "exclusiveMaximum":
				s.exclusiveMaximum = n
			}
		case "minLength", "maxLength", "minItems", "maxItems", "minProperties", "maxProperties":
			n, err := number(v)
			if err != nil || !n.IsInt() || n.Sign() < 0 || !n.Num().IsInt64() || n.Num().Int64() > 1<<30 {
				return nil, errors.New("invalid size bound")
			}
			value := int(n.Num().Int64())
			switch k {
			case "minLength":
				s.minLength = &value
			case "maxLength":
				s.maxLength = &value
			case "minItems":
				s.minItems = &value
			case "maxItems":
				s.maxItems = &value
			case "minProperties":
				s.minProperties = &value
			case "maxProperties":
				s.maxProperties = &value
			}
		default:
			return nil, fmt.Errorf("unsupported schema keyword %q", k)
		}
	}
	return s, nil
}

func jsonEqual(a, b any) bool {
	switch x := a.(type) {
	case json.Number:
		y, err := number(b)
		if err != nil {
			return false
		}
		n, _ := number(x)
		return n.Cmp(y) == 0
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			other, ok := y[k]
			if !ok || !jsonEqual(v, other) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i, v := range x {
			if !jsonEqual(v, y[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}

func sizeOK(n int, min, max *int) bool { return (min == nil || n >= *min) && (max == nil || n <= *max) }

func (s *schema) validate(v any, path string) error {
	fail := func(why string) error { return fmt.Errorf("%s: %s", path, why) }
	if !s.allow {
		return fail("value forbidden")
	}
	kind := "null"
	switch v.(type) {
	case map[string]any:
		kind = "object"
	case []any:
		kind = "array"
	case string:
		kind = "string"
	case bool:
		kind = "boolean"
	case json.Number:
		kind = "number"
	}
	if len(s.types) != 0 && !s.types[kind] {
		if kind != "number" || !s.types["integer"] {
			return fail("type mismatch")
		}
		n, _ := number(v)
		if !n.IsInt() {
			return fail("expected integer")
		}
	}
	if s.enum != nil {
		found := false
		for _, e := range s.enum {
			if jsonEqual(v, e) {
				found = true
				break
			}
		}
		if !found {
			return fail("value outside enum")
		}
	}
	switch value := v.(type) {
	case map[string]any:
		if !sizeOK(len(value), s.minProperties, s.maxProperties) {
			return fail("object size out of bounds")
		}
		for _, key := range s.required {
			if _, ok := value[key]; !ok {
				return fail("missing required property")
			}
		}
		for key, v := range value {
			child, ok := s.properties[key]
			if !ok {
				child = s.additional
			}
			if child != nil {
				if err := child.validate(v, path+"."+key); err != nil {
					return err
				}
			}
		}
	case []any:
		if !sizeOK(len(value), s.minItems, s.maxItems) {
			return fail("array size out of bounds")
		}
		if s.items != nil {
			for i, v := range value {
				if err := s.items.validate(v, fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
			}
		}
	case string:
		if !sizeOK(utf8.RuneCountInString(value), s.minLength, s.maxLength) {
			return fail("string length out of bounds")
		}
	case json.Number:
		n, _ := number(value)
		if s.minimum != nil && n.Cmp(s.minimum) < 0 || s.maximum != nil && n.Cmp(s.maximum) > 0 || s.exclusiveMinimum != nil && n.Cmp(s.exclusiveMinimum) <= 0 || s.exclusiveMaximum != nil && n.Cmp(s.exclusiveMaximum) >= 0 {
			return fail("number out of bounds")
		}
	}
	return nil
}
