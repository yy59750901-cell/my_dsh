package main

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestCheckCompatibilityAllowsAdditiveChanges(t *testing.T) {
	baseline := testSchema()
	current := proto.Clone(baseline).(*descriptorpb.FileDescriptorSet)
	message := current.File[0].MessageType[0]
	message.Field = append(message.Field, testField("label", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING))
	current.File[0].EnumType[0].Value = append(current.File[0].EnumType[0].Value, &descriptorpb.EnumValueDescriptorProto{
		Name: proto.String("STATE_DONE"), Number: proto.Int32(2),
	})
	current.File[0].Service[0].Method = append(current.File[0].Service[0].Method, &descriptorpb.MethodDescriptorProto{
		Name: proto.String("Get"), InputType: proto.String(".test.v1.Item"), OutputType: proto.String(".test.v1.Item"),
	})

	if violations := checkCompatibility(baseline, current, "test/"); len(violations) != 0 {
		t.Fatalf("additive changes rejected: %v", violations)
	}
}

func TestCheckCompatibilityRejectsBreakingFieldChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*descriptorpb.FieldDescriptorProto)
		want   string
	}{
		{name: "rename", mutate: func(field *descriptorpb.FieldDescriptorProto) { field.Name = proto.String("other_id") }, want: "renamed"},
		{name: "type", mutate: func(field *descriptorpb.FieldDescriptorProto) {
			field.Type = descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()
		}, want: "type changed"},
		{name: "cardinality", mutate: func(field *descriptorpb.FieldDescriptorProto) {
			field.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		}, want: "cardinality changed"},
		{name: "json name", mutate: func(field *descriptorpb.FieldDescriptorProto) { field.JsonName = proto.String("otherId") }, want: "json_name changed"},
		{name: "presence", mutate: func(field *descriptorpb.FieldDescriptorProto) { field.Proto3Optional = proto.Bool(true) }, want: "presence changed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseline := testSchema()
			current := proto.Clone(baseline).(*descriptorpb.FileDescriptorSet)
			tt.mutate(current.File[0].MessageType[0].Field[0])
			assertViolation(t, checkCompatibility(baseline, current, "test/"), tt.want)
		})
	}
}

func TestCheckCompatibilityRejectsRemovedField(t *testing.T) {
	baseline := testSchema()
	current := proto.Clone(baseline).(*descriptorpb.FileDescriptorSet)
	current.File[0].MessageType[0].Field = nil
	assertViolation(t, checkCompatibility(baseline, current, "test/"), "removed or renumbered")
}

func TestCheckCompatibilityRejectsEnumAndRPCChanges(t *testing.T) {
	baseline := testSchema()

	t.Run("enum value rename", func(t *testing.T) {
		current := proto.Clone(baseline).(*descriptorpb.FileDescriptorSet)
		current.File[0].EnumType[0].Value[1].Name = proto.String("STATE_ACTIVE")
		assertViolation(t, checkCompatibility(baseline, current, "test/"), "enum value")
	})

	t.Run("rpc response", func(t *testing.T) {
		current := proto.Clone(baseline).(*descriptorpb.FileDescriptorSet)
		current.File[0].Service[0].Method[0].OutputType = proto.String(".test.v1.Other")
		assertViolation(t, checkCompatibility(baseline, current, "test/"), "request or response type changed")
	})
}

func TestCheckCompatibilitySupportsEnumAliases(t *testing.T) {
	baseline := testSchema()
	baseline.File[0].EnumType[0].Options = &descriptorpb.EnumOptions{AllowAlias: proto.Bool(true)}
	baseline.File[0].EnumType[0].Value = append(baseline.File[0].EnumType[0].Value, &descriptorpb.EnumValueDescriptorProto{
		Name: proto.String("STATE_AVAILABLE"), Number: proto.Int32(1),
	})
	current := proto.Clone(baseline).(*descriptorpb.FileDescriptorSet)
	if violations := checkCompatibility(baseline, current, "test/"); len(violations) != 0 {
		t.Fatalf("unchanged enum alias rejected: %v", violations)
	}
	current.File[0].EnumType[0].Value = current.File[0].EnumType[0].Value[:2]
	assertViolation(t, checkCompatibility(baseline, current, "test/"), "STATE_AVAILABLE")
}

func TestCheckCompatibilityIgnoresImportedDescriptors(t *testing.T) {
	baseline := testSchema()
	current := proto.Clone(baseline).(*descriptorpb.FileDescriptorSet)
	baseline.File = append(baseline.File, &descriptorpb.FileDescriptorProto{Name: proto.String("google/protobuf/example.proto")})
	current.File = append(current.File, &descriptorpb.FileDescriptorProto{Name: proto.String("google/protobuf/renamed.proto")})

	if violations := checkCompatibility(baseline, current, "test/"); len(violations) != 0 {
		t.Fatalf("imported descriptor change rejected: %v", violations)
	}
}

func testSchema() *descriptorpb.FileDescriptorSet {
	return &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:    proto.String("test/v1/api.proto"),
		Package: proto.String("test.v1"),
		Syntax:  proto.String("proto3"),
		Options: &descriptorpb.FileOptions{GoPackage: proto.String("example/testv1")},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:  proto.String("Item"),
			Field: []*descriptorpb.FieldDescriptorProto{testField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING)},
		}},
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("State"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("STATE_UNSPECIFIED"), Number: proto.Int32(0)},
				{Name: proto.String("STATE_READY"), Number: proto.Int32(1)},
			},
		}},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("ItemService"),
			Method: []*descriptorpb.MethodDescriptorProto{{
				Name: proto.String("Put"), InputType: proto.String(".test.v1.Item"), OutputType: proto.String(".test.v1.Item"),
			}},
		}},
	}}}
}

func testField(name string, number int32, fieldType descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(number),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:     fieldType.Enum(),
		JsonName: proto.String(name),
	}
}

func assertViolation(t *testing.T, violations []string, want string) {
	t.Helper()
	for _, violation := range violations {
		if strings.Contains(violation, want) {
			return
		}
	}
	t.Fatalf("expected violation containing %q, got %v", want, violations)
}
