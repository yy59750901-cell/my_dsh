package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
)

const defaultOwnedPrefix = "dsh/protocol/"

type schemaIndex struct {
	files    map[string]*descriptorpb.FileDescriptorProto
	messages map[string]*descriptorpb.DescriptorProto
	enums    map[string]*descriptorpb.EnumDescriptorProto
	services map[string]*descriptorpb.ServiceDescriptorProto
}

func main() {
	baselinePath := flag.String("baseline", "", "baseline descriptor set")
	currentPath := flag.String("current", "", "current descriptor set")
	ownedPrefix := flag.String("owned-prefix", defaultOwnedPrefix, "owned proto file prefix")
	flag.Parse()

	if *baselinePath == "" || *currentPath == "" {
		fmt.Fprintln(os.Stderr, "baseline and current descriptor paths are required")
		os.Exit(2)
	}

	baseline, err := loadDescriptorSet(*baselinePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load baseline: %v\n", err)
		os.Exit(2)
	}
	current, err := loadDescriptorSet(*currentPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load current: %v\n", err)
		os.Exit(2)
	}

	violations := checkCompatibility(baseline, current, *ownedPrefix)
	if len(violations) == 0 {
		return
	}
	for _, violation := range violations {
		fmt.Fprintf(os.Stderr, "BREAKING: %s\n", violation)
	}
	os.Exit(1)
}

func loadDescriptorSet(path string) (*descriptorpb.FileDescriptorSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	set := new(descriptorpb.FileDescriptorSet)
	if err := proto.Unmarshal(data, set); err != nil {
		return nil, fmt.Errorf("decode descriptor set: %w", err)
	}
	if _, err := protodesc.NewFiles(set); err != nil {
		return nil, fmt.Errorf("resolve descriptors: %w", err)
	}
	return set, nil
}

func checkCompatibility(baseline, current *descriptorpb.FileDescriptorSet, ownedPrefix string) []string {
	oldIndex := buildIndex(baseline, ownedPrefix)
	newIndex := buildIndex(current, ownedPrefix)
	violations := make([]string, 0)

	fileNames := sortedKeys(oldIndex.files)
	for _, name := range fileNames {
		oldFile := oldIndex.files[name]
		newFile, ok := newIndex.files[name]
		if !ok {
			violations = append(violations, fmt.Sprintf("file %s removed or renamed", name))
			continue
		}
		if oldFile.GetPackage() != newFile.GetPackage() {
			violations = append(violations, fmt.Sprintf("file %s package changed from %q to %q", name, oldFile.GetPackage(), newFile.GetPackage()))
		}
		if oldFile.GetSyntax() != newFile.GetSyntax() {
			violations = append(violations, fmt.Sprintf("file %s syntax changed from %q to %q", name, oldFile.GetSyntax(), newFile.GetSyntax()))
		}
		if oldFile.GetOptions().GetGoPackage() != newFile.GetOptions().GetGoPackage() {
			violations = append(violations, fmt.Sprintf("file %s go_package changed", name))
		}
	}

	for _, name := range sortedKeys(oldIndex.messages) {
		oldMessage := oldIndex.messages[name]
		newMessage, ok := newIndex.messages[name]
		if !ok {
			violations = append(violations, fmt.Sprintf("message %s removed, renamed, or moved", name))
			continue
		}
		violations = append(violations, compareMessage(name, oldMessage, newMessage)...)
	}

	for _, name := range sortedKeys(oldIndex.enums) {
		oldEnum := oldIndex.enums[name]
		newEnum, ok := newIndex.enums[name]
		if !ok {
			violations = append(violations, fmt.Sprintf("enum %s removed, renamed, or moved", name))
			continue
		}
		violations = append(violations, compareEnum(name, oldEnum, newEnum)...)
	}

	for _, name := range sortedKeys(oldIndex.services) {
		oldService := oldIndex.services[name]
		newService, ok := newIndex.services[name]
		if !ok {
			violations = append(violations, fmt.Sprintf("service %s removed, renamed, or moved", name))
			continue
		}
		violations = append(violations, compareService(name, oldService, newService)...)
	}

	sort.Strings(violations)
	return violations
}

func buildIndex(set *descriptorpb.FileDescriptorSet, ownedPrefix string) schemaIndex {
	index := schemaIndex{
		files:    make(map[string]*descriptorpb.FileDescriptorProto),
		messages: make(map[string]*descriptorpb.DescriptorProto),
		enums:    make(map[string]*descriptorpb.EnumDescriptorProto),
		services: make(map[string]*descriptorpb.ServiceDescriptorProto),
	}
	for _, file := range set.GetFile() {
		if !strings.HasPrefix(file.GetName(), ownedPrefix) {
			continue
		}
		index.files[file.GetName()] = file
		prefix := file.GetPackage()
		for _, message := range file.GetMessageType() {
			indexMessage(index, file.GetName(), prefix, message)
		}
		for _, enum := range file.GetEnumType() {
			index.enums[qualified(prefix, enum.GetName())] = enum
		}
		for _, service := range file.GetService() {
			index.services[qualified(prefix, service.GetName())] = service
		}
	}
	return index
}

func indexMessage(index schemaIndex, fileName, prefix string, message *descriptorpb.DescriptorProto) {
	name := qualified(prefix, message.GetName())
	index.messages[name] = message
	for _, nested := range message.GetNestedType() {
		indexMessage(index, fileName, name, nested)
	}
	for _, enum := range message.GetEnumType() {
		index.enums[qualified(name, enum.GetName())] = enum
	}
}

func compareMessage(name string, oldMessage, newMessage *descriptorpb.DescriptorProto) []string {
	violations := make([]string, 0)
	if oldMessage.GetOptions().GetMapEntry() != newMessage.GetOptions().GetMapEntry() {
		violations = append(violations, fmt.Sprintf("message %s map-entry semantics changed", name))
	}

	newFieldsByNumber := make(map[int32]*descriptorpb.FieldDescriptorProto, len(newMessage.GetField()))
	for _, field := range newMessage.GetField() {
		newFieldsByNumber[field.GetNumber()] = field
	}
	for _, oldField := range oldMessage.GetField() {
		newField, ok := newFieldsByNumber[oldField.GetNumber()]
		fieldName := fmt.Sprintf("%s.%s(%d)", name, oldField.GetName(), oldField.GetNumber())
		if !ok {
			violations = append(violations, fmt.Sprintf("field %s removed or renumbered", fieldName))
			continue
		}
		if oldField.GetName() != newField.GetName() {
			violations = append(violations, fmt.Sprintf("field %s renamed to %s", fieldName, newField.GetName()))
		}
		if oldField.GetType() != newField.GetType() || oldField.GetTypeName() != newField.GetTypeName() {
			violations = append(violations, fmt.Sprintf("field %s type changed", fieldName))
		}
		if oldField.GetLabel() != newField.GetLabel() {
			violations = append(violations, fmt.Sprintf("field %s cardinality changed", fieldName))
		}
		if oldField.GetJsonName() != newField.GetJsonName() {
			violations = append(violations, fmt.Sprintf("field %s json_name changed", fieldName))
		}
		if oldField.GetProto3Optional() != newField.GetProto3Optional() {
			violations = append(violations, fmt.Sprintf("field %s presence changed", fieldName))
		}
		if fieldOneofName(oldMessage, oldField) != fieldOneofName(newMessage, newField) {
			violations = append(violations, fmt.Sprintf("field %s oneof membership changed", fieldName))
		}
		if oldField.GetDefaultValue() != newField.GetDefaultValue() {
			violations = append(violations, fmt.Sprintf("field %s default value changed", fieldName))
		}
	}

	violations = append(violations, compareReservedRanges("message "+name, oldMessage.GetReservedRange(), newMessage.GetReservedRange())...)
	violations = append(violations, compareStrings("message "+name+" reserved name", oldMessage.GetReservedName(), newMessage.GetReservedName())...)
	violations = append(violations, compareExtensionRanges(name, oldMessage.GetExtensionRange(), newMessage.GetExtensionRange())...)
	return violations
}

func compareEnum(name string, oldEnum, newEnum *descriptorpb.EnumDescriptorProto) []string {
	violations := make([]string, 0)
	currentValues := make(map[string]struct{}, len(newEnum.GetValue()))
	for _, value := range newEnum.GetValue() {
		currentValues[fmt.Sprintf("%d:%s", value.GetNumber(), value.GetName())] = struct{}{}
	}
	for _, oldValue := range oldEnum.GetValue() {
		key := fmt.Sprintf("%d:%s", oldValue.GetNumber(), oldValue.GetName())
		valueName := fmt.Sprintf("%s.%s(%d)", name, oldValue.GetName(), oldValue.GetNumber())
		if _, ok := currentValues[key]; !ok {
			violations = append(violations, fmt.Sprintf("enum value %s removed, renamed, or renumbered", valueName))
		}
	}
	if oldEnum.GetOptions().GetAllowAlias() != newEnum.GetOptions().GetAllowAlias() {
		violations = append(violations, fmt.Sprintf("enum %s alias semantics changed", name))
	}
	violations = append(violations, compareEnumReservedRanges(name, oldEnum.GetReservedRange(), newEnum.GetReservedRange())...)
	violations = append(violations, compareStrings("enum "+name+" reserved name", oldEnum.GetReservedName(), newEnum.GetReservedName())...)
	return violations
}

func compareService(name string, oldService, newService *descriptorpb.ServiceDescriptorProto) []string {
	violations := make([]string, 0)
	newMethods := make(map[string]*descriptorpb.MethodDescriptorProto, len(newService.GetMethod()))
	for _, method := range newService.GetMethod() {
		newMethods[method.GetName()] = method
	}
	for _, oldMethod := range oldService.GetMethod() {
		newMethod, ok := newMethods[oldMethod.GetName()]
		methodName := name + "." + oldMethod.GetName()
		if !ok {
			violations = append(violations, fmt.Sprintf("method %s removed or renamed", methodName))
			continue
		}
		if oldMethod.GetInputType() != newMethod.GetInputType() || oldMethod.GetOutputType() != newMethod.GetOutputType() {
			violations = append(violations, fmt.Sprintf("method %s request or response type changed", methodName))
		}
		if oldMethod.GetClientStreaming() != newMethod.GetClientStreaming() || oldMethod.GetServerStreaming() != newMethod.GetServerStreaming() {
			violations = append(violations, fmt.Sprintf("method %s streaming semantics changed", methodName))
		}
	}
	return violations
}

func fieldOneofName(message *descriptorpb.DescriptorProto, field *descriptorpb.FieldDescriptorProto) string {
	if field.OneofIndex == nil {
		return ""
	}
	index := int(field.GetOneofIndex())
	if index < 0 || index >= len(message.GetOneofDecl()) {
		return fmt.Sprintf("<invalid:%d>", index)
	}
	return message.GetOneofDecl()[index].GetName()
}

func compareReservedRanges(owner string, oldRanges, newRanges []*descriptorpb.DescriptorProto_ReservedRange) []string {
	current := make(map[string]struct{}, len(newRanges))
	for _, item := range newRanges {
		current[fmt.Sprintf("%d:%d", item.GetStart(), item.GetEnd())] = struct{}{}
	}
	violations := make([]string, 0)
	for _, item := range oldRanges {
		key := fmt.Sprintf("%d:%d", item.GetStart(), item.GetEnd())
		if _, ok := current[key]; !ok {
			violations = append(violations, fmt.Sprintf("%s reserved range [%d,%d) removed or changed", owner, item.GetStart(), item.GetEnd()))
		}
	}
	return violations
}

func compareEnumReservedRanges(name string, oldRanges, newRanges []*descriptorpb.EnumDescriptorProto_EnumReservedRange) []string {
	current := make(map[string]struct{}, len(newRanges))
	for _, item := range newRanges {
		current[fmt.Sprintf("%d:%d", item.GetStart(), item.GetEnd())] = struct{}{}
	}
	violations := make([]string, 0)
	for _, item := range oldRanges {
		key := fmt.Sprintf("%d:%d", item.GetStart(), item.GetEnd())
		if _, ok := current[key]; !ok {
			violations = append(violations, fmt.Sprintf("enum %s reserved range [%d,%d] removed or changed", name, item.GetStart(), item.GetEnd()))
		}
	}
	return violations
}

func compareExtensionRanges(name string, oldRanges, newRanges []*descriptorpb.DescriptorProto_ExtensionRange) []string {
	current := make(map[string]struct{}, len(newRanges))
	for _, item := range newRanges {
		current[fmt.Sprintf("%d:%d", item.GetStart(), item.GetEnd())] = struct{}{}
	}
	violations := make([]string, 0)
	for _, item := range oldRanges {
		key := fmt.Sprintf("%d:%d", item.GetStart(), item.GetEnd())
		if _, ok := current[key]; !ok {
			violations = append(violations, fmt.Sprintf("message %s extension range [%d,%d) removed or changed", name, item.GetStart(), item.GetEnd()))
		}
	}
	return violations
}

func compareStrings(owner string, oldValues, newValues []string) []string {
	current := make(map[string]struct{}, len(newValues))
	for _, value := range newValues {
		current[value] = struct{}{}
	}
	violations := make([]string, 0)
	for _, value := range oldValues {
		if _, ok := current[value]; !ok {
			violations = append(violations, fmt.Sprintf("%s %q removed", owner, value))
		}
	}
	return violations
}

func qualified(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
