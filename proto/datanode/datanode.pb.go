// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.4
// source: proto/datanode/datanode.proto

package datanode

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type DeleteReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BlockId string `protobuf:"bytes,1,opt,name=BlockId,proto3" json:"BlockId,omitempty"`
}

func (x *DeleteReq) Reset() {
	*x = DeleteReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_datanode_datanode_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteReq) ProtoMessage() {}

func (x *DeleteReq) ProtoReflect() protoreflect.Message {
	mi := &file_proto_datanode_datanode_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteReq.ProtoReflect.Descriptor instead.
func (*DeleteReq) Descriptor() ([]byte, []int) {
	return file_proto_datanode_datanode_proto_rawDescGZIP(), []int{0}
}

func (x *DeleteReq) GetBlockId() string {
	if x != nil {
		return x.BlockId
	}
	return ""
}

type DeleteResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Success bool `protobuf:"varint,1,opt,name=Success,proto3" json:"Success,omitempty"`
}

func (x *DeleteResp) Reset() {
	*x = DeleteResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_datanode_datanode_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteResp) ProtoMessage() {}

func (x *DeleteResp) ProtoReflect() protoreflect.Message {
	mi := &file_proto_datanode_datanode_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteResp.ProtoReflect.Descriptor instead.
func (*DeleteResp) Descriptor() ([]byte, []int) {
	return file_proto_datanode_datanode_proto_rawDescGZIP(), []int{1}
}

func (x *DeleteResp) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

type PingReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Host string `protobuf:"bytes,1,opt,name=Host,proto3" json:"Host,omitempty"`
	Port uint32 `protobuf:"varint,2,opt,name=Port,proto3" json:"Port,omitempty"`
}

func (x *PingReq) Reset() {
	*x = PingReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_datanode_datanode_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PingReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PingReq) ProtoMessage() {}

func (x *PingReq) ProtoReflect() protoreflect.Message {
	mi := &file_proto_datanode_datanode_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PingReq.ProtoReflect.Descriptor instead.
func (*PingReq) Descriptor() ([]byte, []int) {
	return file_proto_datanode_datanode_proto_rawDescGZIP(), []int{2}
}

func (x *PingReq) GetHost() string {
	if x != nil {
		return x.Host
	}
	return ""
}

func (x *PingReq) GetPort() uint32 {
	if x != nil {
		return x.Port
	}
	return 0
}

type PingResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Success bool `protobuf:"varint,1,opt,name=Success,proto3" json:"Success,omitempty"`
}

func (x *PingResp) Reset() {
	*x = PingResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_datanode_datanode_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PingResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PingResp) ProtoMessage() {}

func (x *PingResp) ProtoReflect() protoreflect.Message {
	mi := &file_proto_datanode_datanode_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PingResp.ProtoReflect.Descriptor instead.
func (*PingResp) Descriptor() ([]byte, []int) {
	return file_proto_datanode_datanode_proto_rawDescGZIP(), []int{3}
}

func (x *PingResp) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

type HeartBeatReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Request bool `protobuf:"varint,1,opt,name=Request,proto3" json:"Request,omitempty"`
}

func (x *HeartBeatReq) Reset() {
	*x = HeartBeatReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_datanode_datanode_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HeartBeatReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HeartBeatReq) ProtoMessage() {}

func (x *HeartBeatReq) ProtoReflect() protoreflect.Message {
	mi := &file_proto_datanode_datanode_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HeartBeatReq.ProtoReflect.Descriptor instead.
func (*HeartBeatReq) Descriptor() ([]byte, []int) {
	return file_proto_datanode_datanode_proto_rawDescGZIP(), []int{4}
}

func (x *HeartBeatReq) GetRequest() bool {
	if x != nil {
		return x.Request
	}
	return false
}

type HeartBeatResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Success bool `protobuf:"varint,1,opt,name=Success,proto3" json:"Success,omitempty"`
}

func (x *HeartBeatResp) Reset() {
	*x = HeartBeatResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_datanode_datanode_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HeartBeatResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HeartBeatResp) ProtoMessage() {}

func (x *HeartBeatResp) ProtoReflect() protoreflect.Message {
	mi := &file_proto_datanode_datanode_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HeartBeatResp.ProtoReflect.Descriptor instead.
func (*HeartBeatResp) Descriptor() ([]byte, []int) {
	return file_proto_datanode_datanode_proto_rawDescGZIP(), []int{5}
}

func (x *HeartBeatResp) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

type PutReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Path             string              `protobuf:"bytes,1,opt,name=Path,proto3" json:"Path,omitempty"`
	BlockId          string              `protobuf:"bytes,2,opt,name=BlockId,proto3" json:"BlockId,omitempty"`
	Data             string              `protobuf:"bytes,3,opt,name=Data,proto3" json:"Data,omitempty"`
	ReplicationNodes []*DataNodeInstance `protobuf:"bytes,4,rep,name=ReplicationNodes,proto3" json:"ReplicationNodes,omitempty"` //有一个数组参数：副本DataNode节点
}

func (x *PutReq) Reset() {
	*x = PutReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_datanode_datanode_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PutReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutReq) ProtoMessage() {}

func (x *PutReq) ProtoReflect() protoreflect.Message {
	mi := &file_proto_datanode_datanode_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PutReq.ProtoReflect.Descriptor instead.
func (*PutReq) Descriptor() ([]byte, []int) {
	return file_proto_datanode_datanode_proto_rawDescGZIP(), []int{6}
}

func (x *PutReq) GetPath() string {
	if x != nil {
		return x.Path
	}
	return ""
}

func (x *PutReq) GetBlockId() string {
	if x != nil {
		return x.BlockId
	}
	return ""
}

func (x *PutReq) GetData() string {
	if x != nil {
		return x.Data
	}
	return ""
}

func (x *PutReq) GetReplicationNodes() []*DataNodeInstance {
	if x != nil {
		return x.ReplicationNodes
	}
	return nil
}

type DataNodeInstance struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Host        string `protobuf:"bytes,1,opt,name=Host,proto3" json:"Host,omitempty"`
	ServicePort string `protobuf:"bytes,2,opt,name=ServicePort,proto3" json:"ServicePort,omitempty"`
}

func (x *DataNodeInstance) Reset() {
	*x = DataNodeInstance{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_datanode_datanode_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DataNodeInstance) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DataNodeInstance) ProtoMessage() {}

func (x *DataNodeInstance) ProtoReflect() protoreflect.Message {
	mi := &file_proto_datanode_datanode_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DataNodeInstance.ProtoReflect.Descriptor instead.
func (*DataNodeInstance) Descriptor() ([]byte, []int) {
	return file_proto_datanode_datanode_proto_rawDescGZIP(), []int{7}
}

func (x *DataNodeInstance) GetHost() string {
	if x != nil {
		return x.Host
	}
	return ""
}

func (x *DataNodeInstance) GetServicePort() string {
	if x != nil {
		return x.ServicePort
	}
	return ""
}

type PutResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Success bool `protobuf:"varint,1,opt,name=Success,proto3" json:"Success,omitempty"`
}

func (x *PutResp) Reset() {
	*x = PutResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_datanode_datanode_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PutResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutResp) ProtoMessage() {}

func (x *PutResp) ProtoReflect() protoreflect.Message {
	mi := &file_proto_datanode_datanode_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PutResp.ProtoReflect.Descriptor instead.
func (*PutResp) Descriptor() ([]byte, []int) {
	return file_proto_datanode_datanode_proto_rawDescGZIP(), []int{8}
}

func (x *PutResp) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

type GetReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BlockId string `protobuf:"bytes,1,opt,name=BlockId,proto3" json:"BlockId,omitempty"`
}

func (x *GetReq) Reset() {
	*x = GetReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_datanode_datanode_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetReq) ProtoMessage() {}

func (x *GetReq) ProtoReflect() protoreflect.Message {
	mi := &file_proto_datanode_datanode_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetReq.ProtoReflect.Descriptor instead.
func (*GetReq) Descriptor() ([]byte, []int) {
	return file_proto_datanode_datanode_proto_rawDescGZIP(), []int{9}
}

func (x *GetReq) GetBlockId() string {
	if x != nil {
		return x.BlockId
	}
	return ""
}

type GetResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data string `protobuf:"bytes,1,opt,name=Data,proto3" json:"Data,omitempty"`
}

func (x *GetResp) Reset() {
	*x = GetResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_datanode_datanode_proto_msgTypes[10]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetResp) ProtoMessage() {}

func (x *GetResp) ProtoReflect() protoreflect.Message {
	mi := &file_proto_datanode_datanode_proto_msgTypes[10]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetResp.ProtoReflect.Descriptor instead.
func (*GetResp) Descriptor() ([]byte, []int) {
	return file_proto_datanode_datanode_proto_rawDescGZIP(), []int{10}
}

func (x *GetResp) GetData() string {
	if x != nil {
		return x.Data
	}
	return ""
}

var File_proto_datanode_datanode_proto protoreflect.FileDescriptor

var file_proto_datanode_datanode_proto_rawDesc = []byte{
	0x0a, 0x1d, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65,
	0x2f, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x08, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x22, 0x25, 0x0a, 0x09, 0x44, 0x65, 0x6c,
	0x65, 0x74, 0x65, 0x52, 0x65, 0x71, 0x12, 0x18, 0x0a, 0x07, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x49,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x49, 0x64,
	0x22, 0x26, 0x0a, 0x0a, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x12, 0x18,
	0x0a, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52,
	0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x22, 0x31, 0x0a, 0x07, 0x50, 0x69, 0x6e, 0x67,
	0x52, 0x65, 0x71, 0x12, 0x12, 0x0a, 0x04, 0x48, 0x6f, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x48, 0x6f, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x50, 0x6f, 0x72, 0x74, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x04, 0x50, 0x6f, 0x72, 0x74, 0x22, 0x24, 0x0a, 0x08, 0x50,
	0x69, 0x6e, 0x67, 0x52, 0x65, 0x73, 0x70, 0x12, 0x18, 0x0a, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65,
	0x73, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73,
	0x73, 0x22, 0x28, 0x0a, 0x0c, 0x48, 0x65, 0x61, 0x72, 0x74, 0x42, 0x65, 0x61, 0x74, 0x52, 0x65,
	0x71, 0x12, 0x18, 0x0a, 0x07, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x08, 0x52, 0x07, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x29, 0x0a, 0x0d, 0x48,
	0x65, 0x61, 0x72, 0x74, 0x42, 0x65, 0x61, 0x74, 0x52, 0x65, 0x73, 0x70, 0x12, 0x18, 0x0a, 0x07,
	0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x53,
	0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x22, 0x92, 0x01, 0x0a, 0x06, 0x50, 0x75, 0x74, 0x52, 0x65,
	0x71, 0x12, 0x12, 0x0a, 0x04, 0x50, 0x61, 0x74, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x04, 0x50, 0x61, 0x74, 0x68, 0x12, 0x18, 0x0a, 0x07, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x49, 0x64,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x49, 0x64, 0x12,
	0x12, 0x0a, 0x04, 0x44, 0x61, 0x74, 0x61, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x44,
	0x61, 0x74, 0x61, 0x12, 0x46, 0x0a, 0x10, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x4e, 0x6f, 0x64, 0x65, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a, 0x2e,
	0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x2e, 0x44, 0x61, 0x74, 0x61, 0x4e, 0x6f, 0x64,
	0x65, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x52, 0x10, 0x52, 0x65, 0x70, 0x6c, 0x69,
	0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x4e, 0x6f, 0x64, 0x65, 0x73, 0x22, 0x48, 0x0a, 0x10, 0x44,
	0x61, 0x74, 0x61, 0x4e, 0x6f, 0x64, 0x65, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x12,
	0x12, 0x0a, 0x04, 0x48, 0x6f, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x48,
	0x6f, 0x73, 0x74, 0x12, 0x20, 0x0a, 0x0b, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x50, 0x6f,
	0x72, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63,
	0x65, 0x50, 0x6f, 0x72, 0x74, 0x22, 0x23, 0x0a, 0x07, 0x50, 0x75, 0x74, 0x52, 0x65, 0x73, 0x70,
	0x12, 0x18, 0x0a, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x08, 0x52, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x22, 0x22, 0x0a, 0x06, 0x47, 0x65,
	0x74, 0x52, 0x65, 0x71, 0x12, 0x18, 0x0a, 0x07, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x49, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x49, 0x64, 0x22, 0x1d,
	0x0a, 0x07, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x12, 0x12, 0x0a, 0x04, 0x44, 0x61, 0x74,
	0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x44, 0x61, 0x74, 0x61, 0x32, 0x8e, 0x02,
	0x0a, 0x08, 0x44, 0x61, 0x74, 0x61, 0x4e, 0x6f, 0x64, 0x65, 0x12, 0x2f, 0x0a, 0x04, 0x50, 0x69,
	0x6e, 0x67, 0x12, 0x11, 0x2e, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x2e, 0x50, 0x69,
	0x6e, 0x67, 0x52, 0x65, 0x71, 0x1a, 0x12, 0x2e, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65,
	0x2e, 0x50, 0x69, 0x6e, 0x67, 0x52, 0x65, 0x73, 0x70, 0x22, 0x00, 0x12, 0x3e, 0x0a, 0x09, 0x48,
	0x65, 0x61, 0x72, 0x74, 0x42, 0x65, 0x61, 0x74, 0x12, 0x16, 0x2e, 0x64, 0x61, 0x74, 0x61, 0x6e,
	0x6f, 0x64, 0x65, 0x2e, 0x48, 0x65, 0x61, 0x72, 0x74, 0x42, 0x65, 0x61, 0x74, 0x52, 0x65, 0x71,
	0x1a, 0x17, 0x2e, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x2e, 0x48, 0x65, 0x61, 0x72,
	0x74, 0x42, 0x65, 0x61, 0x74, 0x52, 0x65, 0x73, 0x70, 0x22, 0x00, 0x12, 0x2c, 0x0a, 0x03, 0x50,
	0x75, 0x74, 0x12, 0x10, 0x2e, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x2e, 0x50, 0x75,
	0x74, 0x52, 0x65, 0x71, 0x1a, 0x11, 0x2e, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x2e,
	0x50, 0x75, 0x74, 0x52, 0x65, 0x73, 0x70, 0x22, 0x00, 0x12, 0x2c, 0x0a, 0x03, 0x47, 0x65, 0x74,
	0x12, 0x10, 0x2e, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x2e, 0x47, 0x65, 0x74, 0x52,
	0x65, 0x71, 0x1a, 0x11, 0x2e, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x2e, 0x47, 0x65,
	0x74, 0x52, 0x65, 0x73, 0x70, 0x22, 0x00, 0x12, 0x35, 0x0a, 0x06, 0x44, 0x65, 0x6c, 0x65, 0x74,
	0x65, 0x12, 0x13, 0x2e, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x2e, 0x44, 0x65, 0x6c,
	0x65, 0x74, 0x65, 0x52, 0x65, 0x71, 0x1a, 0x14, 0x2e, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64,
	0x65, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x22, 0x00, 0x42, 0x12,
	0x5a, 0x10, 0x2e, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f,
	0x64, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_datanode_datanode_proto_rawDescOnce sync.Once
	file_proto_datanode_datanode_proto_rawDescData = file_proto_datanode_datanode_proto_rawDesc
)

func file_proto_datanode_datanode_proto_rawDescGZIP() []byte {
	file_proto_datanode_datanode_proto_rawDescOnce.Do(func() {
		file_proto_datanode_datanode_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_datanode_datanode_proto_rawDescData)
	})
	return file_proto_datanode_datanode_proto_rawDescData
}

var file_proto_datanode_datanode_proto_msgTypes = make([]protoimpl.MessageInfo, 11)
var file_proto_datanode_datanode_proto_goTypes = []interface{}{
	(*DeleteReq)(nil),        // 0: datanode.DeleteReq
	(*DeleteResp)(nil),       // 1: datanode.DeleteResp
	(*PingReq)(nil),          // 2: datanode.PingReq
	(*PingResp)(nil),         // 3: datanode.PingResp
	(*HeartBeatReq)(nil),     // 4: datanode.HeartBeatReq
	(*HeartBeatResp)(nil),    // 5: datanode.HeartBeatResp
	(*PutReq)(nil),           // 6: datanode.PutReq
	(*DataNodeInstance)(nil), // 7: datanode.DataNodeInstance
	(*PutResp)(nil),          // 8: datanode.PutResp
	(*GetReq)(nil),           // 9: datanode.GetReq
	(*GetResp)(nil),          // 10: datanode.GetResp
}
var file_proto_datanode_datanode_proto_depIdxs = []int32{
	7,  // 0: datanode.PutReq.ReplicationNodes:type_name -> datanode.DataNodeInstance
	2,  // 1: datanode.DataNode.Ping:input_type -> datanode.PingReq
	4,  // 2: datanode.DataNode.HeartBeat:input_type -> datanode.HeartBeatReq
	6,  // 3: datanode.DataNode.Put:input_type -> datanode.PutReq
	9,  // 4: datanode.DataNode.Get:input_type -> datanode.GetReq
	0,  // 5: datanode.DataNode.Delete:input_type -> datanode.DeleteReq
	3,  // 6: datanode.DataNode.Ping:output_type -> datanode.PingResp
	5,  // 7: datanode.DataNode.HeartBeat:output_type -> datanode.HeartBeatResp
	8,  // 8: datanode.DataNode.Put:output_type -> datanode.PutResp
	10, // 9: datanode.DataNode.Get:output_type -> datanode.GetResp
	1,  // 10: datanode.DataNode.Delete:output_type -> datanode.DeleteResp
	6,  // [6:11] is the sub-list for method output_type
	1,  // [1:6] is the sub-list for method input_type
	1,  // [1:1] is the sub-list for extension type_name
	1,  // [1:1] is the sub-list for extension extendee
	0,  // [0:1] is the sub-list for field type_name
}

func init() { file_proto_datanode_datanode_proto_init() }
func file_proto_datanode_datanode_proto_init() {
	if File_proto_datanode_datanode_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_datanode_datanode_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_datanode_datanode_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteResp); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_datanode_datanode_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PingReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_datanode_datanode_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PingResp); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_datanode_datanode_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HeartBeatReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_datanode_datanode_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HeartBeatResp); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_datanode_datanode_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PutReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_datanode_datanode_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DataNodeInstance); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_datanode_datanode_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PutResp); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_datanode_datanode_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_datanode_datanode_proto_msgTypes[10].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetResp); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_datanode_datanode_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   11,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_proto_datanode_datanode_proto_goTypes,
		DependencyIndexes: file_proto_datanode_datanode_proto_depIdxs,
		MessageInfos:      file_proto_datanode_datanode_proto_msgTypes,
	}.Build()
	File_proto_datanode_datanode_proto = out.File
	file_proto_datanode_datanode_proto_rawDesc = nil
	file_proto_datanode_datanode_proto_goTypes = nil
	file_proto_datanode_datanode_proto_depIdxs = nil
}