package brbv1_test

import (
	"testing"

	brbv1 "github.com/alkval/gorums-brb/proto/brb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Field 4 used to be `bool optimistic`. A binary built from that schema reads
// the READY-digest variant (2) as optimistic=true, so it cannot tell the two
// variants apart. This is why mixed-version groups are unsupported.
func TestLegacySchemaMisreadsDigestVariant(t *testing.T) {
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    new("legacy.proto"),
		Package: new("legacy"),
		Syntax:  new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: new("PhaseMessage"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     new("optimistic"),
				Number:   proto.Int32(4),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				JsonName: new("optimistic"),
			}},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	legacy := file.Messages().ByName("PhaseMessage")
	field := legacy.Fields().ByName("optimistic")
	for variant, wantOptimistic := range map[brbv1.Variant]bool{
		brbv1.Variant_VARIANT_BRACHA:              false,
		brbv1.Variant_VARIANT_OPTIMISTIC:          true,
		brbv1.Variant_VARIANT_BRACHA_READY_DIGEST: true, // indistinguishable from optimistic
	} {
		wire, err := proto.Marshal(brbv1.PhaseMessage_builder{Variant: variant}.Build())
		if err != nil {
			t.Fatal(err)
		}
		old := dynamicpb.NewMessage(legacy)
		if err := proto.Unmarshal(wire, old); err != nil {
			t.Fatal(err)
		}
		if got := old.Get(field).Bool(); got != wantOptimistic {
			t.Fatalf("legacy schema read %v as optimistic=%t, want %t", variant, got, wantOptimistic)
		}
	}
}
