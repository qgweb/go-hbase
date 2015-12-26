package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/go-hbase/proto"
)

const filterPath = "org.apache.hadoop.hbase.filter."

type Filter interface {
	ToPBFilter() (*proto.Filter, error)
}

type PrefixFilter proto.PrefixFilter

func NewPrefixFilter(prefix []byte) *PrefixFilter {
	return &PrefixFilter{
		Prefix: prefix,
	}
}

func (f *PrefixFilter) ToPBFilter() (*proto.Filter, error) {
	serializedFilter, err := pb.Marshal((*proto.PrefixFilter)(f))
	if err != nil {
		return nil, errors.Trace(err)
	}
	filter := &proto.Filter{
		Name:             pb.String(filterPath + "PrefixFilter"),
		SerializedFilter: serializedFilter,
	}
	return filter, nil
}
