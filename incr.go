package hbase

import (
	"bytes"
	"math"

	pb "github.com/golang/protobuf/proto"
	"github.com/pingcap/go-hbase/proto"
	"encoding/binary"
)

type Incr struct {
	Row        []byte
	Families   [][]byte
	Qualifiers [][][]byte
	Values     [][][]byte
	Timestamp  uint64
}

func NewIncr(row []byte) *Incr {
	return &Incr{
		Row:        row,
		Families:   make([][]byte, 0),
		Qualifiers: make([][][]byte, 0),
		Values:     make([][][]byte, 0),
	}
}

func (p *Incr) GetRow() []byte {
	return p.Row
}

func (p *Incr) AddValue(family, qual, value []byte) *Incr {
	pos := p.posOfFamily(family)
	if pos == -1 {
		p.Families = append(p.Families, family)
		p.Qualifiers = append(p.Qualifiers, make([][]byte, 0))
		p.Values = append(p.Values, make([][]byte, 0))

		pos = p.posOfFamily(family)
	}

	p.Qualifiers[pos] = append(p.Qualifiers[pos], qual)
	p.Values[pos] = append(p.Values[pos], value)
	return p
}

func (p *Incr) AddStringValue(family, column  string,value int64) *Incr {
	var bufvalue = make([]byte,8)
	binary.BigEndian.PutUint64(bufvalue, uint64(value))
	return p.AddValue([]byte(family), []byte(column), bufvalue)
}

func (p *Incr) AddTimestamp(ts uint64) *Incr {
	if ts == 0 {
		p.Timestamp = math.MaxInt64
	} else {
		p.Timestamp = ts
	}
	return p
}

func (p *Incr) posOfFamily(family []byte) int {
	for p, v := range p.Families {
		if bytes.Equal(family, v) {
			return p
		}
	}
	return -1
}

func (p *Incr) ToProto() pb.Message {
	bufvalue := &proto.MutationProto{
		Row:        p.Row,
		MutateType: proto.MutationProto_INCREMENT.Enum(),
	}

	for i, family := range p.Families {
		cv := &proto.MutationProto_ColumnValue{
			Family: family,
		}

		for j := range p.Qualifiers[i] {
			cv.QualifierValue = append(cv.QualifierValue, &proto.MutationProto_ColumnValue_QualifierValue{
				Qualifier: p.Qualifiers[i][j],
				Value:     p.Values[i][j],
				Timestamp: pb.Uint64(p.Timestamp),
			})
		}

		bufvalue.ColumnValue = append(bufvalue.ColumnValue, cv)
	}

	return bufvalue
}
