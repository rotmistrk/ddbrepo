package ddbrepo

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

const (
	TagDdb          = "ddb"
	TagDdbGsi       = "ddb-gsi"
	TagItemHashKey  = "hash-key"
	TagItemRangeKey = "range-key"
	TagItemRequired = "required"
	TagItemTtlField = "expire"
	TagItemIgnore   = "ignore"
	TagVersion      = "version"
)

type fieldSpec struct {
	name       string
	required   bool
	isHashKey  bool
	isRangeKey bool
	isVersion  bool
	isTtlField bool
	gsiHash    map[string]bool
}

type parseProps interface {
	AllowUntaggedFields() bool
	LowercaseUntaggedFields() bool
}

func newFieldSpec(props parseProps, field *reflect.StructField) (*fieldSpec, error) {
	tagStr, ok := field.Tag.Lookup(TagDdb)
	gsiStr, gsiOk := field.Tag.Lookup(TagDdbGsi)
	if (ok || gsiOk) && !field.IsExported() {
		return nil, errors.New("can't handle ddb tags on private field " + field.Name)
	}
	if !field.IsExported() {
		return nil, nil
	}
	spec := &fieldSpec{
		name: field.Name,
	}
	if gsiOk {
		gsis := strings.Split(gsiStr, ",")
		spec.gsiHash = make(map[string]bool)
		for _, v := range gsis {
			v = strings.TrimSpace(v)
			parts := strings.Split(v, " ")
			if len(parts) != 2 {
				return nil, fmt.Errorf("malformed annotation element %v for %v of %v", v, TagDdbGsi, field.Name)
			}
			if _, found := spec.gsiHash[parts[0]]; found {
				return nil, fmt.Errorf("duplicate gsi spec for gsi %v of %v", parts[0], field.Name)
			}
			switch parts[1] {
			case TagItemRangeKey:
				spec.gsiHash[parts[0]] = false
			case TagItemHashKey:
				spec.gsiHash[parts[0]] = true
			default:
				return nil, fmt.Errorf("unexpected key type in %v on %v for field %v", v, TagDdbGsi, field.Name)
			}
		}
	}
	if ok {
		tags := strings.Split(tagStr, ",")
		if tag0 := strings.TrimSpace(tags[0]); len(tag0) > 0 {
			spec.name = tag0
		}
		for _, v := range tags[1:] {
			switch strings.TrimSpace(v) {
			case TagItemRequired:
				spec.required = true
			case TagItemHashKey:
				spec.isHashKey = true
			case TagItemRangeKey:
				spec.isRangeKey = true
			case TagItemTtlField:
				spec.isTtlField = true
			case TagVersion:
				spec.isVersion = true
			case TagItemIgnore:
				return nil, nil
			default:
				return nil, errors.New("unknown annotation: [" + v + "]")
			}
		}
		if spec.isRangeKey && spec.isHashKey {
			return nil, errors.New("both " + TagItemHashKey + " and " + TagItemRangeKey + " are set for " + field.Name)
		}
	} else if !props.AllowUntaggedFields() {
		return nil, nil
	} else if props.LowercaseUntaggedFields() && len(spec.name) > 0 {
		spec.name = mangleName(spec)
	}

	return spec, nil
}

func mangleName(spec *fieldSpec) string {
	return strings.ToLower(spec.name[0:1]) + spec.name[1:]
}

func (s fieldSpec) IsRequired() bool {
	return s.required
}

func (s fieldSpec) IsHashKey() bool {
	return s.isHashKey
}

func (s fieldSpec) IsRangeKey() bool {
	return s.isRangeKey
}

func (s fieldSpec) IsKey() bool {
	return s.isHashKey || s.isRangeKey
}

func (s fieldSpec) IsTtlField() bool {
	return s.isTtlField
}

func (s fieldSpec) FieldName() string {
	return s.name
}

func (s fieldSpec) IsVersionField() bool {
	return s.isVersion
}
