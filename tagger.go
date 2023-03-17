package datax

import (
	"fmt"
	"reflect"
)

var tags = []string{"datax", "json", "bson"}

type tagger struct {
}

func newTagger() *tagger {
	return &tagger{}
}

func (tgr *tagger) MakeTag(t reflect.Type, fieldIndex int) reflect.StructTag {
	field := t.Field(fieldIndex)
	value := ""
	found := false
	for i := range tags {
		value, found = field.Tag.Lookup(tags[i])
		if found {
			break
		}
	}
	if !found {
		return field.Tag
	}
	return reflect.StructTag(fmt.Sprintf(`msgpack:"%s"`, value))
}
