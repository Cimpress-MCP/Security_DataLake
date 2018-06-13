package datalakemsg

import (
	"encoding/json"
)

// base - base class used to provide Pack and Unpack methods
// private type used for inheritance
type base struct {
	Type    string      `json:"$type"`
	Version string      `json:"version"`
	selfP   interface{} // stores a pointer to the actual pack function
}

//SetSelfP - sets self pack interface
func (b *base) SetSelfP(p interface{}) {
	b.selfP = p
}

//GetJSON - returns a marshaled JSON representation of this object
func (b *base) GetJSON() ([]byte, error) {
	return json.Marshal(b.selfP)
}

//SetType - sets type string for this message
func (b *base) SetType(typeStr string) {
	b.Type = typeStr
}

//SetVersion - sets version string for this message
func (b *base) SetVersion(version string) {
	b.Version = version
}
