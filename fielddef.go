package main

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v8/market"
	"github.com/ipfs/go-cid"
)

type FieldDefinition interface {
	FieldPtr() interface{}
	Marshall() (interface{}, error)
	Unmarshall() error
}

type CidFieldDef struct {
	cidStr sql.NullString
	F      *cid.Cid
}

func (fd *CidFieldDef) FieldPtr() interface{} {
	return &fd.cidStr
}

func (fd *CidFieldDef) Marshall() (interface{}, error) {
	if fd.F == nil {
		return nil, nil
	}
	return fd.F.String(), nil
}

func (fd *CidFieldDef) Unmarshall() error {
	if !fd.cidStr.Valid {
		return nil
	}

	c, err := cid.Parse(fd.cidStr.String)
	if err != nil {
		return fmt.Errorf("parsing CID from string '%s': %w", fd.cidStr.String, err)
	}

	*fd.F = c
	return nil
}

type FieldDef struct {
	F interface{}
}

var _ FieldDefinition = (*FieldDef)(nil)

func (fd *FieldDef) FieldPtr() interface{} {
	return fd.F
}

func (fd *FieldDef) Marshall() (interface{}, error) {
	return fd.F, nil
}

func (fd *FieldDef) Unmarshall() error {
	return nil
}

type AddrFieldDef struct {
	Marshalled string
	F          *address.Address
}

func (fd *AddrFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *AddrFieldDef) Marshall() (interface{}, error) {
	return fd.F.String(), nil
}

func (fd *AddrFieldDef) Unmarshall() error {
	addr, err := address.NewFromString(fd.Marshalled)
	if err != nil {
		return fmt.Errorf("parsing address: %w", err)
	}

	*fd.F = addr
	return nil
}

type LabelFieldDef struct {
	Marshalled sql.NullString
	F          *market.DealLabel
}

func (fd *LabelFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *LabelFieldDef) Marshall() (interface{}, error) {
	if fd.F == nil {
		return nil, nil
	}

	// If the deal label is a string, add a ' character at the beginning
	if fd.F.IsString() {
		s, err := fd.F.ToString()
		if err != nil {
			return nil, fmt.Errorf("marshalling deal label as string: %w", err)
		}
		return "'" + s, nil
	}

	// The deal label is a byte array, so hex-encode the data and add an 'x' at
	// the beginning
	bz, err := fd.F.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("marshalling deal label as bytes: %w", err)
	}
	return "x" + hex.EncodeToString(bz), nil
}

func (fd *LabelFieldDef) Unmarshall() error {
	if !fd.Marshalled.Valid {
		return nil
	}

	if fd.Marshalled.String == "" || fd.Marshalled.String == "'" {
		*fd.F = market.EmptyDealLabel
		return nil
	}

	// If the first character is 'x' it's a hex-encoded byte array
	if fd.Marshalled.String[0] == 'x' {
		if len(fd.Marshalled.String) == 1 {
			return fmt.Errorf("cannot unmarshall empty string to hex")
		}
		bz, err := hex.DecodeString(fd.Marshalled.String[1:])
		if err != nil {
			return fmt.Errorf("unmarshalling hex string %s into bytes: %w", fd.Marshalled.String, err)
		}
		l, err := market.NewLabelFromBytes(bz)
		if err != nil {
			return fmt.Errorf("unmarshalling '%s' into label: %w", fd.Marshalled.String, err)
		}
		*fd.F = l
		return nil
	}

	// It's a string prefixed by the ' character
	l, err := market.NewLabelFromString(fd.Marshalled.String[1:])
	if err != nil {
		return fmt.Errorf("unmarshalling '%s' into label: %w", fd.Marshalled.String, err)
	}

	*fd.F = l
	return nil
}

type BigIntFieldDef struct {
	Marshalled sql.NullString
	F          *big.Int
}

func (fd *BigIntFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *BigIntFieldDef) Marshall() (interface{}, error) {
	if fd.F == nil {
		return nil, nil
	}
	return fd.F.String(), nil
}

func (fd *BigIntFieldDef) Unmarshall() error {
	if !fd.Marshalled.Valid {
		*fd.F = big.NewInt(0)
		return nil
	}

	i := big.NewInt(0)
	i.SetString(fd.Marshalled.String, 0)
	*fd.F = i
	return nil
}
