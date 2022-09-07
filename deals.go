package main

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"github.com/filecoin-project/lotus/api"
	"strings"
)

// Used for SELECT statements: "ID, CreatedAt, ..."
var dealFields []string
var dealFieldsStr = ""

func init() {
	var deal DealModel
	def := newDealAccessor(nil, &deal)
	dealFields = make([]string, 0, len(def.def))
	for k := range def.def {
		dealFields = append(dealFields, k)
	}
	dealFieldsStr = strings.Join(dealFields, ", ")
}

type Scannable interface {
	Scan(dest ...interface{}) error
}

type DealModel struct {
	ID string
	api.MarketDeal
}

type dealAccessor struct {
	db   *sql.DB
	deal *DealModel
	def  map[string]FieldDefinition
}

func (d *DealsDB) newDealDef(deal *DealModel) *dealAccessor {
	return newDealAccessor(d.db, deal)
}

func newDealAccessor(db *sql.DB, deal *DealModel) *dealAccessor {
	return &dealAccessor{
		db:   db,
		deal: deal,
		def: map[string]FieldDefinition{
			"ID":                   &FieldDef{F: deal.ID},
			"PieceCID":             &CidFieldDef{F: &deal.Proposal.PieceCID},
			"PieceSize":            &FieldDef{F: &deal.Proposal.PieceSize},
			"VerifiedDeal":         &FieldDef{F: &deal.Proposal.VerifiedDeal},
			"ClientAddress":        &AddrFieldDef{F: &deal.Proposal.Client},
			"ProviderAddress":      &AddrFieldDef{F: &deal.Proposal.Provider},
			"Label":                &LabelFieldDef{F: &deal.Proposal.Label},
			"StartEpoch":           &FieldDef{F: &deal.Proposal.StartEpoch},
			"EndEpoch":             &FieldDef{F: &deal.Proposal.EndEpoch},
			"StoragePricePerEpoch": &BigIntFieldDef{F: &deal.Proposal.StoragePricePerEpoch},
			"ProviderCollateral":   &BigIntFieldDef{F: &deal.Proposal.ProviderCollateral},
			"ClientCollateral":     &BigIntFieldDef{F: &deal.Proposal.ClientCollateral},
			"SectorStartEpoch":     &FieldDef{F: &deal.State.SectorStartEpoch},
			"LastUpdatedEpoch":     &FieldDef{F: &deal.State.LastUpdatedEpoch},
			"SlashEpoch":           &FieldDef{F: &deal.State.SlashEpoch},
		},
	}
}

func (d *dealAccessor) scan(row Scannable) error {
	// For each field
	var dest []interface{}
	for _, name := range dealFields {
		// Get a pointer to the field that will receive the scanned value
		fieldDef := d.def[name]
		dest = append(dest, fieldDef.FieldPtr())
	}

	// Scan the row into each pointer
	err := row.Scan(dest...)
	if err != nil {
		return fmt.Errorf("scanning deal row: %w", err)
	}

	// For each field
	for name, fieldDef := range d.def {
		// Unmarshall the scanned value into deal object
		err := fieldDef.Unmarshall()
		if err != nil {
			return fmt.Errorf("unmarshalling db field %s: %s", name, err)
		}
	}
	return nil
}

func (d *dealAccessor) insert(ctx context.Context, tx *sql.Tx) error {
	// For each field
	var values []interface{}
	placeholders := make([]string, 0, len(values))
	for _, name := range dealFields {
		// Add a placeholder "?"
		fieldDef := d.def[name]
		placeholders = append(placeholders, "?")

		// Marshall the field into a value that can be stored in the database
		v, err := fieldDef.Marshall()
		if err != nil {
			return err
		}
		values = append(values, v)
	}

	// Execute the INSERT
	qry := "INSERT INTO Deals (" + dealFieldsStr + ") "
	qry += "VALUES (" + strings.Join(placeholders, ",") + ")"
	_, err := tx.ExecContext(ctx, qry, values...)
	return err
}

func (d *dealAccessor) update(ctx context.Context) error {
	// For each field
	var values []interface{}
	setNames := make([]string, 0, len(values))
	for _, name := range dealFields {
		// Skip the ID field
		if name == "ID" {
			continue
		}

		// Add "fieldName = ?"
		fieldDef := d.def[name]
		setNames = append(setNames, name+" = ?")

		// Marshall the field into a value that can be stored in the database
		v, err := fieldDef.Marshall()
		if err != nil {
			return err
		}
		values = append(values, v)
	}

	// Execute the UPDATE
	qry := "UPDATE Deals "
	qry += "SET " + strings.Join(setNames, ", ")

	qry += "WHERE ID = ?"
	values = append(values, d.deal.ID)

	_, err := d.db.ExecContext(ctx, qry, values...)
	return err
}

type DealsDB struct {
	db *sql.DB
}

func NewDealsDB(db *sql.DB) *DealsDB {
	return &DealsDB{db: db}
}

func (d *DealsDB) Insert(ctx context.Context, deals []*DealModel) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	for _, deal := range deals {
		if err := d.newDealDef(deal).insert(ctx, tx); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *DealsDB) Update(ctx context.Context, deal *DealModel) error {
	return d.newDealDef(deal).update(ctx)
}

func (d *DealsDB) ByID(ctx context.Context, id uint64) (*DealModel, error) {
	qry := "SELECT " + dealFieldsStr + " FROM Deals WHERE id=?"
	row := d.db.QueryRowContext(ctx, qry, id)
	return d.scanRow(row)
}

func (d *DealsDB) scanRow(row Scannable) (*DealModel, error) {
	var deal DealModel
	err := d.newDealDef(&deal).scan(row)
	return &deal, err
}
