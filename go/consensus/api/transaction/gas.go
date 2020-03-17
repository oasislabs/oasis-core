package transaction

import (
	"encoding/binary"
	"math/big"

	fuzz "github.com/google/gofuzz"

	"github.com/oasislabs/oasis-core/go/common/errors"
	"github.com/oasislabs/oasis-core/go/common/quantity"
)

var (
	// ErrInsufficientFeeBalance is the error returned when there is insufficient
	// balance to pay consensus fees.
	ErrInsufficientFeeBalance = errors.New(moduleName, 2, "transaction: insufficient balance to pay fees")

	// ErrGasPriceTooLow is the error returned when the gas price is too low.
	ErrGasPriceTooLow = errors.New(moduleName, 3, "transaction: gas price too low")
)

// Gas is the consensus gas representation.
type Gas uint64

func (g *Gas) Fuzz(c fuzz.Continue) {
	*g = Gas(c.Uint64())
}

// Fee is the consensus transaction fee the sender wishes to pay for
// operations which require a fee to be paid to validators.
type Fee struct {
	// Amount is the fee amount to be paid.
	Amount quantity.Quantity `json:"amount"`
	// Gas is the maximum gas that a transaction can use.
	Gas Gas `json:"gas"`
}

// GasPrice returns the gas price implied by the amount and gas.
func (f Fee) GasPrice() *quantity.Quantity {
	if f.Amount.IsZero() || f.Gas == 0 {
		return quantity.NewQuantity()
	}

	var gasQ quantity.Quantity
	if err := gasQ.FromBigInt(big.NewInt(int64(f.Gas))); err != nil {
		// Should never happen.
		panic(err)
	}

	amt := f.Amount.Clone()
	if err := amt.Quo(&gasQ); err != nil {
		// Should never happen.
		panic(err)
	}
	return amt
}

// Costs defines gas costs for different operations.
type Costs map[Op]Gas

// Op identifies an operation that requires gas to run.
type Op string

func (o *Op) Fuzz(c fuzz.Continue) {
	var buf [16]byte
	for i := 0; i < len(buf); i += 8 {
		binary.LittleEndian.PutUint64(buf[i:], c.Uint64())
	}
	end := 16
	for end > 0 && buf[end - 1] == 0 {
		end--
	}
	*o = Op(buf[:end])
}
