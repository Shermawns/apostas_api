package model

import "errors"

var ErrInvalidTransaction = errors.New("invalid transaction")
var ErrTerminalTransaction = errors.New("terminal transaction")

type Kind string

const (
	Bet      Kind = "BET"
	Win      Kind = "WIN"
	Loss     Kind = "LOSS"
	Refund   Kind = "REFUND"
	Rollback Kind = "ROLLBACK"
)

type Status string

const (
	Pending          Status = "PENDING"
	PendingReference Status = "PENDING_REFERENCE"
	Processed        Status = "PROCESSED"
	Rejected         Status = "REJECTED"
	Failed           Status = "FAILED"
)

func ValidateOperation(kind Kind, money Money, reference string) error {
	if !money.Valid() || money.Minor() < 0 {
		return ErrInvalidTransaction
	}
	switch kind {
	case Bet, Win:
		if !money.IsPositive() || (kind == Bet && reference != "") {
			return ErrInvalidTransaction
		}
	case Loss:
		if !money.IsZero() || reference != "" {
			return ErrInvalidTransaction
		}
	case Refund, Rollback:
		if !money.IsPositive() || reference == "" {
			return ErrInvalidTransaction
		}
	default:
		return ErrInvalidTransaction
	}
	return nil
}

type TransactionState struct{ status Status }

func NewTransactionState() TransactionState { return TransactionState{status: Pending} }
func RehydrateTransactionState(status Status) (TransactionState, error) {
	switch status {
	case Pending, PendingReference, Processed, Rejected, Failed:
		return TransactionState{status}, nil
	}
	return TransactionState{}, ErrInvalidTransaction
}
func (s TransactionState) Status() Status { return s.status }
func (s TransactionState) Valid() bool {
	switch s.status {
	case Pending, PendingReference, Processed, Rejected, Failed:
		return true
	default:
		return false
	}
}
func (s *TransactionState) Transition(next Status) error {
	if !s.Valid() {
		return ErrInvalidTransaction
	}
	if s.status == Processed || s.status == Rejected || s.status == Failed {
		return ErrTerminalTransaction
	}
	if next != PendingReference && next != Processed && next != Rejected && next != Failed {
		return ErrInvalidTransaction
	}
	s.status = next
	return nil
}
