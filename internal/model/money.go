package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
)

var ErrInvalidMoney = errors.New("invalid money")
var ErrCurrencyMismatch = errors.New("currency mismatch")
var ErrOverflow = errors.New("money overflow")

var amountPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{1,2})?$`)
var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

type Money struct {
	minor    int64
	currency string
}

func ParseMoney(amount, currency string) (Money, error) {
	if !currencyPattern.MatchString(currency) || !amountPattern.MatchString(amount) {
		return Money{}, ErrInvalidMoney
	}
	var whole, fraction string
	for i, c := range amount {
		if c == '.' {
			whole, fraction = amount[:i], amount[i+1:]
			break
		}
	}
	if whole == "" {
		whole = amount
	}
	if len(fraction) == 1 {
		fraction += "0"
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || w > math.MaxInt64/100 {
		return Money{}, ErrOverflow
	}
	f := int64(0)
	if fraction != "" {
		f, _ = strconv.ParseInt(fraction, 10, 64)
	}
	if w == math.MaxInt64/100 && f > math.MaxInt64%100 {
		return Money{}, ErrOverflow
	}
	return Money{minor: w*100 + f, currency: currency}, nil
}

func MoneyFromMinor(minor int64, currency string) (Money, error) {
	if !currencyPattern.MatchString(currency) {
		return Money{}, ErrInvalidMoney
	}
	return Money{minor: minor, currency: currency}, nil
}

func (m Money) Valid() bool      { return currencyPattern.MatchString(m.currency) }
func (m Money) Minor() int64     { return m.minor }
func (m Money) Currency() string { return m.currency }
func (m Money) IsPositive() bool { return m.Valid() && m.minor > 0 }
func (m Money) IsZero() bool     { return m.Valid() && m.minor == 0 }

func (m Money) Add(n Money) (Money, error) {
	if !m.Valid() || !n.Valid() || m.currency != n.currency {
		return Money{}, ErrCurrencyMismatch
	}
	if (n.minor > 0 && m.minor > math.MaxInt64-n.minor) || (n.minor < 0 && m.minor < math.MinInt64-n.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + n.minor, currency: m.currency}, nil
}

func (m Money) Negate() (Money, error) {
	if !m.Valid() {
		return Money{}, ErrInvalidMoney
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

func (m Money) Sub(n Money) (Money, error) {
	negative, err := n.Negate()
	if err != nil {
		return Money{}, err
	}
	return m.Add(negative)
}

func (m Money) Compare(n Money) (int, error) {
	if !m.Valid() || !n.Valid() || m.currency != n.currency {
		return 0, ErrCurrencyMismatch
	}
	if m.minor < n.minor {
		return -1, nil
	}
	if m.minor > n.minor {
		return 1, nil
	}
	return 0, nil
}

func (m Money) Amount() (string, error) {
	if !m.Valid() {
		return "", ErrInvalidMoney
	}
	if m.minor < 0 {
		return fmt.Sprintf("-%d.%02d", -(m.minor / 100), -(m.minor % 100)), nil
	}
	return fmt.Sprintf("%d.%02d", m.minor/100, m.minor%100), nil
}

func (m Money) MarshalJSON() ([]byte, error) {
	if !m.Valid() {
		return nil, ErrInvalidMoney
	}
	amount, err := m.Amount()
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}{amount, m.currency})
}

func (m *Money) UnmarshalJSON(data []byte) error {
	var wire struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	parsed, err := ParseMoney(wire.Amount, wire.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
