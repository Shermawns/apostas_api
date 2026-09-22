package usecases

import (
	"context"
	"time"

	"github.com/google/uuid"

	"apostas_api/internal/model"
)

type WalletStore interface {
	CreateWallet(context.Context, model.Wallet) error
	GetWallet(context.Context, uuid.UUID) (model.Wallet, error)
}

type Wallets struct{ store WalletStore }

func NewWallets(store WalletStore) *Wallets {
	return &Wallets{store}
}

func (u *Wallets) Open(ctx context.Context, player uuid.UUID, initial model.Money) (model.Wallet, error) {
	wallet, err := model.NewWallet(uuid.New(), player, initial, time.Now().UTC())
	if err != nil {
		return model.Wallet{}, err
	}
	if err := u.store.CreateWallet(ctx, wallet); err != nil {
		return model.Wallet{}, err
	}
	return wallet, nil
}
func (u *Wallets) Get(ctx context.Context, id uuid.UUID) (model.Wallet, error) {
	return u.store.GetWallet(ctx, id)
}
