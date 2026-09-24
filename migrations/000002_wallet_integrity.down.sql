DROP TRIGGER ledger_requires_wallet ON wallet_ledger_entries;
DROP TRIGGER wallets_require_ledger ON wallets;
DROP TRIGGER wallet_identity_immutable ON wallets;
DROP FUNCTION enforce_wallet_ledger_integrity();
DROP FUNCTION protect_wallet_identity();
ALTER TABLE wager_transactions DROP CONSTRAINT wager_amount_by_kind;
ALTER TABLE wallet_ledger_entries DROP CONSTRAINT ledger_wallet_version_unique;
ALTER TABLE wallet_ledger_entries DROP CONSTRAINT ledger_wallet_version_positive;
ALTER TABLE wallet_ledger_entries DROP COLUMN wallet_version;
