DROP TRIGGER wallet_history_protected ON wallets;
DROP TRIGGER wager_history_protected ON wager_transactions;
DROP FUNCTION protect_financial_history();
DROP TRIGGER ledger_matches_transaction ON wallet_ledger_entries;
DROP FUNCTION enforce_ledger_transaction();
