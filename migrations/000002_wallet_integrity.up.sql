ALTER TABLE wallet_ledger_entries ADD COLUMN wallet_version bigint;
ALTER TABLE wallet_ledger_entries DISABLE TRIGGER wallet_ledger_immutable;

WITH numbered AS (
    SELECT l.id,
           row_number() OVER (PARTITION BY l.wallet_id ORDER BY l.created_at, l.id)
             + CASE WHEN EXISTS (
                 SELECT 1 FROM wager_transactions t
                 WHERE t.wallet_id = l.wallet_id AND t.kind = 'OPENING'
               ) THEN 0 ELSE 1 END AS version
    FROM wallet_ledger_entries l
)
UPDATE wallet_ledger_entries l SET wallet_version = numbered.version
FROM numbered WHERE l.id = numbered.id;
ALTER TABLE wallet_ledger_entries ENABLE TRIGGER wallet_ledger_immutable;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM wallets w
        WHERE w.version <> COALESCE((
            SELECT max(l.wallet_version) FROM wallet_ledger_entries l WHERE l.wallet_id = w.id
        ), 1)
        OR w.balance_minor <> COALESCE((
            SELECT l.balance_after_minor FROM wallet_ledger_entries l
            WHERE l.wallet_id = w.id ORDER BY l.wallet_version DESC LIMIT 1
        ), 0)
    ) THEN
        RAISE EXCEPTION 'existing wallet balance or version disagrees with ledger';
    END IF;
END;
$$;

ALTER TABLE wallet_ledger_entries ALTER COLUMN wallet_version SET NOT NULL;
ALTER TABLE wallet_ledger_entries ADD CONSTRAINT ledger_wallet_version_positive CHECK (wallet_version >= 1);
ALTER TABLE wallet_ledger_entries ADD CONSTRAINT ledger_wallet_version_unique UNIQUE (wallet_id, wallet_version);
ALTER TABLE wager_transactions ADD CONSTRAINT wager_amount_by_kind CHECK (
    (kind = 'LOSS' AND amount_minor = 0)
    OR (kind = 'OPENING' AND amount_minor >= 0)
    OR (kind IN ('BET','WIN','REFUND','ROLLBACK') AND amount_minor > 0)
);

CREATE FUNCTION protect_wallet_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id OR NEW.currency <> OLD.currency
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wallet identity and currency are immutable';
    END IF;
    IF NEW.version <> OLD.version AND NEW.balance_minor = OLD.balance_minor THEN
        RAISE EXCEPTION 'wallet version changes only with balance';
    END IF;
    IF NEW.balance_minor <> OLD.balance_minor AND NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'wallet balance requires the next version';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER wallet_identity_immutable BEFORE UPDATE ON wallets
FOR EACH ROW EXECUTE FUNCTION protect_wallet_identity();

CREATE FUNCTION enforce_wallet_ledger_integrity() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    entry wallet_ledger_entries%ROWTYPE;
    previous_balance bigint;
BEGIN
    IF TG_TABLE_NAME = 'wallets' THEN
        IF TG_OP = 'UPDATE' AND NEW.balance_minor = OLD.balance_minor THEN
            RETURN NULL;
        END IF;
        SELECT * INTO entry FROM wallet_ledger_entries
        WHERE wallet_id = NEW.id AND wallet_version = NEW.version;
        IF TG_OP = 'INSERT' AND NEW.version <> 1 THEN
            RAISE EXCEPTION 'wallet initial version must be one';
        END IF;
        IF TG_OP = 'INSERT' AND NEW.balance_minor = 0 AND entry.id IS NULL THEN
            RETURN NULL;
        END IF;
        IF entry.id IS NULL OR entry.balance_after_minor <> NEW.balance_minor THEN
            RAISE EXCEPTION 'wallet balance requires a matching ledger entry';
        END IF;
        IF TG_OP = 'UPDATE' AND entry.balance_before_minor <> OLD.balance_minor THEN
            RAISE EXCEPTION 'ledger previous balance disagrees with wallet';
        END IF;
        IF TG_OP = 'INSERT' AND (entry.balance_before_minor <> 0 OR entry.direction <> 'CREDIT') THEN
            RAISE EXCEPTION 'opening ledger must credit from zero';
        END IF;
        RETURN NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM wallets w WHERE w.id = NEW.wallet_id
          AND w.version = NEW.wallet_version
          AND w.balance_minor = NEW.balance_after_minor
    ) THEN
        RAISE EXCEPTION 'ledger entry must match wallet balance and version';
    END IF;
    IF NEW.wallet_version = 1 THEN
        IF NEW.balance_before_minor <> 0 OR NEW.direction <> 'CREDIT'
           OR NOT EXISTS (
               SELECT 1 FROM wager_transactions t WHERE t.id = NEW.transaction_id
                 AND t.kind = 'OPENING' AND t.wallet_id = NEW.wallet_id
           ) THEN
            RAISE EXCEPTION 'version one ledger must be the opening credit';
        END IF;
    ELSE
        SELECT l.balance_after_minor INTO previous_balance FROM wallet_ledger_entries l
        WHERE l.wallet_id = NEW.wallet_id AND l.wallet_version = NEW.wallet_version - 1;
        IF previous_balance IS NULL THEN
            IF NEW.wallet_version <> 2 OR NEW.balance_before_minor <> 0 THEN
                RAISE EXCEPTION 'ledger version gap';
            END IF;
        ELSIF previous_balance <> NEW.balance_before_minor THEN
            RAISE EXCEPTION 'ledger balance chain is broken';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER wallets_require_ledger
AFTER INSERT OR UPDATE ON wallets DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_wallet_ledger_integrity();
CREATE CONSTRAINT TRIGGER ledger_requires_wallet
AFTER INSERT ON wallet_ledger_entries DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_wallet_ledger_integrity();
