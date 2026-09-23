CREATE TABLE wallets (
    id uuid PRIMARY KEY,
    player_id uuid NOT NULL,
    currency char(3) NOT NULL,
    balance_minor bigint NOT NULL CHECK (balance_minor >= 0),
    version bigint NOT NULL CHECK (version >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (player_id, currency)
);

CREATE TABLE wager_transactions (
    id uuid PRIMARY KEY,
    external_transaction_id text,
    provider_id text,
    idempotency_key text,
    payload_hash char(64),
    wallet_id uuid NOT NULL REFERENCES wallets(id),
    player_id uuid NOT NULL,
    round_id text,
    game_id text,
    kind text NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency char(3) NOT NULL,
    reference_external_transaction_id text,
    reference_transaction_id uuid REFERENCES wager_transactions(id),
    status text NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
    failure_code text,
    result_balance_minor bigint,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((kind = 'OPENING' AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL)
        OR (kind <> 'OPENING' AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)),
    CHECK ((kind = 'LOSS' AND amount_minor = 0) OR (kind IN ('OPENING','BET','WIN','REFUND','ROLLBACK') AND amount_minor >= 0))
);
CREATE UNIQUE INDEX wager_provider_external_unique ON wager_transactions(provider_id, external_transaction_id) WHERE provider_id IS NOT NULL;
CREATE UNIQUE INDEX wager_provider_key_unique ON wager_transactions(provider_id, idempotency_key) WHERE provider_id IS NOT NULL;
CREATE UNIQUE INDEX wager_opening_unique ON wager_transactions(wallet_id) WHERE kind = 'OPENING';
CREATE UNIQUE INDEX wager_single_reversal_unique ON wager_transactions(reference_transaction_id) WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK');

CREATE TABLE wallet_ledger_entries (
    id uuid PRIMARY KEY,
    wallet_id uuid NOT NULL REFERENCES wallets(id),
    transaction_id uuid NOT NULL REFERENCES wager_transactions(id),
    direction text NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    balance_before_minor bigint NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor bigint NOT NULL CHECK (balance_after_minor >= 0),
	wallet_version bigint NOT NULL CHECK (wallet_version >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (wallet_id, transaction_id),
	UNIQUE (wallet_id, wallet_version),
    CHECK ((direction = 'CREDIT' AND balance_after_minor::numeric = balance_before_minor::numeric + amount_minor::numeric)
        OR (direction = 'DEBIT' AND balance_after_minor::numeric = balance_before_minor::numeric - amount_minor::numeric))
);
CREATE FUNCTION prevent_ledger_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ledger entries are immutable';
END;
$$;
CREATE TRIGGER wallet_ledger_immutable BEFORE UPDATE OR DELETE ON wallet_ledger_entries FOR EACH ROW EXECUTE FUNCTION prevent_ledger_change();

CREATE FUNCTION enforce_wallet_ledger_integrity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'wallets' THEN
        IF TG_OP = 'UPDATE' AND NEW.balance_minor = OLD.balance_minor AND NEW.version = OLD.version THEN
            RETURN NULL;
        END IF;
        IF TG_OP = 'UPDATE' AND (NEW.balance_minor = OLD.balance_minor OR NEW.version <> OLD.version + 1) THEN
            RAISE EXCEPTION 'wallet balance changes require exactly one new wallet version';
        END IF;
        IF (TG_OP = 'UPDATE' OR NEW.balance_minor <> 0) AND NOT EXISTS (
            SELECT 1 FROM wallet_ledger_entries
            WHERE wallet_id = NEW.id
              AND wallet_version = NEW.version
              AND balance_after_minor = NEW.balance_minor
        ) THEN
            RAISE EXCEPTION 'wallet balance requires a matching ledger entry';
        END IF;
        RETURN NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM wallets
        WHERE id = NEW.wallet_id
          AND version = NEW.wallet_version
          AND balance_minor = NEW.balance_after_minor
    ) THEN
        RAISE EXCEPTION 'ledger entry must match the wallet balance and version';
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

CREATE TABLE inbox_messages (
    consumer_name text NOT NULL,
    message_id text NOT NULL,
    payload_hash char(64) NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox_events (
    id uuid PRIMARY KEY,
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    claimed_until timestamptz
);
CREATE INDEX outbox_ready_idx ON outbox_events(next_attempt_at) WHERE published_at IS NULL;
