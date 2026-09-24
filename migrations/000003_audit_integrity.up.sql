CREATE FUNCTION enforce_ledger_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    source wager_transactions%ROWTYPE;
    referenced_kind text;
    expected_direction text;
BEGIN
    SELECT * INTO source FROM wager_transactions WHERE id = NEW.transaction_id;
    IF source.id IS NULL OR source.wallet_id <> NEW.wallet_id
       OR source.amount_minor <> NEW.amount_minor OR source.status <> 'PROCESSED'
       OR source.currency <> (SELECT currency FROM wallets WHERE id = NEW.wallet_id) THEN
        RAISE EXCEPTION 'ledger entry does not match its processed transaction';
    END IF;
    CASE source.kind
        WHEN 'OPENING' THEN expected_direction := 'CREDIT';
        WHEN 'BET' THEN expected_direction := 'DEBIT';
        WHEN 'WIN', 'REFUND' THEN expected_direction := 'CREDIT';
        WHEN 'ROLLBACK' THEN
            SELECT kind INTO referenced_kind FROM wager_transactions WHERE id = source.reference_transaction_id;
            IF referenced_kind = 'BET' THEN expected_direction := 'CREDIT';
            ELSIF referenced_kind IN ('WIN', 'REFUND') THEN expected_direction := 'DEBIT';
            ELSE RAISE EXCEPTION 'rollback has no valid processed reference';
            END IF;
        ELSE RAISE EXCEPTION 'transaction kind cannot create a ledger entry';
    END CASE;
    IF NEW.direction <> expected_direction THEN
        RAISE EXCEPTION 'ledger direction disagrees with transaction kind';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER ledger_matches_transaction
AFTER INSERT ON wallet_ledger_entries DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_ledger_transaction();

CREATE FUNCTION protect_financial_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'financial history cannot be deleted';
    END IF;
    IF TG_TABLE_NAME = 'wallets' THEN
        RETURN NEW;
    END IF;
    IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
        RAISE EXCEPTION 'terminal transaction cannot change';
    END IF;
    IF ROW(NEW.id, NEW.external_transaction_id, NEW.provider_id, NEW.idempotency_key,
           NEW.payload_hash, NEW.wallet_id, NEW.player_id, NEW.round_id, NEW.game_id,
           NEW.kind, NEW.amount_minor, NEW.currency, NEW.reference_external_transaction_id,
           NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.id, OLD.external_transaction_id, OLD.provider_id, OLD.idempotency_key,
           OLD.payload_hash, OLD.wallet_id, OLD.player_id, OLD.round_id, OLD.game_id,
           OLD.kind, OLD.amount_minor, OLD.currency, OLD.reference_external_transaction_id,
           OLD.created_at) THEN
        RAISE EXCEPTION 'transaction identity and amount are immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER wager_history_protected BEFORE UPDATE OR DELETE ON wager_transactions
FOR EACH ROW EXECUTE FUNCTION protect_financial_history();
CREATE TRIGGER wallet_history_protected BEFORE DELETE ON wallets
FOR EACH ROW EXECUTE FUNCTION protect_financial_history();
