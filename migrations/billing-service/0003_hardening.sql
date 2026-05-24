ALTER TABLE payments
    ADD COLUMN IF NOT EXISTS command_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_command_id_non_empty
ON payments (command_id)
WHERE command_id <> '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_payment_id_non_empty
ON payments (payment_id)
WHERE payment_id <> '';

ALTER TABLE billing_recurring_profiles
    ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_billing_recurring_processing_locked
ON billing_recurring_profiles (status, locked_at)
WHERE status = 'processing';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_billing_recurring_status'
    ) THEN
        ALTER TABLE billing_recurring_profiles
            ADD CONSTRAINT chk_billing_recurring_status
            CHECK (status IN ('active','processing','retry','grace','expiring','expired','disabled'))
            NOT VALID;
    END IF;
END $$;