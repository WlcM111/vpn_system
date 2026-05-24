DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_user_subscriptions_status'
    ) THEN
        ALTER TABLE user_subscriptions
            ADD CONSTRAINT chk_user_subscriptions_status
            CHECK (status IN ('none','trial','active','grace','expired'))
            NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_user_subscriptions_access_rev_non_negative'
    ) THEN
        ALTER TABLE user_subscriptions
            ADD CONSTRAINT chk_user_subscriptions_access_rev_non_negative
            CHECK (access_rev >= 0)
            NOT VALID;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_subscriptions_last_payment_id_non_empty
ON user_subscriptions (last_payment_id)
WHERE last_payment_id IS NOT NULL AND last_payment_id <> '';

UPDATE subscription_plans SET is_active = false WHERE code IN ('monthly','quarterly');