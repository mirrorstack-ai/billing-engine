-- 097 down: drop the window column (its CHECK goes with it). Every row reverts
-- to the period window, the only one the pre-097 code knows.
ALTER TABLE ms_billing.budgets
    DROP COLUMN IF EXISTS spend_window;
