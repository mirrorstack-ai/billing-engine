-- 093 down: drop the transition memory. The notifier then treats every owner
-- as first-seen again (a still-blocked account other than NO_USABLE_CARD would
-- be mailed once more if the notice flag is on), so turn the flag off first.
DROP TABLE IF EXISTS ms_billing.standing_state;
