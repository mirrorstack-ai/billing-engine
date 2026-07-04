-- Down for 027 — drop the app existence mirror. The trigger and index drop
-- with the table. Rolling back reverts base-fee charging to the pre-027
-- posture (usage arrears only): the charge spine treats an empty roster as
-- base 0, so no code change is required to survive a rollback window.

DROP TABLE IF EXISTS ms_billing.apps;
