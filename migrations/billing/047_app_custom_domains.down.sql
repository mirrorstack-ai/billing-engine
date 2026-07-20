-- Migration 047 (down) — remove the custom-domain billing mirror.

DROP TABLE IF EXISTS ms_billing.app_custom_domains;
