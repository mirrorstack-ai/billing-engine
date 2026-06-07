-- Unqualified index name: an index is dropped from whatever schema its
-- table lives in; Postgres rejects a schema-qualified DROP INDEX where
-- the schema prefix names the index rather than its table.
DROP INDEX IF EXISTS acr_account_id_idx;
DROP TABLE IF EXISTS ms_billing.add_card_requests;
DROP TYPE IF EXISTS ms_billing.add_card_request_status;
