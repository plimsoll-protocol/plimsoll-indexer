-- Reporters de-registered on chain keep their row, so the reports they posted
-- still resolve to an address the indexer can name. deleted_at records when the
-- registry revoked them; /v1/reporters hides those rows unless the caller asks
-- for them with ?include_deleted=true. Re-registering the address clears it.
ALTER TABLE reporters ADD COLUMN deleted_at TIMESTAMPTZ;

-- Reporters revoked before this column existed were only marked inactive.
UPDATE reporters SET deleted_at = updated_at WHERE NOT active AND deleted_at IS NULL;
