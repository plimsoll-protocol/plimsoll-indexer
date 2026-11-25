ALTER TABLE reporters
    ADD COLUMN display_name TEXT NOT NULL DEFAULT '',
    ADD COLUMN website_uri TEXT,
    ADD COLUMN contact_hash TEXT;

UPDATE reporters SET display_name = name WHERE display_name = '';
