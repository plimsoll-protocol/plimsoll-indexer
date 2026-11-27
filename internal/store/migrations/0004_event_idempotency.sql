-- Older databases may have these tables without the event_id constraints
-- declared in 0001. Keep one row per event before adding the missing keys.
DELETE FROM supply_snapshots a
USING supply_snapshots b
WHERE a.event_id = b.event_id AND a.ctid > b.ctid;

DELETE FROM reserve_reports a
USING reserve_reports b
WHERE a.event_id = b.event_id AND a.ctid > b.ctid;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'supply_snapshots'::regclass
          AND contype IN ('p', 'u')
          AND conkey = ARRAY[(
              SELECT attnum
              FROM pg_attribute
              WHERE attrelid = 'supply_snapshots'::regclass
                AND attname = 'event_id'
          )]::smallint[]
    ) THEN
        ALTER TABLE supply_snapshots
            ADD CONSTRAINT supply_snapshots_event_id_unique UNIQUE (event_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'reserve_reports'::regclass
          AND contype IN ('p', 'u')
          AND conkey = ARRAY[(
              SELECT attnum
              FROM pg_attribute
              WHERE attrelid = 'reserve_reports'::regclass
                AND attname = 'event_id'
          )]::smallint[]
    ) THEN
        ALTER TABLE reserve_reports
            ADD CONSTRAINT reserve_reports_event_id_unique UNIQUE (event_id);
    END IF;
END
$$;
