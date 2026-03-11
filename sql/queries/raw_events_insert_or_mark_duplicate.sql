-- Insert a raw ingest event payload (minimal raw_events table)
INSERT INTO raw_events (
    raw_payload
)
VALUES (
    :raw_payload
)
RETURNING id;
