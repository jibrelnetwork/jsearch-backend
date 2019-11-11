-- +goose Up
-- +goose StatementBegin
-- At this moment, Blocks Syncer could already sync some pairs into the table,
-- so use 'UPSERT' here.
INSERT INTO assets_summary_pairs
SELECT DISTINCT address, asset_address
FROM assets_summary
WHERE asset_address != ''
ON CONFLICT DO NOTHING;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 1;
-- +goose StatementEnd
