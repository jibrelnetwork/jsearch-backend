-- +goose Up
-- +goose StatementBegin
ALTER TABLE dex_logs ADD COLUMN event_index BIGINT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE dex_logs DROP COLUMN event_index;
-- +goose StatementEnd
