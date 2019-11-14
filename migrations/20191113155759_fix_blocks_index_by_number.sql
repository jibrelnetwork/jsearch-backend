-- +goose Up
-- +goose StatementBegin
DROP INDEX ix_blocks_number;
CREATE INDEX ix_blocks_number ON blocks USING btree (number);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 1;
-- +goose StatementEnd
