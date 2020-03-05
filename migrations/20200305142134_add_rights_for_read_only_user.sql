-- +goose Up
-- +goose StatementBegin
GRANT SELECT on dex_logs TO jsearch_main_ro_user;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
REVOKE SELECT on dex_logs FROM jsearch_main_ro_user;
-- +goose StatementEnd
