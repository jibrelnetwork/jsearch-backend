-- +goose Up
-- +goose StatementBegin
CREATE TABLE assets_summary_pairs
(
    address       character varying NOT NULL,
    asset_address character varying NOT NULL
);

INSERT INTO assets_summary_pairs
SELECT DISTINCT address, asset_address
FROM assets_summary;

CREATE UNIQUE INDEX ix_assets_summary_pairs_address_asset_address ON assets_summary_pairs USING btree (address, asset_address);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE assets_summary_pairs;
-- +goose StatementEnd
