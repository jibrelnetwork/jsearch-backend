-- +goose Up
-- +goose StatementBegin
DROP FUNCTION IF EXISTS get_assets_summaries(addresses TEXT[], assets TEXT[]);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION get_assets_summaries(addresses TEXT[], assets TEXT[]) RETURNS SETOF assets_summary AS
$BODY$
DECLARE
  _address TEXT;
  _asset_address TEXT;
BEGIN
  FOREACH _address IN ARRAY addresses
  LOOP
    FOREACH _asset_address IN ARRAY assets
    LOOP
      RETURN QUERY
        SELECT *
        FROM assets_summary
        WHERE assets_summary.address = _address
          AND assets_summary.asset_address = _asset_address
          AND assets_summary.is_forked = false
        ORDER BY block_number DESC
        LIMIT 1;
    END LOOP;
  END LOOP;
END
$BODY$ LANGUAGE plpgsql;
-- +goose StatementEnd
