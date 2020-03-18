-- +goose Up
-- +goose StatementBegin
CREATE INDEX ix_dex_logs_by_event_type ON
    public.dex_logs USING btree (event_type)
    WHERE (is_forked = false);

CREATE INDEX ix_dex_logs_by_orders_by_assets
    ON public.dex_logs USING btree (event_type,
                                    (event_data ->> 'tradedAsset'::text))
    WHERE is_forked = false;

CREATE INDEX ix_dex_logs_by_orders_by_orders
    ON public.dex_logs USING btree (event_type,
                                    (event_data ->> 'orderID'::text))
    WHERE is_forked = false;

CREATE INDEX ix_dex_logs_by_orders_by_trades
    ON public.dex_logs USING btree (event_type,
                                    (event_data ->> 'tradeID'::text))
    WHERE is_forked = false;


CREATE INDEX ix_dex_logs_by_orders_by_user
    ON public.dex_logs USING btree (event_type,
                                    (event_data ->> 'userAddress'::text),
                                    (event_data ->> 'assetAddress'::text))
    WHERE (is_forked = false);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX ix_dex_logs_by_event_type;
DROP INDEX ix_dex_logs_by_orders_by_assets;
DROP INDEX ix_dex_logs_by_orders_by_trades;
DROP INDEX ix_dex_logs_by_orders_by_orders;
DROP INDEX ix_dex_logs_by_orders_by_user;

-- +goose StatementEnd
