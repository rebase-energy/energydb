-- EnergyDB asset tables (extends TimeDB schema)

-- Energy asset (WindTurbine, PVSystem, Battery, ...)
CREATE TABLE IF NOT EXISTS asset (
    asset_id    bigserial PRIMARY KEY,
    asset_type  text NOT NULL,
    name        text NOT NULL,
    properties  jsonb NOT NULL DEFAULT '{}',
    latitude    double precision,
    longitude   double precision,
    altitude    double precision,
    timezone    text,
    created_at  timestamptz DEFAULT now(),
    updated_at  timestamptz DEFAULT now(),
    UNIQUE (name, asset_type)
);
CREATE INDEX IF NOT EXISTS idx_asset_properties ON asset USING GIN (properties);
CREATE INDEX IF NOT EXISTS idx_asset_type ON asset (asset_type);

-- Collection hierarchy (Portfolio, Site, WindFarm, EnergyCommunity, ...)
CREATE TABLE IF NOT EXISTS collection (
    collection_id   bigserial PRIMARY KEY,
    collection_type text NOT NULL,
    name            text,
    properties      jsonb NOT NULL DEFAULT '{}',
    parent_id       bigint REFERENCES collection(collection_id) ON DELETE CASCADE,
    latitude        double precision,
    longitude       double precision,
    created_at      timestamptz DEFAULT now(),
    UNIQUE (name, collection_type)
);
CREATE INDEX IF NOT EXISTS idx_collection_parent ON collection (parent_id);

-- Many-to-many: collection contains assets
CREATE TABLE IF NOT EXISTS collection_asset (
    collection_id   bigint REFERENCES collection(collection_id) ON DELETE CASCADE,
    asset_id        bigint REFERENCES asset(asset_id) ON DELETE CASCADE,
    PRIMARY KEY (collection_id, asset_id)
);

-- Asset owns time series (links to TimeDB's series_table)
CREATE TABLE IF NOT EXISTS asset_series (
    asset_id    bigint REFERENCES asset(asset_id) ON DELETE CASCADE,
    series_id   bigint NOT NULL,
    role        text,
    PRIMARY KEY (asset_id, series_id)
);
CREATE INDEX IF NOT EXISTS idx_asset_series_series ON asset_series (series_id);

-- Collection owns time series (links to TimeDB's series_table)
CREATE TABLE IF NOT EXISTS collection_series (
    collection_id   bigint REFERENCES collection(collection_id) ON DELETE CASCADE,
    series_id       bigint NOT NULL,
    role            text,
    PRIMARY KEY (collection_id, series_id)
);
CREATE INDEX IF NOT EXISTS idx_collection_series_series ON collection_series (series_id);
