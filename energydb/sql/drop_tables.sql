-- Drop EnergyDB asset tables (in dependency order)
DROP TABLE IF EXISTS asset_series CASCADE;
DROP TABLE IF EXISTS collection_asset CASCADE;
DROP TABLE IF EXISTS collection CASCADE;
DROP TABLE IF EXISTS asset CASCADE;
