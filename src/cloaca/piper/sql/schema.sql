CREATE TABLE hotspot_year_species (
    hotspot_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    species_code TEXT NOT NULL,
    common_name TEXT NOT NULL,
    scientific_name TEXT NOT NULL,
    first_obs_date DATE NOT NULL,
    observer_name TEXT NOT NULL,
    checklist_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hotspot_id, year, species_code)
);

CREATE TABLE backfill_status (
    hotspot_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    species_count INTEGER NOT NULL,
    PRIMARY KEY (hotspot_id, year)
);

CREATE TABLE hotspot_all_time_species (
    hotspot_id TEXT NOT NULL,
    species_code TEXT NOT NULL,
    PRIMARY KEY (hotspot_id, species_code)
);

CREATE TABLE pending_provisional_lifers (
    hotspot_id TEXT NOT NULL,
    species_code TEXT NOT NULL,
    common_name TEXT NOT NULL,
    scientific_name TEXT NOT NULL,
    obs_date DATE NOT NULL,
    observer_name TEXT NOT NULL,
    sub_id TEXT NOT NULL,
    lifer_type TEXT NOT NULL,
    year INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hotspot_id, species_code, lifer_type)
);
