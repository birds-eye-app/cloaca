-- name: GetYearTotal :one
SELECT COUNT(*) FROM hotspot_year_species
WHERE hotspot_id = $1 AND year = $2;

-- name: GetKnownSpecies :many
SELECT species_code FROM hotspot_year_species
WHERE hotspot_id = $1 AND year = $2;

-- name: GetAllTimeTotal :one
SELECT COUNT(*) FROM hotspot_all_time_species
WHERE hotspot_id = $1;

-- name: GetKnownAllTimeSpecies :many
SELECT species_code FROM hotspot_all_time_species
WHERE hotspot_id = $1;

-- name: GetPendingProvisionals :many
SELECT hotspot_id, species_code, common_name, scientific_name,
       obs_date, observer_name, sub_id, lifer_type, year, created_at
FROM pending_provisional_lifers
WHERE hotspot_id = $1;

-- name: IsBackfillComplete :one
SELECT 1 FROM backfill_status
WHERE hotspot_id = $1 AND year = $2;

-- name: InsertSpecies :exec
INSERT INTO hotspot_year_species
    (hotspot_id, year, species_code, common_name, scientific_name,
     first_obs_date, observer_name, checklist_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT DO NOTHING;

-- name: InsertAllTimeSpecies :exec
INSERT INTO hotspot_all_time_species (hotspot_id, species_code)
VALUES ($1, $2)
ON CONFLICT DO NOTHING;

-- name: InsertPendingProvisional :exec
INSERT INTO pending_provisional_lifers
    (hotspot_id, species_code, common_name, scientific_name,
     obs_date, observer_name, sub_id, lifer_type, year)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT DO NOTHING;

-- name: MarkBackfillComplete :exec
INSERT INTO backfill_status (hotspot_id, year, completed_at, species_count)
VALUES ($1, $2, CURRENT_TIMESTAMP, $3)
ON CONFLICT DO NOTHING;

-- name: RemovePendingProvisional :exec
DELETE FROM pending_provisional_lifers
WHERE hotspot_id = $1 AND species_code = $2 AND lifer_type = $3;

-- name: RemoveYearSpecies :exec
DELETE FROM hotspot_year_species
WHERE hotspot_id = $1 AND year = $2 AND species_code = $3;

-- name: RemoveAllTimeSpecies :exec
DELETE FROM hotspot_all_time_species
WHERE hotspot_id = $1 AND species_code = $2;

-- name: IsBirdcastPosted :one
SELECT 1 FROM birdcast_post_log
WHERE location = $1 AND forecast_date = $2;

-- name: InsertBirdcastPost :exec
INSERT INTO birdcast_post_log (location, forecast_date)
VALUES ($1, $2)
ON CONFLICT DO NOTHING;

-- name: GetRecentRareBirdAlert :one
SELECT 1 FROM rare_bird_alerts
WHERE species_code = $1 AND region_code = $2
  AND alerted_at > CURRENT_TIMESTAMP - INTERVAL '7 days';

-- name: InsertRareBirdAlert :exec
INSERT INTO rare_bird_alerts
    (species_code, region_code, common_name, aba_code, obs_date,
     observer_name, sub_id, location_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT DO NOTHING;
