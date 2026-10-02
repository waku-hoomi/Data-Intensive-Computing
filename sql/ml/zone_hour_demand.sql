-- Training dataset for the Week 4 hourly taxi-demand model.
--
-- One row per (pickup zone x hour) over the full analysis window, including
-- hours where the zone recorded zero pickups. The zero-demand hours are the
-- point: a model trained only on observed trips would never see the base rate
-- of an idle zone-hour, and would over-predict everywhere.
--
-- The zone x hour grid is distilled from the platform's own analysis views
-- (`analysis_trips`, `hour_spine`, registered by
-- `analytics.context.register_analysis_views`), which is what makes this an
-- "Approach B" dataset: ingestion, validation, integration, timezone
-- normalisation and the environmental joins all already happened upstream.
-- Only the demand aggregation and the feature-ready column selection are new.
WITH zone_hours AS (
  SELECT z.pu_location_id, z.pickup_zone, z.pickup_borough,
         h.pickup_hour, h.local_hour,
         coalesce(t.trip_count, 0L) AS trip_count
  FROM (SELECT DISTINCT pu_location_id, pickup_zone, pickup_borough
        FROM analysis_trips
        WHERE pu_location_id IS NOT NULL) z
  CROSS JOIN hour_spine h
  LEFT JOIN (SELECT pu_location_id, pickup_hour, count(*) AS trip_count
             FROM analysis_trips
             GROUP BY pu_location_id, pickup_hour) t
    ON z.pu_location_id = t.pu_location_id AND h.pickup_hour = t.pickup_hour
),
-- Weather is a single citywide series (no location dimension in the source),
-- so it is joined per hour for every zone alike.
city_weather AS (
  SELECT pickup_hour,
         max(temp_c) AS temp_c,
         max(relative_humidity_pct) AS relative_humidity_pct,
         max(humidity_pct_v2) AS humidity_pct_v2,
         max(precipitation_mm) AS precipitation_mm,
         max(wind_speed_kmh) AS wind_speed_kmh,
         max(weather_condition_code) AS weather_condition_code
  FROM analysis_trips
  GROUP BY pickup_hour
),
-- Air quality is station-derived and only available for Bronx, Brooklyn and
-- Queens. Manhattan and Staten Island rows stay NULL here on purpose; imputing
-- a pollutant reading that no instrument measured would fabricate data. The
-- feature pipeline decides what to do with the NULLs, and the evaluation report
-- records how many zone-hours that affects.
borough_aq AS (
  SELECT pickup_borough, pickup_hour,
         max(pm25_ug_m3) AS pm25_ug_m3,
         max(aqi_max) AS aqi_max,
         max(aq_station_count) AS aq_station_count
  FROM analysis_trips
  GROUP BY pickup_borough, pickup_hour
)
SELECT zh.pu_location_id, zh.pickup_zone, zh.pickup_borough,
       zh.pickup_hour, zh.local_hour, zh.trip_count,
       cw.temp_c, cw.relative_humidity_pct, cw.humidity_pct_v2,
       cw.precipitation_mm, cw.wind_speed_kmh, cw.weather_condition_code,
       ba.pm25_ug_m3, ba.aqi_max, ba.aq_station_count
FROM zone_hours zh
LEFT JOIN city_weather cw ON zh.pickup_hour = cw.pickup_hour
LEFT JOIN borough_aq ba
  ON zh.pickup_borough = ba.pickup_borough AND zh.pickup_hour = ba.pickup_hour
