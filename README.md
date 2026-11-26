# fabric-urban-analytics

A Microsoft Fabric solution that joins NYC taxi mobility, OpenAQ air quality and
macro-economic data (World Bank GDP, ECB FX) into one Lakehouse and Warehouse,
following a Bronze -> Silver -> Gold medallion architecture.

Built on an F2 trial capacity. That constraint shaped several decisions below.

## Architecture

```
                    Bronze (lh_bronze)        Silver (lh_silver)      Gold (wh_gold)
NYC TLC parquet ->  Files/taxi/*.parquet  ->  silver_taxi        ->   FactTaxiDaily
TLC zone lookup ->  taxi_zones            ->  silver_zones       ->   DimZone
OpenAQ API      ->  openaq_days           ->  silver_air_quality ->   FactAirQualityDaily
OpenAQ API      ->  openaq_sensors        ->  silver_sensors     ->   DimSensor
ECB API         ->  ecb_fx                ->  silver_fx          ->   DimFX
World Bank API  ->  worldbank_gdp         ->  silver_gdp         ->   DimGDP
                                                                      DimDate (generated)
                                                                          |
                                                          sm_urban_analytics (Direct Lake)
                                                                          |
                                                                  rpt_urban_analytics
```

Bronze keeps raw payloads and is never modified after ingestion. Silver and Gold
are both fully rebuildable from Bronze by rerunning the notebook and the Warehouse
scripts, in that order.

## Workspace items

| Item | Type | Purpose |
| --- | --- | --- |
| `pl_ingest_taxi` | Data Pipeline | Copies monthly TLC parquet files into Bronze |
| `df_openaq` | Dataflow Gen2 | OpenAQ sensors and daily measurements |
| `df_ecb` | Dataflow Gen2 | ECB daily USD/EUR reference rates |
| `df_worldbank` | Dataflow Gen2 | World Bank GDP for the USA |
| `df_taxi_zones` | Dataflow Gen2 | TLC taxi zone lookup |
| `lh_bronze` | Lakehouse | Raw landing zone |
| `nb_transform_silver` | Notebook (PySpark) | Cleans and types Bronze into Silver |
| `lh_silver` | Lakehouse | Clean typed Delta tables |
| `wh_gold` | Warehouse | Star schema, T-SQL |
| `sm_urban_analytics` | Semantic model | Direct Lake over `wh_gold` |
| `rpt_urban_analytics` | Report | Four dashboard pages |

## Ingestion

### Taxi (pipeline, not dataflow)

`pl_ingest_taxi` generates a list of month offsets and copies one parquet file per
month. The month count is computed at runtime, so the pipeline does not need editing
when a new month is published:

```
@range(0, add(mul(sub(int(formatDateTime(utcNow(),'yyyy')), 2024), 12),
              sub(int(formatDateTime(utcNow(),'MM')), 2)))
```

The `- 2` accounts for the roughly two month publishing lag at TLC.

The copy activity uses the **Binary** file format. Parsing the parquet here would be
pointless work: Bronze stores the file as published, and Spark reads it in Silver.
ForEach runs with a batch count of 4. Sequential would take four times longer, and
higher parallelism gains nothing on F2.

Result: 33 files, about 2 GB, covering 2024-01 to 2026-09.

### APIs (dataflows)

All three API sources land through Dataflow Gen2, as the project scope requires.

**OpenAQ** is the only non-trivial one. `df_openaq` has three queries:

- `ApiKey` reads the key from `lh_bronze/Files/config/openaq.key`. Staging is disabled
  and it has no destination, so the key never lands in a table and never reaches Git.
- `openaq_sensors` queries the NYC bounding box `-74.5,40.4,-73.6,41.0`, flattens the
  nested sensor records and joins a hard-coded sensor-to-borough map.
- `openaq_days` pages through `/sensors/{id}/days` for each sensor and flattens the
  nested `period`, `summary` and `coverage` records into 22 columns.

The API returns intermittent 500s. `Value.WaitFor` does not exist in Dataflow Gen2, so
the retry is written by hand: `Web.Contents` with
`ManualStatusHandling = {429, 500, 502, 503, 504}`, the status read through
`Value.Metadata(Response)[Response.Status]`, and `List.Generate` driving up to five
attempts with `Function.InvokeAfter` and an exponential backoff of 2^n seconds.
Without it, roughly one run in three failed on sensor 23341.

Pagination stops when a page returns fewer rows than the page size, rather than
reading a total from the response. One fewer field to trust.

## Transformation (Silver)

Full rules are in `nb_transform_silver`, which documents each decision next to the
code. Summary:

**Taxi.** Profiling 2024-01 found pickup dates from 2002, distances up to 312722 miles,
totals down to -900 and trips with zero passengers. Rows are dropped when the pickup
date falls outside 2024-01-01 .. 2026-09-30, duration is not between 0 and 24 hours,
distance is not in (0, 100] miles, total is not in (0, 1000] USD, or either zone id is
outside 1..265.

A bad `passenger_count` nulls the field, not the row. The trip itself is real; only
that one attribute is unusable.

Deduplication runs per month rather than globally. There is no trip key, and a full
shuffle over 117M rows does not fit F2. Cross-month duplicates are impossible because
each month is a separate source file.

**Air quality.** Only days with `coverage_percentComplete >= 75` are kept. That is the
EPA threshold for a valid daily average; it drops 260 of 11265 rows. Negative
concentrations are dropped as well, 7 rows of sensor noise near the detection limit.
Coverage above 100 percent is expected on the clock-change day, when a sensor reports
25 hours against 24 expected.

The date comes from `period_datetimeFrom_local`, not the UTC field. A daily average
belongs to the local calendar day.

**FX.** Rates are cast to `decimal(18,6)`, not double. These rates convert revenue in
Gold, and double loses precision on money.

**GDP.** `date` is a year and stays an integer. `decimal(20,2)` is needed because US
GDP has 14 digits and a narrower decimal silently overflows to null.

Every Silver table carries `source_system` and `loaded_at`.

### Silver row counts

| Table | Rows |
| --- | --- |
| `silver_taxi` | 117 618 292 |
| `silver_air_quality` | 10 998 |
| `silver_fx` | 724 |
| `silver_zones` | 265 |
| `silver_gdp` | 66 |
| `silver_sensors` | 12 |

## Gold star schema

Two fact tables at daily grain and five dimensions, built with T-SQL in `wh_gold`.
Cross-database queries read Silver directly as `lh_silver.dbo.<table>`.

Tables are created with explicit DDL rather than `CREATE TABLE AS SELECT`. CTAS
produces nullable columns, and a primary key cannot be defined on a nullable column.
All constraints are `NONCLUSTERED ... NOT ENFORCED`: Fabric Warehouse does not enforce
them, they exist for the query optimiser and as documentation of intent. Integrity is
therefore verified by query, not assumed.

Two Fabric Warehouse limitations worth noting: there is no `NVARCHAR`, so
`DATENAME(...)` has to be cast to `VARCHAR`, and there is no `TINYINT`, so small
integers use `SMALLINT`.

### Data dictionary

**FactTaxiDaily** — one row per day and pickup zone. 238 038 rows.

| Column | Type | Null | Description |
| --- | --- | --- | --- |
| `date_key` | INT | no | FK to DimDate, format yyyymmdd |
| `pickup_zone_id` | INT | no | FK to DimZone |
| `trip_count` | BIGINT | no | Trips starting in this zone on this day |
| `total_distance_miles` | DECIMAL(18,2) | no | Sum of trip distances |
| `total_fare_usd` | DECIMAL(18,2) | no | Sum of fares, excluding tips and surcharges |
| `total_tip_usd` | DECIMAL(18,2) | no | Sum of tips |
| `total_amount_usd` | DECIMAL(18,2) | no | Sum of totals, including tips and surcharges |
| `total_duration_seconds` | BIGINT | no | Sum of trip durations |
| `total_passengers` | BIGINT | yes | Sum over trips where passenger count is known |
| `trips_with_passenger_count` | BIGINT | no | Denominator for average passengers |

**FactAirQualityDaily** — one row per day and sensor. 10 998 rows.

| Column | Type | Null | Description |
| --- | --- | --- | --- |
| `date_key` | INT | no | FK to DimDate |
| `sensor_id` | BIGINT | no | FK to DimSensor |
| `value_avg` | FLOAT | no | Daily average concentration |
| `value_max` | FLOAT | yes | Daily maximum |
| `coverage_pct` | FLOAT | no | Share of expected hourly readings received |

**DimDate** — 1 096 rows, 2024-01-01 to 2026-12-31.
`date_key`, `full_date`, `year_number`, `quarter_number`, `month_number`,
`month_name`, `day_of_month`, `day_name`, `is_weekend`.

**DimZone** — 265 rows. `zone_id`, `borough`, `zone_name`, `service_zone` (nullable).
Zones 264 and 265 are "Unknown" and "N/A" in the TLC lookup. They are kept
deliberately: dropping them would silently lose trips instead of showing them as
unknown.

**DimSensor** — 12 rows. `sensor_id`, `station_name`, `borough`, `parameter`,
`latitude`, `longitude`.

**DimFX** — 1 096 rows, one per calendar day. `date_key`, `rate_date`, `usd_per_eur`,
`source_rate_date`, `is_carried_forward`.

**DimGDP** — 66 rows. `country_code`, `year_number`, `gdp_usd`.

### FX carry-forward

ECB publishes rates on business days only, so about 30 percent of taxi days have no
rate of their own. Leaving those days null would drop every weekend and holiday out of
any EUR figure.

`DimFX` therefore has one row per calendar day. On days without a published rate the
last known rate is carried forward with a window function:

```sql
MAX(CASE WHEN usd_per_eur IS NOT NULL THEN full_date END)
    OVER (ORDER BY full_date ROWS UNBOUNDED PRECEDING)
```

`source_rate_date` records which trading day the rate actually came from and
`is_carried_forward` flags it, so a carried value is never mistaken for a published
one. 302 of 1 096 days are carried forward.

### Verification

`wh_gold` holds saved verification queries alongside the build scripts:

- row counts and null checks per table
- referential integrity between every fact and its dimensions
- FX coverage: every date in the fact range resolves to a rate
- USD and EUR revenue reconciliation
- constraint inventory: 5 primary keys and 5 foreign keys present

## Semantic model and dashboards

`sm_urban_analytics` is a Direct Lake model over `wh_gold`: the report reads the
Warehouse directly, with no import and no duplicated copy of the data.

Relationships are defined manually. Fabric does not import `NOT ENFORCED` foreign keys
into the model, and Direct Lake does not verify cardinality, so each relationship is
declared explicitly as many-to-one with single-direction filtering.

Thirteen DAX measures. Three are worth explaining:

- `Total Trips` sums `trip_count` rather than counting rows. A row is a day and a zone,
  not a trip.
- `Avg Passengers` divides by `trips_with_passenger_count`, not by `Total Trips`.
  Dividing by all trips would understate the average, because the numerator only covers
  trips where the count is known.
- `Total Revenue EUR` iterates over days and converts each day at that day's rate:

  ```dax
  SUMX(VALUES(DimDate[date_key]), DIVIDE([Total Revenue USD], [USD per EUR]))
  ```

  Dividing total revenue by an average rate would be wrong whenever revenue is not
  evenly distributed across the period.

Report pages, mapped to the analytical questions in the project scope:

| Page | Question |
| --- | --- |
| Mobility | Trips per day, average fare, busiest pickup zones |
| Air Quality | PM2.5 and NO2 trends by day and location |
| Mobility vs Air Quality | How traffic intensity relates to air quality |
| Economic Impact | Revenue in USD vs EUR, GDP context |

Taxi and air quality can only be compared at borough level: `DimZone[borough]` and
`DimSensor[borough]` carry the same borough names, but the two columns sit in
unrelated tables, so one slicer does not filter both facts. Doing it properly needs a
shared `DimBorough` dimension related to both fact tables; that is not built yet, so
the correlation page compares the city as a whole. There is no finer link in any case,
because sensors do not map to taxi zones.

## Known limitations

- **Zero-distance trips are excluded.** `clean_taxi` requires `trip_distance > 0`,
  which drops roughly 2.6 percent of trips. On reflection this is too aggressive: a
  trip with zero distance, a positive duration and a positive fare is a real event —
  the driver arrived, started the meter and waited. Dropping it silently removes paid
  work from the revenue figures. The correct treatment is to keep these rows and expose
  them as a separate drill-down.
- **Daily grain only.** The scope specifies `FactTaxiDaily`, so time-of-day analysis is
  out of reach. An hourly fact would answer it but would be roughly 24 times larger.
- **Borough-level correlation.** Twelve sensors across five boroughs and New Jersey is
  a coarse grid against 265 taxi zones. Any correlation shown is directional, not causal.
- **Parameters are not comparable.** PM2.5 is in µg/m³ and NO2 in ppm. Any visual that
  shows a concentration must be sliced by `DimSensor[parameter]`; mixing them produces
  a meaningless number.
- **NO2 only in New Jersey.** The two New Jersey sensors measure NO2 and nothing else,
  so PM2.5 comparisons exclude them.
- **Manual borough mapping.** OpenAQ returns coordinates but not boroughs. The mapping
  of twelve sensor ids to boroughs is hard-coded in `df_openaq` and would need
  maintenance if the sensor set changed.
- **Averages are unweighted.** `Avg Pollutant Value` averages daily averages without
  weighting by the number of readings behind each day. `coverage_pct` is kept in the
  fact so that weighting or stricter filtering remains possible.
- **GDP is annual and national.** It provides context for the revenue figures; it
  cannot be correlated with daily city-level data in any meaningful way.

## Governance and operations

**Secrets.** The OpenAQ API key lives in `lh_bronze/Files/config/openaq.key` and is read
at runtime by the `ApiKey` query. It is not in this repository, not in any dataflow
definition and not in any table.

**Source control.** The workspace is connected to this repository through Fabric Git
integration. Dataflow mashups, the pipeline definition, the notebook and the Warehouse
DDL are all versioned as text.

**Lineage.** Fabric tracks item-level lineage in the workspace lineage view: source
items, the lakehouses they write to, the notebook, the warehouse, the semantic model
and the report.

**Idempotency.** Every stage can be rerun safely. The pipeline overwrites files by
name, the notebook writes Silver with `overwrite`, and the Warehouse scripts drop and
rebuild the star schema.

**Refresh.** Ingestion and transformation are scheduled daily. The TLC publishing lag
means new taxi data appears roughly monthly, but the OpenAQ, ECB and World Bank
sources update more often and the pipeline is cheap to rerun.

## Rebuilding from scratch

1. Create a Lakehouse **without schemas enabled**. Dataflow Gen2 cannot write to a
   schema-enabled Lakehouse; it fails at `WriteToDataDestination` with
   `ErrorCode 10277, null -> Text`, which is a known issue and not a problem with the
   query. This cost a day to diagnose.
2. Place the OpenAQ API key in `Files/config/openaq.key`.
3. Run `pl_ingest_taxi`, then the four dataflows.
4. Run `nb_transform_silver` top to bottom. The first cell defines constants used by
   every later cell; a notebook stores code and output, not session state, so it has to
   run first in a fresh session.
5. In `wh_gold`, run the scripts in numeric order: `00_drop_all`, `05_create_tables`,
   `10_dim_date`, `20_dim_zone_sensor_gdp`, `25_dim_fx`, `30_fact_air_quality_daily`,
   `40_fact_taxi_daily`, `50_add_keys`, then the `9x_check_*` verification queries.

On F2, only one Spark session runs at a time. A second notebook session fails with
`TooManyRequestsForCapacity` (HTTP 430).

## Sources

- NYC TLC trip records — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- OpenAQ — https://docs.openaq.org/about/about
- World Bank GDP — https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json
- ECB daily FX — https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata
