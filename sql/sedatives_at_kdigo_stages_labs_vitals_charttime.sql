-- Determines if a patient is subject to sedatives for each chart time in kdigo_stages.
-- Creates a table with the result.
-- Requires the `kdigo_stages` and `sedativesduration`, `labs`, `vitals` table

set search_path to mimiciv, mimiciv_icu, mimiciv_hosp, mimiciv_derived;
DROP MATERIALIZED VIEW IF EXISTS mimiciv.sedatives_kdigo_stages_labs_vitals_charttime CASCADE;
CREATE MATERIALIZED VIEW mimiciv.sedatives_kdigo_stages_labs_vitals_charttime AS
select *
from (
(
-- Sedatives for each time in kdigo_stages
select
  ie.stay_id, charttime
  , max(case
      when vd.stay_id is not null then 1
    else 0 end) as sedative
from kdigo_stages ie
left join sedativedurations vd
  on ie.stay_id = vd.stay_id
  and
  (
    (vd.starttime <= ie.charttime and vd.endtime >= ie.charttime)
  )
group by ie.stay_id, charttime
) union (
-- Sedatives for each time in labs
select
  ie.stay_id, charttime
  , max(case
      when vd.stay_id is not null then 1
    else 0 end) as sedative
from labs ie
left join sedativedurations vd
  on ie.stay_id = vd.stay_id
  and
  (
    (vd.starttime <= ie.charttime and vd.endtime >= ie.charttime)
  )
group by ie.stay_id, charttime
) union (
-- Sedatives for each time in vitals
select
  ie.stay_id, charttime
  , max(case
      when vd.stay_id is not null then 1
    else 0 end) as sedative
from vitals ie
left join sedativedurations vd
  on ie.stay_id = vd.stay_id
  and
  (
    (vd.starttime <= ie.charttime and vd.endtime >= ie.charttime)
  )
group by ie.stay_id, charttime
)
) u
order by stay_id, charttime;
