-- Determines if a patient is ventilated for each chart time in kdigo_stages.
-- Creates a table with the result.
-- Requires the `vasopressordurations` and `kdigo_stages`, `labs` and `vitals` table

set search_path to mimiciv, mimiciv_icu, mimiciv_hosp, mimiciv_derived;
DROP MATERIALIZED VIEW IF EXISTS mimiciv.vasopressor_kdigo_stages_labs_vitals_charttime CASCADE;
CREATE MATERIALIZED VIEW mimiciv.vasopressor_kdigo_stages_labs_vitals_charttime AS
select *
from (
(
-- Vasopressors for each time in kdigo_stages
select
  ie.stay_id, charttime
  -- if vd.stay_id is not null, then they have a valid vasopressor event
  -- in this case, we say they are given vasopressor
  -- otherwise, they are not
  , max(case
      when vd.stay_id is not null then 1
    else 0 end) as vasopressor
from kdigo_stages ie
left join vasopressin vd
  on ie.stay_id = vd.stay_id
  and
  (
    -- vasopressor duration overlaps with charttime
    (vd.starttime <= ie.charttime and vd.endtime >= ie.charttime)
  )
group by ie.stay_id, charttime
) union (
-- Vasopressors for each time in labs
select
  ie.stay_id, charttime
  -- if vd.stay_id is not null, then they have a valid vasopressor event
  -- in this case, we say they are given vasopressor
  -- otherwise, they are not
  , max(case
      when vd.stay_id is not null then 1
    else 0 end) as vasopressor
from labs ie
left join vasopressin vd
  on ie.stay_id = vd.stay_id
  and
  (
    -- vasopressor duration overlaps with charttime
    (vd.starttime <= ie.charttime and vd.endtime >= ie.charttime)
  )
group by ie.stay_id, charttime
) union (
-- Vasopressors for each time in vitals
select
  ie.stay_id, charttime
  -- if vd.stay_id is not null, then they have a valid vasopressor event
  -- in this case, we say they are given vasopressor
  -- otherwise, they are not
  , max(case
      when vd.stay_id is not null then 1
    else 0 end) as vasopressor
from vitals ie
left join vasopressin vd
  on ie.stay_id = vd.stay_id
  and
  (
    -- vasopressor duration overlaps with charttime
    (vd.starttime <= ie.charttime and vd.endtime >= ie.charttime)
  )
group by ie.stay_id, charttime
)
) u
order by stay_id, charttime;
