-- Determines if a patient is ventilated for each chart time in kdigo_stages, labs, and vitals.
-- Creates a table with the result.
-- Requires the `ventdurations` and `kdigo_stages` table

set search_path to mimiciv, mimiciv_icu, mimiciv_hosp, mimiciv_derived;
DROP MATERIALIZED VIEW IF EXISTS mimiciv.vent_kdigo_stages_labs_vitals_charttime CASCADE;
CREATE MATERIALIZED VIEW mimiciv.vent_kdigo_stages_labs_vitals_charttime AS
select *
from (
(
-- Ventilation for each time in kdigo_stages
select
  ie.stay_id, charttime
  -- if vd.icustay_id is not null, then they have a valid ventilation event
  -- in this case, we say they are ventilated
  -- otherwise, they are not
  , max(case
      when vd.stay_id is not null then 1
    else 0 end) as vent
from mimiciv_derived.kdigo_stages ie
left join mimiciv_derived.ventilation vd
  on ie.stay_id = vd.stay_id
  and
  (
    -- ventilation duration overlaps with charttime
    (vd.starttime <= ie.charttime and vd.endtime >= ie.charttime)
  )
group by ie.stay_id, charttime
) union (
-- Ventilation for each time in labs
select
  ie.stay_id, charttime
  -- if vd.icustay_id is not null, then they have a valid ventilation event
  -- in this case, we say they are ventilated
  -- otherwise, they are not
  , max(case
      when vd.stay_id is not null then 1
    else 0 end) as vent
from mimiciv_icu.labs ie
left join mimiciv_derived.ventilation vd
  on ie.stay_id = vd.stay_id
  and
  (
    -- ventilation duration overlaps with charttime
    (vd.starttime <= ie.charttime and vd.endtime >= ie.charttime)
  )
group by ie.stay_id, charttime
) union (
-- Ventilation for each time in vitals
select
  ie.stay_id, charttime
  -- if vd.icustay_id is not null, then they have a valid ventilation event
  -- in this case, we say they are ventilated
  -- otherwise, they are not
  , max(case
      when vd.stay_id is not null then 1
    else 0 end) as vent
from mimiciv_icu.vitals ie
left join mimiciv_derived.ventilation vd
  on ie.stay_id = vd.stay_id
  and
  (
    -- ventilation duration overlaps with charttime
    (vd.starttime <= ie.charttime and vd.endtime >= ie.charttime)
  )
group by ie.stay_id, charttime
)
) u
order by stay_id, charttime;
