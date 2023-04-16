-- extract icu stays with at least one measurement of creatinine or urine output into kdigo_stages_measured.csv
set search_path to mimiciv, mimiciv_icu, mimiciv_hosp, mimiciv_derived;

COPY (SELECT * FROM kdigo_stages WHERE stay_id IN (SELECT stay_id FROM kdigo_stages WHERE (creat IS NOT NULL OR uo_rt_6hr IS NOT NULL OR uo_rt_12hr IS NOT NULL OR uo_rt_24hr IS NOT NULL) AND aki_stage IS NOT NULL GROUP BY stay_id HAVING COUNT(*) > 0 )) TO 'path/to/data/kdigo_stages_measured.csv' WITH CSV HEADER DELIMITER ';';

-- extract demographics of patients with at least one measurement of creatinine or urine output into stay_detail-kdigo_stages_measured.csv
COPY (SELECT * FROM stay_detail WHERE stay_id IN (SELECT stay_id FROM kdigo_stages WHERE (creat IS NOT NULL OR uo_rt_6hr IS NOT NULL OR uo_rt_12hr IS NOT NULL OR uo_rt_24hr IS NOT NULL)	AND aki_stage IS NOT NULL GROUP BY stay_id HAVING COUNT(*) > 0 )) TO 'path/to/data/stay_detail-kdigo_stages_measured.csv' WITH CSV HEADER DELIMITER ';';
 
-- extract vitals of icu stays with at least one measurement of creatinine or urine output and an AKI label into vitals-kdigo_stages_measured.csv
COPY (SELECT * FROM vitals WHERE stay_id IN (SELECT stay_id FROM kdigo_stages WHERE (creat IS NOT NULL OR uo_rt_6hr IS NOT NULL OR uo_rt_12hr IS NOT NULL OR uo_rt_24hr IS NOT NULL) AND aki_stage IS NOT NULL GROUP BY stay_id HAVING COUNT(*) > 0 )) TO 'path/to/data/vitals-kdigo_stages_measured.csv' WITH CSV HEADER DELIMITER ';';
 
-- extract labs of icu stays with at least one measurement of creatinine or urine output and an AKI label into labs-kdigo_stages_measured.csv
COPY (SELECT * FROM labs WHERE stay_id IN (SELECT stay_id FROM kdigo_stages WHERE (creat IS NOT NULL OR uo_rt_6hr IS NOT NULL OR uo_rt_12hr IS NOT NULL OR uo_rt_24hr IS NOT NULL) AND aki_stage IS NOT NULL GROUP BY stay_id HAVING COUNT(*) > 0 )) TO 'path/to/data/labs-kdigo_stages_measured.csv' WITH CSV HEADER DELIMITER ';';

-- extract ventilations, vasopressor, and sedatives of icu stays with at least one measurement of creatinine or urine output and an AKI label into vents-vasopressor-sedatives-kdigo_stages_measured.csv 
COPY (SELECT ve.stay_id AS stay_id, ve.charttime AS charttime, vent, vasopressor, sedative FROM vent_kdigo_stages_labs_vitals_charttime ve, vasopressor_kdigo_stages_labs_vitals_charttime va, sedatives_kdigo_stages_labs_vitals_charttime s WHERE ve.stay_id = va.stay_id AND ve.charttime = va.charttime AND va.stay_id = s.stay_id AND va.charttime = s.charttime AND ve.stay_id IN (SELECT stay_id FROM kdigo_stages WHERE (creat IS NOT NULL OR uo_rt_6hr IS NOT NULL OR uo_rt_12hr IS NOT NULL OR uo_rt_24hr IS NOT NULL) AND aki_stage IS NOT NULL GROUP BY stay_id HAVING COUNT(*) > 0 ) ORDER BY ve.stay_id, ve.charttime;) TO 'path/to/data/vents-vasopressor-sedatives-kdigo_stages_measured.csv' WITH CSV HEADER DELIMITER ';';
