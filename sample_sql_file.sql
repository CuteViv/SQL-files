;WITH catr
     AS (SELECT catr.id,
                Max(go170) AS Highest_FCO
         FROM   [trans_return_data] catr
         WHERE  is_current = 1
         GROUP  BY catr.id),
     find_days_over_30
     AS (SELECT ds.id,
                Iif(Datediff(day, dd.i_due_date, Getdate()) > 30, 1, 0) AS
                if_o_days_over_30
         FROM   delinqency_summary ds
                INNER JOIN delinqency_detail dd
                        ON ds.id = dd.contract_number
         WHERE  dd.i_paid_off IS NULL),
     o_a_over_30
     AS (SELECT id
         FROM   find_days_over_30
         WHERE  if_o_days_over_30 = 1),
     paid_a_over_30
     AS (SELECT id
         FROM   delinqency_summary ds
                INNER JOIN delinqency_detail dd
                        ON ds.id = dd.contract_number
         WHERE  ds.d_60_day_payments >= 1
                 OR d_over_60_day_payments >= 1),
     d_flag
     AS (SELECT *
         FROM   o_a_over_30
         UNION
         SELECT *
         FROM   paid_a_over_30),
     first_i_due_date
     AS (SELECT ds.id,
                Min(dd.i_due_date) AS first_i_due_date
         FROM   delinqency_summary ds
                INNER JOIN delinqency_detail dd
                        ON ds.id = dd.contract_number
         WHERE  ds.id IN (SELECT *
                                     FROM   d_flag)
         GROUP  BY ds.id),
     find_submitted_a
     AS (SELECT ca.id,
                Count(cl.s_c_log_id) AS cou_of_submits
         FROM   v_all ca
                INNER JOIN s_c_log cl
                        ON cl.id = ca.id
         WHERE  cl.s_id IN ( 289, 290 )
         GROUP  BY ca.id),
     decisions
     AS (SELECT a.id,
                d.subdate,
                d.decisiondate,
                d.decisiondescription,
                Isnull(d.analyst, 'Unknown') AS canalyst,
                Row_number()
                  OVER (
                    partition BY a.id
                    ORDER BY d.decisiondate)       AS seq_no_asc,
                Row_number()
                  OVER (
                    partition BY a.id
                    ORDER BY d.decisiondate DESC)  AS seq_no_desc
         FROM   ino_decisions d
                INNER JOIN ino_application a
                        ON a.application_id = d.applicationid
                INNER JOIN find_submitted_a
                        ON find_submitted_a.id = a.id
         WHERE  NOT ( d.analyst = 'acp'
                      AND d.cdecision = 'CPP' )
                AND d.decisiondescription IN (
                    'App', 'Con App',
                    'D',
                    'C P' )),
     find_all_app
     AS (SELECT *,
                Row_number()
                  OVER(
                    partition BY a.id
                    ORDER BY d.decisiondate DESC) AS app_seq
         FROM   ino_decisions d
                INNER JOIN ino_application a
                        ON a.application_id = d.applicationid
         WHERE  d.decisiondescription IN (
                'App', 'Con App',
                'Doc Re', 'Boo' )),
     find_last_app
     AS (SELECT *
         FROM   find_all_app
         WHERE  app_seq = 1),
     find_all_fun
     AS (SELECT *,
                Row_number()
                  OVER(
                    partition BY id
                    ORDER BY created_date_time DESC) AS fun_seq
         FROM   s_c_log
         WHERE  s_id = 297),
     find_last_fun
     AS (SELECT *
         FROM   find_all_fun
         WHERE  fun_seq = 1),
     group_ini_ino_app_by_type
     AS (SELECT *,
                Row_number()
                  OVER(
                    partition BY application_no
                    ORDER BY CASE WHEN applicant_type = 'Prim' THEN 1 ELSE 2
                  END)
                AS
                   seq
         FROM   v_funing_data_ino),
     find_ini_ino_app_with_c_app
     AS (SELECT application_no,
                is_comm
         FROM   group_ini_ino_app_by_type
         WHERE  seq = 2),
     order_by_up_date_time
     AS (SELECT application_id,
                wf_state_name,
                up_date_time,
                up_by,
                Row_number()
                  OVER(
                    partition BY application_id
                    ORDER BY up_date_time DESC) AS seq
         FROM   ino_wf_history),
     latest_up_date_time
     AS (SELECT *
         FROM   order_by_up_date_time
         WHERE  seq = 1),
     latest_up_date_time_with_id
     AS (SELECT a.id,
                l.wf_state_name,
                l.up_date_time,
                l.up_by,
                Row_number()
                  OVER(
                    partition BY a.id
                    ORDER BY l.up_date_time DESC) AS seq
         FROM   ino_application a
                LEFT JOIN latest_up_date_time l
                       ON a.application_id = l.application_id),
     unique_latest_up_date_time_with_id
     AS (SELECT *
         FROM   latest_up_date_time_with_id
         WHERE  seq = 1),
     unique_d_to_ams
     AS (SELECT ams.ams_key,
                Isnull(ams.ams_name, 'Unknown') AS AMS,
                Count(DISTINCT d.d_key)        AS Total_d_under_AMS
         FROM   dbo.d
                INNER JOIN ams
                        ON Isnull(d.ams_key, 0) = ams.ams_key
         WHERE  Isnull(is_test_d, 0) = 0
                AND Isnull(d.is_old_s, 0) = 0
                AND EXISTS(SELECT *
                           FROM   v_all ca
                           WHERE  ca.d_key = d.d_key)
         GROUP  BY ams.ams_key,
                   Isnull(ams.ams_name, 'Unknown')),
     find_returningcflag
     AS (SELECT a.id,
                1 AS ReturningCFlag
         FROM   ino_application a
                LEFT JOIN ino_c c
                       ON a.application_id = c.applicationid
         WHERE  c.returningcflag = 1
         GROUP  BY a.id),
     original_fun_pac_date_re
     AS (SELECT ca.id,
                (SELECT TOP 1 cl.created_date_time
                 FROM   s_c_log cl
                 WHERE  ca.id = cl.id
                        AND cl.s_id = 94
                 ORDER  BY cl.created_date_time) AS
                   original_fun_pac_date_re
         FROM   v_all ca
                INNER JOIN d
                        ON d.d_key = ca.d_key
         WHERE  ca.ca_st_key = 100),
     determine_ca_tableau_c_major_dgg
     AS (SELECT id
         FROM   tableau_c_major_dgg
         WHERE  Isnull(ptype, '') <> 'O'
         GROUP  BY id),
     determine_pre_bank
     AS (SELECT id
         FROM   tableau_c_bureau_detail
         WHERE  prebankre <> 0
         GROUP  BY id),
     prim_an_total_table_duplicate
     AS (SELECT app.id,
                cust.ginomefre,
                cust.ginome,
                CASE
                  WHEN cust.ginomefre = 'W' THEN cust.ginome * 52
                  WHEN cust.ginomefre = 'M' THEN cust.ginome * 12
                  WHEN cust.ginomefre = 'A' THEN cust.ginome
                  ELSE cust.ginome * 12
                END                       AS sub_a_inome,
                cust.antotal,
                cust.relationtype,
                Row_number()
                  OVER(
                    partition BY id
                    ORDER BY ginome) AS seq
         FROM   ino_application app
                INNER JOIN ino_c cust
                        ON app.application_id = cust.applicationid
         WHERE  isdeleted = 0
                AND cust.relationtype = 1),
     prim_an_total_table
     AS (SELECT *
         FROM   prim_an_total_table_duplicate
         WHERE  seq = 1),
     co_app_an_total_table_duplicate
     AS (SELECT app.id,
                cust.ginomefre,
                cust.ginome,
                CASE
                  WHEN cust.ginomefre = 'W' THEN cust.ginome * 52
                  WHEN cust.ginomefre = 'M' THEN cust.ginome * 12
                  WHEN cust.ginomefre = 'A' THEN cust.ginome
                  ELSE cust.ginome * 12
                END                       AS sub_a_inome,
                cust.antotal,
                cust.relationtype,
                Row_number()
                  OVER(
                    partition BY id
                    ORDER BY ginome) AS seq
         FROM   ino_application app
                INNER JOIN ino_c cust
                        ON app.application_id = cust.applicationid
         WHERE  isdeleted = 0
                AND cust.relationtype = 2),
     co_app_an_total_table
     AS (SELECT *
         FROM   co_app_an_total_table_duplicate
         WHERE  seq = 1),
     co_si_an_total_table_duplicate
     AS (SELECT app.id,
                cust.ginomefre,
                cust.ginome,
                CASE
                  WHEN cust.ginomefre = 'W' THEN cust.ginome * 52
                  WHEN cust.ginomefre = 'M' THEN cust.ginome * 12
                  WHEN cust.ginomefre = 'A' THEN cust.ginome
                  ELSE cust.ginome * 12
                END                       AS sub_a_inome,
                cust.antotal,
                cust.relationtype,
                Row_number()
                  OVER(
                    partition BY id
                    ORDER BY ginome) AS seq
         FROM   ino_application app
                INNER JOIN ino_c cust
                        ON app.application_id = cust.applicationid
         WHERE  isdeleted = 0
                AND cust.relationtype = 3),
     co_si_an_total_table
     AS (SELECT *
         FROM   co_si_an_total_table_duplicate
         WHERE  seq = 1),
     wf_fun
     AS (SELECT a.id,
                wf.up_by,
                wf.up_date_time,
                Row_number()
                  OVER (
                    partition BY a.id
                    ORDER BY wf.up_date_time DESC) AS seq_no
         FROM   ino_wf_history wf
                INNER JOIN ino_application a
                        ON a.application_id = wf.application_id
                INNER JOIN v_all ca
                        ON ca.id = a.id
         WHERE  wf.wf_s_id = 62
                AND wf_state_name = 'Funing'
                AND ca.ca_st_key = 100),
     wf_boo
     AS (SELECT a.id,
                wf.up_by,
                wf.up_date_time,
                Row_number()
                  OVER (
                    partition BY a.id
                    ORDER BY wf.up_date_time DESC) AS seq_no
         FROM   ino_wf_history wf
                INNER JOIN ino_application a
                        ON a.application_id = wf.application_id
                INNER JOIN v_all ca
                        ON ca.id = a.id
         WHERE  wf.wf_s_id = 4
                AND wf_state_name = 'Boo'
                AND ca.ca_st_key = 100)
---------------------------------------------------------------------------------------------------
SELECT
ca.id,
s.ca_s_name								   AS current_s_name,
ds.full_date                               AS sub_date,
CASE
  WHEN ca.last_decisioned_ca_st_key IN ( 14, 15 ) THEN 1
  ELSE 0
END                                        AS is_app,
CASE
  WHEN ca.last_decisioned_ca_st_key IN ( 4, 5 ) THEN da.full_date
  ELSE ''
END                                        AS app_date,
CASE
  WHEN ca.ca_st_key = 10 THEN 1
  ELSE 0
END                                        AS is_fun,
CASE
  WHEN ca.ca_st_key = 10 THEN df.full_date
  ELSE ''
END                                        AS fun_date,
ino.applicant_p                            AS prim_app_p,
ino.c_age								   AS prim_app_age,
CASE
  WHEN find_ini.application_no IS NULL THEN 0
  ELSE 1
END                                        AS have_a_co_app,
ino.is_comm,
ca.vehicle_make                            AS make,
ca.vehicle_model                           AS model,
ca.vehicle_trim                            AS trim,
ca.vehicle_body_style                      AS body_style,
ca.l_start_date,
ca.l_end_date_original,
ca.term,
ca.gkrp,
ca.b_s_price							   AS s_price,
ca.r_value_original						   AS boo_r,
ca.l_rate,
ca.d_payment,
ca.net_t_in,
ca.extended_war,
ca.payment_f,
ca.ins_before_tax,
ino.ltv_r,
ino.ltv_r_at_decision,
ino.tds_r,
ino.tds_v,
ino.pti,
ino.fco_score,
CASE
  WHEN catr.highest_fco IS NULL THEN 'N/A'
  ELSE catr.highest_fco
END                                        AS FCO_Score_refresh,
ca.c_rating                                AS R_c_rating,
ino.r_app_score							   AS R_application_score,
ino.bk_score							   AS bk_score,
ino.gross_in                               AS gross_in,
ino.gross_in_combined                      AS gross_in_combined,
CASE
  WHEN del_f.id IS NULL THEN 0
  ELSE 1
END                                        AS d_flag,
CASE
  WHEN first_inv.id IS NULL THEN NULL
  ELSE first_inv.first_i_due_date
END                                        AS first_i_due_date,
CASE
  WHEN def.id IS NULL THEN 0
  ELSE 1
END                                        AS def_flag,
CASE
  WHEN def.id IS NULL THEN NULL
  ELSE def.def_from_date
END                                        AS def_from_date,
CASE
  WHEN def.id IS NULL THEN NULL
  ELSE def.def_until_date
END                                        AS def_until_date,
ds_first.analyst						   AS first_de_user,
ds_last.analyst                            AS last_de_user,
ca.is_in_veri_waived					   AS PO_waived_flag,
CASE
  WHEN ca.ca_st_key = 100 THEN 'Boo'
  WHEN ca.ca_st_key <> 100
       AND ca.sub_date_key = 0 THEN 'Qu'
  WHEN ca.ca_st_key <> 100
       AND ca.sub_date_key > 0
       AND ca.ca_st_key IN ( 11, 12 ) THEN 'Can'
  WHEN ca.ca_st_key <> 100
       AND ca.sub_date_key > 0
       AND ca.last_decisioned_ca_st_key IN ( 4, 5 )
       AND ca.last_decisioned_ca_st_key = ca.ca_st_key
THEN 'App'
  WHEN ca.ca_st_key <> 100
       AND ca.sub_date_key > 0
       AND ca.last_decisioned_ca_st_key IN ( 6 )
       AND ca.last_decisioned_ca_st_key = ca.ca_st_key
THEN 'D'
  WHEN ca.ca_st_key <> 100
       AND ca.sub_date_key > 0
       AND ca.last_decisioned_ca_st_key = 0 THEN 'Sub'
  ELSE 'Pen'
END                                        AS ca_status,
CASE
  WHEN ca.funing_port_key = 1 THEN 'FCA'
  ELSE 'Mas'
END                                        AS 'FCA_or_Mas',
ds_first.subdate						   AS sub_date_time,
last_app.decisiondate                      AS last_app_date_time,
last_fu.created_date_time                  AS fun_date_time,
ds_first.decisiondate                      AS first_decision_date_time,
ds_first.decisiondescription			   AS first_decision_s,
ds_last.decisiondate                       AS last_decision_date_time,
ds_last.decisiondescription				   AS last_decision_s,
find_submitted_a.cou_of_submits,
ca.tr_year,
ca.amount_to_be_am_cap,
ca.b_rate,
ca.le_fee_rate,
ca.lo_re_value,
ca.cpp_re_value,
ca.oem_sub_amount_before_tax,
ca.oem_sub_amount,
ca.fun_sub_amount,
latest_date.wf_state_name,
latest_date.up_date_time,
last_app.vin,
ca.vehicle_year,
ino.gkrp_msrp,
ca.ds_name,
ca.d_pro,
ams.ams_name,
ca.is_ewt_currently_sub					   AS c_care_flag,
d.pro									   AS Region,
u.total_d_under_ams,
ca.mo_ins_before_tax,
CASE
  WHEN mkt_prg.mk_program_name LIKE '%alt%' THEN 'Alt'
  ELSE 'Std'
END                                        AS std_or_alt,
ca.accident_health_insurance,
ca.critical_illness_insurance,
f.returningcflag,
res_adj.[re program]            AS re_program,
res_adj.[re program start date] AS re_program_start_date,
date_received.original_fun_pac_date_re,
ino.tds_r_at_decision                    AS TDSR_Last_Decision,
ca.total_an_km,
ca.sec_deposit,
latest_date.up_by,
ca.r_value_original,
ca.l_end_date,
ca.d_key,
ca.ve_delivery_date,
CASE
  WHEN md.id IS NULL THEN 0
  ELSE 1
END                                        AS major_dgg,
CASE
  WHEN pb.id IS NULL THEN 0
  ELSE 1
END                                        AS prev_bkr,
CASE
  WHEN ino.tds_v IS NULL THEN pai.sub_a_inome
  ELSE pai.antotal
END                                        AS prim_an_income,
CASE
  WHEN ino.tds_v IS NULL THEN cai.sub_a_inome
  ELSE cai.antotal
END                                        AS co_app_an_income,
CASE
  WHEN ino.tds_v IS NULL THEN csai.sub_a_inome
  ELSE csai.antotal
END                                        AS co_si_an_income,
wf_f.up_by								   AS funer,
CASE
  WHEN ee.most_recent_app IS NULL THEN 0
  ELSE 1
END                                        AS exposure_exce,
wf_b.up_by                                 AS boo_by,
dr.current_status                          AS current_disp_status
INTO   #t
FROM   v_all ca
       INNER JOIN ca_status s
               ON ca.ca_st_key = s.ca_st_key
       LEFT JOIN v_funing_data_ino ino
              ON ca.id = ino.application_no
                 AND ino.applicant_type = 'Prim'
       LEFT JOIN ca_applicant app
              ON ca.id = app.id
       LEFT JOIN d_flag del_f
              ON ca.id = del_f.id
       LEFT JOIN first_i_due_date first_inv
              ON ca.id = first_inv.id
       LEFT JOIN ca_l_def def
              ON ca.id = def.id
       INNER JOIN date ds
               ON ca.sub_date_key = ds.date_key
       INNER JOIN date da
               ON ca.last_decisioned_date_key = da.date_key
       INNER JOIN date df
               ON ca.funing_date_key = df.date_key
       LEFT JOIN catr
              ON ca.id = catr.id
       LEFT JOIN decisions ds_first
              ON ds_first.id = ca.id
                 AND ds_first.seq_no_asc = 1
       LEFT JOIN decisions ds_last
              ON ds_last.id = ca.id
                 AND ds_last.seq_no_desc = 1
       LEFT JOIN find_last_app last_app
              ON last_app.id = ca.id
       LEFT JOIN find_last_fun last_fu
              ON last_fu.id = ca.id
       LEFT JOIN find_submitted_a
              ON find_submitted_a.id = ca.id
       LEFT JOIN find_ini_ino_app_with_c_app find_ini
              ON ca.id = find_ini.application_no
       LEFT JOIN unique_latest_up_date_time_with_id latest_date
              ON ca.id = latest_date.id
       INNER JOIN d
               ON d.d_key = ca.d_key
       LEFT JOIN ams ams
              ON ams.ams_key = Isnull(d.ams_key, 0)
       LEFT JOIN unique_d_to_ams u
              ON u.ams_key = Isnull(d.ams_key, 0)
       LEFT JOIN mk_program_detail mkt_prg
              ON ca.mk_program_detail_id =
                 mkt_prg.mk_program_detail_id
       LEFT JOIN find_returningcflag f
              ON f.id = ca.id
       LEFT JOIN
 [ADriver_Staging].[dbo].[extract_data_res_value_adjustment_aud]
                                              res_adj
        ON ca.id = res_adj.id
 LEFT JOIN original_fun_pac_date_re date_received
        ON ca.id = date_received.id
 LEFT JOIN determine_ca_tableau_c_major_dgg md
        ON ca.id = md.id
 LEFT JOIN determine_pre_bank pb
        ON ca.id = pb.id
 LEFT JOIN prim_an_total_table pai
        ON ca.id = pai.id
 LEFT JOIN co_app_an_total_table cai
        ON ca.id = cai.id
 LEFT JOIN co_si_an_total_table csai
        ON ca.id = csai.id
 LEFT JOIN wf_fun wf_f
        ON wf_f.id = ca.id
           AND wf_f.seq_no = 1
 LEFT JOIN wf_boo wf_b
        ON wf_b.id = ca.id
           AND wf_b.seq_no = 1
 LEFT JOIN [ATeam].[dbo].[v_active_exposure_exces] ee
        ON ca.id = ee.most_recent_app
 LEFT JOIN ateam.dbo.analytics_team_dis_report dr
        ON ca.id = dr.contract_id
WHERE  ca.sub_date_key > 0

TRUNCATE TABLE ca_tableau_detail

INSERT INTO [dbo].[ca_tableau_detail]
            (id,
             current_s_name,
             sub_date,
             is_app,
             app_date,
             is_fun,
             fun_date,
             prim_app_p,
             prim_app_age,
             have_a_co_app,
             is_comm,
             make,
             model,
             trim,
             body_style,
             l_start_date,
             l_end_date_original,
             term,
             gkrp,
             s_price,
             boo_r,
             l_rate,
             d_payment,
             net_t_in,
             extended_war,
             payment_f,
             ins_before_tax,
             ltv_r,
             ltv_r_at_decision,
             tds_r,
             tds_v,
             pti,
             fco_score,
             fco_score_refresh,
             r_c_rating,
             r_app_score,
             bk_score,
             gross_in,
             gross_in_combined,
             d_flag,
             first_i_due_date,
             def_flag,
             def_from_date,
             def_until_date,
             first_de_user,
             last_de_user,
             poi_waived_flag,
             ca_status,
             fca_or_mas,
             sub_date_time,
             last_app_date_time,
             fun_date_time,
             first_decision_date_time,
             first_decision_s,
             last_decision_date_time,
             last_decision_s,
             cou_of_submits,
             tr_year,
             amount_to_be_am_cap,
             b_rate,
             le_fee_rate,
             lo_re_value,
             cpp_re_value,
             oem_sub_amount_before_tax,
             oem_sub_amount,
             fun_sub_amount,
             wf_state_name,
             up_date_time,
             vin,
             vehicle_year,
             msrp,
             ds_name,
             d_pro,
             ams_name,
             c_care_flag,
             region,
             total_d_under_ams,
             mo_ins_before_tax,
             std_or_alt,
             accident_health_insurance,
             critical_illness_insurance,
             returningcflag,
             re_program,
             re_program_start_date,
             original_fun_pac_date_re,
             tdsr_last_decision,
             total_an_km,
             sec_deposit,
             up_by,
             r_value_original,
             l_end_date,
             d_key,
             ve_delivery_date,
             major_dgg,
             prev_bkr,
             prim_an_income,
             co_app_an_income,
             co_si_an_income,
             funer,
             exposure_exce,
             boo_by,
             current_disp_status)
SELECT id,
       current_s_name,
       sub_date,
       is_app,
       app_date,
       is_fun,
       fun_date,
       prim_app_p,
       prim_app_age,
       have_a_co_app,
       is_comm,
       make,
       model,
       trim,
       body_style,
       l_start_date,
       l_end_date_original,
       term,
       gkrp,
       s_price,
       boo_r,
       l_rate,
       d_payment,
       net_t_in,
       extended_war,
       payment_f,
       ins_before_tax,
       ltv_r,
       ltv_r_at_decision,
       tds_r,
       tds_v,
       pti,
       fco_score,
       fco_score_refresh,
       r_c_rating,
       r_app_score,
       bk_score,
       gross_in,
       gross_in_combined,
       d_flag,
       first_i_due_date,
       def_flag,
       def_from_date,
       def_until_date,
       first_de_user,
       last_de_user,
       poi_waived_flag,
       ca_status,
       fca_or_mas,
       sub_date_time,
       last_app_date_time,
       fun_date_time,
       first_decision_date_time,
       first_decision_s,
       last_decision_date_time,
       last_decision_s,
       cou_of_submits,
       tr_year,
       amount_to_be_am_cap,
       b_rate,
       le_fee_rate,
       lo_re_value,
       cpp_re_value,
       oem_sub_amount_before_tax,
       oem_sub_amount,
       fun_sub_amount,
       wf_state_name,
       up_date_time,
       vin,
       vehicle_year,
       gkrp_msrp,
       ds_name,
       d_pro,
       ams_name,
       c_care_flag,
       region,
       total_d_under_ams,
       mo_ins_before_tax,
       std_or_alt,
       accident_health_insurance,
       critical_illness_insurance,
       returningcflag,
       re_program,
       re_program_start_date,
       original_fun_pac_date_re,
       tdsr_last_decision,
       total_an_km,
       sec_deposit,
       up_by,
       r_value_original,
       l_end_date,
       d_key,
       ve_delivery_date,
       major_dgg,
       prev_bkr,
       prim_an_income,
       co_app_an_income,
       co_si_an_income,
       funer,
       exposure_exce,
       boo_by,
       current_disp_status
FROM   #t 