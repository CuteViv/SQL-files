declare @start_date datetime, @end_date datetime
select @end_date = convert(date,getdate())
select @start_date = convert(date,Dateadd(month,-3, @end_date))

truncate table [AD_Staging].[dbo].[extract_data_sending_stats_details]
truncate table [AD_Staging].[dbo].[extract_data_sending_stats_per_a]
truncate table [AD_Staging].[dbo].[extract_data_sending_stats_per_m]
truncate table [AD_Staging].[dbo].[extract_data_sending_stats_per_a_per_m]


select
ca.ca_id, dt_f.full_date
into #T_sendings
from v_ca_all ca 
inner join date dt_f on dt_f.date_key = ca.sending_date_key
inner join date last_sub_date on ca.last_sub_date_key = last_sub_date.date_key
inner join d d on d.d_key = ca.d_key
left join s_asm on s_asm.s_asm_key = d.s_asm_key
where ca.ca_status_key = 100
and dt_f.full_date  between @start_date and @end_date


;with #T_wf_ready_for_re_all as(
select a.ca_id, 
	   wf.wf_state_name, 
	   wf.u_date_time,
	   wf.u_by,
	   row_number() over (partition by a.ca_id order by wf.u_date_time) as seq_no
FROM ca_input_lea_ino_wf_history wf
inner join ca_input_lea_ino_app a on a.application_id = wf.application_id
inner join #T_sendings f on f.ca_id = a.ca_id and wf.wf_state_type_name = 'AudSendingWFState' and wf.wf_state_id = 1
where wf.u_date_time >= @start_date
)
select *
into #T_wf_ready_for_re
from #T_wf_ready_for_re_all
--where seq_no = 1

;with #T_wf_awaiting_app_all as(
select a.ca_id, 
	   wf.wf_state_name, 
	   wf.u_date_time,
	   wf.u_by,
	   row_number() over (partition by a.ca_id order by wf.u_date_time) as seq_no
FROM ca_input_lea_ino_wf_history wf
inner join ca_input_lea_ino_app a on a.application_id = wf.application_id
inner join #T_Sendings f on f.ca_id = a.ca_id and wf.wf_state_type_name = 'AudSendingWFState' and wf.wf_state_id = 0
where wf.u_date_time >= @start_date
)
select *
into #T_wf_awaiting_app
from #T_wf_awaiting_app_all
--where seq_no = 1



;with #T_wf_def_pen_all as(
select a.ca_id, 
	   wf.wf_state_name, 
	   wf.u_date_time,
	   wf.u_by,
	   row_number() over (partition by a.ca_id order by wf.u_date_time) as seq_no
FROM ca_input_lea_ino_wf_history wf
inner join ca_input_lea_ino_app a on a.application_id = wf.application_id
inner join #T_Sendings f on f.ca_id = a.ca_id and wf.wf_state_type_name = 'AudSendingWFState' and wf.wf_state_id = 3
where wf.u_date_time >= @start_date
)
select *
into #T_wf_def_pen
from #T_wf_def_pen_all
--where seq_no = 1


;with #T_wf_def_new_D_all as(
select a.ca_id, 
	   wf.wf_state_name, 
	   wf.u_date_time,
	   wf.u_by,
	   row_number() over (partition by a.ca_id order by wf.u_date_time) as seq_no
FROM ca_input_lea_ino_wf_history wf
inner join ca_input_lea_ino_app a on a.application_id = wf.application_id
inner join #T_Sendings f on f.ca_id = a.ca_id and wf.wf_state_type_name = 'AudSendingWFState' and wf.wf_state_id = 4
where wf.u_date_time >= @start_date
)
select *
into #T_wf_def_new_d
from #T_wf_def_new_d_all
--where seq_no = 1


;with #T_wf_f_all as(
select a.ca_id, 
	   wf.wf_state_name, 
	   wf.u_date_time,
	   wf.u_by,
	   row_number() over (partition by a.ca_id order by wf.u_date_time) as seq_no
FROM ca_input_lea_ino_wf_history wf
inner join ca_input_lea_ino_app a on a.application_id = wf.application_id
inner join #T_Sendings f on f.ca_id = a.ca_id and wf.wf_state_type_name = 'AudSendingWFState' and wf.wf_state_id in (9, 10)
where wf.u_date_time >= @start_date
)
select *
into #T_wf_f
from #T_wf_f_all
--where seq_no = 1

;with 
stats_after_re as 
(
select ca_id
,wf_state_name
,u_date_time
,u_by
from #T_wf_awaiting_app
union
select ca_id
,wf_state_name
,u_date_time
,u_by
from #T_wf_def_pen
union
select ca_id
,wf_state_name
,u_date_time
,u_by
from #T_wf_f
)
,
r_and_next as 
(
select 
r.ca_id
,r.wf_state_name as Type_1
,r.u_date_time as Type_1_Time
,r.u_by as Type_1_by
,sr.wf_state_name as Type_2
,sr.u_date_time as Type_2_Time
,sr.u_by as Type_2_by
,[dbo].[Get_Business_Response_Time_For_Sending](r.u_date_time, sr.u_date_time) as Type_1_to_Type_2_Duration
,ROW_NUMBER() over(partition by r.ca_id, r.u_date_time order by sr.u_date_time) as seq_no
from #T_wf_ready_for_re r
left join #T_wf_ready_for_re r2 on r.ca_id = r2.ca_id and r2.seq_no = r.seq_no + 1
left join stats_after_re sr on r.ca_id = sr.ca_id and r.u_date_time <= sr.u_date_time and (r2.u_date_time is null or r2.u_date_time > sr.u_date_time)
)
select rn.*
into #T_details
from r_and_next rn 
where rn.seq_no = 1


;with 
stats_after_def_new_d as 
(
select ca_id
,wf_state_name
,u_date_time
,u_by
from #T_wf_def_pen
union
select ca_id
,wf_state_name
,u_date_time
,u_by
from #T_wf_f
)
,
r_and_next as 
(
select 
r.ca_id
,r.wf_state_name as Type_1
,r.u_date_time as Type_1_Time
,r.u_by as Type_1_by
,sr.wf_state_name as Type_2
,sr.u_date_time as Type_2_Time
,sr.u_by as Type_2_by
,[dbo].[Get_Business_Response_Time_For_Sending](r.u_date_time, sr.u_date_time) as Type_1_to_Type_2_Duration
,ROW_NUMBER() over(partition by r.ca_id, r.u_date_time order by sr.u_date_time) as seq_no
from #T_wf_def_new_d r
left join #T_wf_def_new_d r2 on r.ca_id = r2.ca_id and r2.seq_no = r.seq_no + 1
left join stats_after_def_new_d sr on r.ca_id = sr.ca_id and r.u_date_time <= sr.u_date_time and (r2.u_date_time is null or r2.u_date_time > sr.u_date_time)
)
select rn.*
into #T_details_2
from r_and_next rn 
where rn.seq_no = 1


;with find_all_wf_s as
(
select a.ca_id, 
       wf.wf_state_name,
	   ROW_NUMBER() over(partition by a.ca_id order by wf.u_date_time desc) as seq_no
FROM ca_input_lea_ino_wf_history wf
inner join ca_input_lea_ino_app a on a.application_id = wf.application_id
inner join v_ca_all ca on ca.ca_id = a.ca_id
inner join ca_sending_data_ino fd on fd.Application_No = a.ca_id and fd.Applicant_type = 'Primary'
where wf.wf_state_type_name = 'AudSendingWFState'
and wf.u_date_time >= @start_date
)
select *
into #T_find_current_s
from find_all_wf_s
where seq_no = 1

insert into [AD_Staging].[dbo].[extract_data_sending_stats_details]
(
	   [ca_id]
      ,[c_s]
      ,[Type_1]
      ,[Type_1_Time]
      ,[Type_1_by]
      ,[Type_2]
      ,[Type_2_Time]
      ,[Type_2_by]
      ,[Type_1_to_Type_2_Duration_in_Seconds]
      ,[Type_1_to_Type_2_Duration(HH:MM:SS)]
      ,[fd_date]
)
select d.ca_id,
       cs.ca_s_name as c_s,
	   d.Type_1,
	   d.Type_1_Time,
	   d.Type_1_by,
	   d.Type_2,
	   d.Type_2_Time,
	   d.Type_2_by,
	   d.Type_1_to_Type_2_Duration as Type_1_to_Type_2_Duration_in_Seconds,
	   [dbo].[Get_formated_time_from_seconds](d.Type_1_to_Type_2_Duration) as 'Type_1_to_Type_2_Duration(HH:MM:SS)',
	   f.full_date as fd_date
from #T_details d
left join v_ca_all ca on ca.ca_id = d.ca_id 
left join ca_status cs on cs.ca_status_key = ca.ca_status_key
left join #T_sendings f on d.ca_id = f.ca_id
union 
select d.ca_id,
       cs.ca_s_name as c_s,
	   d.Type_1,
	   d.Type_1_Time,
	   d.Type_1_by,
	   d.Type_2,
	   d.Type_2_Time,
	   d.Type_2_by,
	   d.Type_1_to_Type_2_Duration as Type_1_to_Type_2_Duration_in_Seconds,
	   [dbo].[Get_formated_time_from_seconds](d.Type_1_to_Type_2_Duration) as 'Type_1_to_Type_2_Duration(HH:MM:SS)',
	   f.full_date as fd_date
from #T_details_2 d
left join v_ca_all ca on ca.ca_id = d.ca_id 
left join ca_status cs on cs.ca_status_key = ca.ca_status_key
left join #T_sendings f on d.ca_id = f.ca_id
order by ca_id, Type_1_Time, Type_2_Time


; with #T_summary_with_seconds_per_a as(
select 1 as seq,
       'From Ready for Re to Fun' as Group_Name, 
       rfr.Type_2_by as Se_Analyst,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details rfr
where rfr.Type_2 in ('Secu', 'Fun')
group by rfr.Type_2_by
union
select 2 as seq,
       'From Ready for Re to Awaiting Ap' as Group_Name, 
       rfr.Type_2_by as Se_Analyst,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details rfr
where rfr.Type_2 in ('AwaitingAp')
group by rfr.Type_2_by
union
select 3 as seq,
       'From Ready for Re to Def Pen' as Group_Name, 
        rfr.Type_2_by as Se_Analyst,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details rfr
where rfr.Type_2 in ('DefPen')
group by rfr.Type_2_by
union
select 4 as seq,
       'From Def New D to Def Pen' as Group_Name, 
        rfr.Type_2_by as Se_Analyst,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details_2 rfr
where rfr.Type_2 in ('DefPen')
group by rfr.Type_2_by
union
select 5 as seq,
       'From Def New D to Fun' as Group_Name, 
        rfr.Type_2_by as Se_Analyst,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details_2 rfr
where rfr.Type_2 in ('Secu', 'Fun')
group by rfr.Type_2_by
)
select seq, Group_Name, Se_Analyst, UniqueCon, CountOfAc, 
       [dbo].[Get_formated_time_from_seconds](Avg_T) as 'Avg_T(HH:MM:SS)'
into #T_extract_data_sending_stats_per_a
from #T_summary_with_seconds_per_a


insert into [AD_Staging].[dbo].[extract_data_sending_stats_per_a]
(
	   [seq]
      ,[Group_Name]
      ,[Se_Analyst]
      ,[UniqueCon]
      ,[CountOfAc]
      ,[Avg_T(HH:MM:SS)]
)
select seq, 
	   Group_Name, 
	   Se_Analyst, 
	   UniqueCon, 
	   CountOfAc, 
       [Avg_T(HH:MM:SS)]
from #T_extract_data_sending_stats_per_a


;with #T_summary_with_seconds_per_m as(
select 1 as seq,
       'From Ready for Re to Fun' as Group_Name, 
       DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1 as Sending_Mt,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details rfr
where rfr.Type_2 in ('Secu', 'Fun')
group by DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1
union
select 2 as seq,
       'From Ready for Re to Awaiting Ap' as Group_Name, 
       DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1 as Sending_Mt,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details rfr
where rfr.Type_2 in ('AwaitingAp')
group by DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1
union
select 3 as seq,
       'From Ready for Re to Def Pen' as Group_Name, 
       DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1 as Sending_Mt,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details rfr
where rfr.Type_2 in ('DefPen')
group by DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1
union
select 4 as seq,
       'From Def New D to Def Pen' as Group_Name, 
       DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1 as Sending_Mt,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details_2 rfr
where rfr.Type_2 in ('DefPen')
group by DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1
union
select 5 as seq,
       'From Def New D to Fun' as Group_Name, 
       DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1 as Sending_Mt,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details_2 rfr
where rfr.Type_2 in ('Secu', 'Fun')
group by DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1
)
select seq, Group_Name, Sending_Mt, UniqueCon, CountOfAc, 
       [dbo].[Get_formated_time_from_seconds](Avg_T) as 'Avg_T(HH:MM:SS)'
into #T_extract_data_sending_stats_per_m
from #T_summary_with_seconds_per_m


insert into [AD_Staging].[dbo].[extract_data_sending_stats_per_m]
(
	   [seq]
      ,[Group_Name]
      ,[Sending_Mt]
      ,[UniqueCon]
      ,[CountOfAc]
      ,[Avg_T(HH:MM:SS)]
)
select seq, 
	   Group_Name, 
	   Sending_Mt, 
	   UniqueCon, 
	   CountOfAc, 
       [Avg_T(HH:MM:SS)]
from #T_extract_data_sending_stats_per_m


; with #T_summary_with_seconds_per_a_per_m as(
select 1 as seq,
       'From Ready for Re to Fun' as Group_Name, 
       rfr.Type_2_by as Se_Analyst,
	   DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1 as Sending_Mt,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details rfr
where rfr.Type_2 in ('Secu', 'Fun')
group by rfr.Type_2_by, DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1
union
select 2 as seq,
       'From Ready for Re to Awaiting Ap' as Group_Name, 
       rfr.Type_2_by as Se_Analyst,
	   DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1 as Sending_Mt,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details rfr
where rfr.Type_2 in ('AwaitingAp')
group by rfr.Type_2_by, DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1
union
select 3 as seq,
       'From Ready for Re to Def Pen' as Group_Name, 
        rfr.Type_2_by as Se_Analyst,
	   DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1 as Sending_Mt,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details rfr
where rfr.Type_2 in ('DefPen')
group by rfr.Type_2_by, DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1
union
select 4 as seq,
       'From Def New D to Def Pen' as Group_Name, 
        rfr.Type_2_by as Se_Analyst,
	   DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1 as Sending_Mt,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details_2 rfr
where rfr.Type_2 in ('DefPen')
group by rfr.Type_2_by, DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1
union
select 5 as seq,
       'From Def New D to Fun' as Group_Name, 
        rfr.Type_2_by as Se_Analyst,
	   DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1 as Sending_Mt,
       count(distinct rfr.ca_id) as UniqueCon,
       count(rfr.ca_id) as CountOfAc,
	   avg(rfr.Type_1_to_Type_2_Duration) as Avg_T
from #T_details_2 rfr
where rfr.Type_2 in ('Secu', 'Fun')
group by rfr.Type_2_by, DATEPART(year, rfr.Type_2_Time)*100 + DATEPART(month, rfr.Type_2_Time)*1
)
select seq, Group_Name, Se_Analyst, Sending_Mt, UniqueCon, CountOfAc, 
       [dbo].[Get_formated_time_from_seconds](Avg_T) as 'Avg_T(HH:MM:SS)'
into #T_extract_data_sending_stats_per_a_per_m
from #T_summary_with_seconds_per_a_per_m
order by Group_Name, Se_Analyst, Sending_Mt


insert into [AD_Staging].[dbo].[extract_data_sending_stats_per_a_per_m]
(
       [seq]
      ,[Group_Name]
      ,[Se_Analyst]
      ,[Sending_Mt]
      ,[UniqueCon]
      ,[CountOfAc]
      ,[Avg_T(HH:MM:SS)]
)
select seq, 
	   Group_Name, 
	   Se_Analyst, 
	   Sending_Mt, 
	   UniqueCon, 
	   CountOfAc, 
       [Avg_T(HH:MM:SS)]
from #T_extract_data_sending_stats_per_a_per_m
