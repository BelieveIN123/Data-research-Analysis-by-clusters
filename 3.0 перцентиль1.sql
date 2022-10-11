drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_split2_percentile1;

create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_split2_percentile1
as 
(
 select distinct epk_id, 
 				ntile_ as ntile_1, 
 				max(T1.pnl_oi_total_amt) over (partition by split_1, T1.ntile_) as ntile_1_max_v,
 				min(T1.pnl_oi_total_amt) over (partition by split_1, T1.ntile_) as ntile_1_min_v
 from (select epk_id,
 				pnl_oi_total_amt, 
 				case
 					when split_1 = 'left_sbp' and pnl_oi_total_amt < 0
 					then 1
 					when split_1 = 'left_sbp' and pnl_oi_total_amt >= 0 and pnl_oi_total_amt < 2500
 					then 2
 					when split_1 = 'left_sbp' and pnl_oi_total_amt >= 2500 and pnl_oi_total_amt < 5000
 					then 3
 					when split_1 = 'left_sbp' and pnl_oi_total_amt >= 5000 and pnl_oi_total_amt < 10000
 					then 4
 					when split_1 = 'left_sbp' and pnl_oi_total_amt >= 10000
 					then 5
 					
 					when split_1 = 'pachege_sbp' and pnl_oi_total_amt < 0
 					then 1
 					when split_1 = 'pachege_sbp' and pnl_oi_total_amt >= 0 and pnl_oi_total_amt < 2500
 					then 2
 					when split_1 = 'pachege_sbp' and pnl_oi_total_amt >= 2500 and pnl_oi_total_amt < 5000
 					then 3
 					when split_1 = 'pachege_sbp' and pnl_oi_total_amt >= 5000 and pnl_oi_total_amt < 10000
 					then 4
 					when split_1 = 'pachege_sbp' and pnl_oi_total_amt >= 10000
 					then 5
 					
 					when split_1 = 'sbp' and pnl_oi_total_amt < 0
 					then 1
 					when split_1 = 'sbp' and pnl_oi_total_amt >= 0 and pnl_oi_total_amt < 2500
 					then 2
 					when split_1 = 'sbp' and pnl_oi_total_amt >= 2500 and pnl_oi_total_amt < 5000
 					then 3
 					when split_1 = 'sbp' and pnl_oi_total_amt >= 5000 and pnl_oi_total_amt < 10000
 					then 4
 					when split_1 = 'sbp' and pnl_oi_total_amt >= 10000
 					then 5
 				end as ntile_, split_1
 
  	   from (select epk_id, split_1, pnl_oi_total_amt  
	  		 from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg 
	  		 where year_ = 2021) T2
	 ) T1
);




------------------------------------
------1 разбиение по общей прибыли
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1;

create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1
as 
(
select  t2.ntile_1, 
		t2.ntile_1_max_v, 
		t2.ntile_1_min_v, 
		t1.* 
	from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg t1
join s_grnplm_ld_rozn_electron_hq_core_dm.aka_split2_percentile1 t2 
on t1.epk_id  = t2.epk_id
);







--1.1 Агрегация по разбиению по общей прибыли. 

---------------------------------------------- 000 pnl_oi_total_amt
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000;

create  table 
s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000
as
(
select t1.Split_1, t1.ntile_1,
concat('от ',round(avg(ntile_1_min_v), -3), ' до ', round(avg(ntile_1_max_v), -3) ) as ntile_1_max_v,
t1.year_,
--T_inf.seg_service_channel_cd, --сервисный канал  
--T_inf.report_dt, --дата  
--prd_client_active_nflag, --Активен ли клиент  
--tp_active_kind_cd, --Название активного пакета услуг  
--tp_active_nflag, --Идентификатор активности клиента
--		count(*) as count_people,
--сумма людей
count(distinct epk_id) as unique_epk_id,
case when type_CITY_MLNR = 1 then 'CITY_MLNR' when type_VILLAGE = 1 then 'VILLAGE' when type_CITY_OTHER=1 then 'CITY_OTHER' else 'CITY_OTHER' end as nasp,

avg(pnl_oi_total_amt)                as pnl_oi_total_amt, -- Сумма общего операционного дохода (ОД) по клиенту за отчетный месяц

--ntile_pnl_oi_pl_amt,
--Вклады
--avg(pnl_oi_amt_sum) 	             as pnl_oi_amt_sum, --ОД вклады
--avg(dep_acct_dep_qty_sum)            as count_dep, --Количество вклады

--pnl_oi_td_amt, -- //Сумма операционного дохода (ОД) по клиенту за отчетный месяц по срочным счетам  
--pnl_oi_ca_amt, --//Сумма операционного дохода (ОД) по клиенту за отчетный месяц по текущим счетам  
--dep_acct_dep_td_qty, --СРОЧН Кол-во действующих  счетов срочных вкладов клиента на отчетную дату  
--dep_acct_dep_ca_qty, --ТЕКУЩ Кол-во действующих текущих счетов клиента на отчетную дату  
--avg(dep_acct_dep_tot_bal_rub_amt)    as dep_acct_dep_tot_bal_rub_amt, -- СДО по вкладам / Суммарный баланс по депозитным (текущим и срочным) счетам клиента на отчетную дату в рублях  
--Пк  
--avg(pnl_oi_pl_amt) 					 as pnl_oi_pl_amt, -- ОД ПК      / Сумма операционного дохода (ОД) по клиенту за отчетный месяц по потребительским кредитам  
--avg(lne_pl_debt_os_rub_amt)          as lne_pl_debt_os_rub_amt,   --ПК ср /  
--avg(prd_pl_active_qty)               as prd_pl_active_qty,  --ПК Количество активных потребительских кредитов за 3 мес
  
--ЖК  
--avg(pnl_oi_ml_amt)                   as pnl_oi_ml_amt, -- ОД по ЖК /Сумма операционного дохода (ОД) по клиенту за отчетный месяц по жилищным (ипотечным) кредитам  
--avg(lne_mg_debt_os_rub_amt)          as lne_mg_debt_os_rub_amt,   --ЖК / Суммарный остаток основного долга (срочная часть + просрочка) - ипотека  
--avg(prd_mg_active_qty)               as prd_mg_active_qty, --ЖК Количество активных ипотек за 3 мес
--КК  
--avg(pnl_oi_cc_amt)                   as pnl_oi_cc_amt, -- ОД по КК / Сумма операционного дохода (ОД) по клиенту за отчетный месяц по кредитным картам  
--avg(crd_cc_tot_credit_rub_amt)       as crd_cc_tot_credit_rub_amt,--КК / Сумма полной задолженности по КК (основной долг + % + просрочка + штрафы) на отчетную дату, руб
--avg(crd_cc_active_open_qty)			 as crd_cc_active_open_qty, --КК / Количество активных открытых кредитных карт  
  
--ДК  
--avg(pnl_oi_dc_amt)                   as pnl_oi_dc_amt, -- ОД по картам **дебет/ Сумма операционного дохода (ОД) по клиенту за отчетный месяц по дебетовым картам  
--avg(crd_dc_act_spend_qty)            as crd_dc_act_spend_qty, --ДЕБ. Кол-во активных по расходным операциям дебетовых карт  
--  
--ЭКВ  
--avg(SUM_pnl_T_acquring)				 as SUM_pnl_T_acquring, -- ОД экваринг  
  
--Проч  
--avg(pnl_oi_othr_amt) 				 as pnl_oi_othr_amt, -- ОД по прочим / 
--ОД Инвест  
--avg(SUM_pnl_T_invest) 			     as SUM_pnl_T_invest, --доход по инвест   
--avg(inv_tot_bal_rub_amt) 			 as inv_tot_bal_rub_amt, --Инвесты / Общая сумма активов в инвестиционных продуктах (ПИФ+ДУ+БО) на конец месяца в рублях  
--avg(inv_bo_agrmnt_open_qty)			 as inv_bo_agrmnt_open_qty, --Количество открытых договоров БО на конец месяца  
--ОД Пакет  
  
--Общ  
--avg(dep_acct_tot_bal_rub_amt) as dep_acct_tot_bal_rub_amt, --СДО (пассивы) / Суммарный баланс по всем пассивным счетам клиента на отчетную дату в рублях (без сбер серт)  
--avg(crd_pos_net_rub_amt),      --POS-оборот /Чистая сумма POS-операций (за минусом возвратов)  

--Доп  
AVG(dep_acct_tot_davg_mnth_rub_amt)  as dep_acct_tot_davg_mnth_rub_amt, --Суммарный среднедневной остаток (СДО) за отчетный месяц
----Инф о человеке1
avg(sd_gender_cd)                    as sd_gender_cd, --Пол
avg(sd_age_yrs_comp_nv)              as sd_age_yrs_comp_nv, -- Возраст клиента, полное  число лет
--max(t2.sd_stlmnt_type_cd)            as t2.sd_stlmnt_type_cd, -- Тип населенного пункта проживания клиента (по финальному адресу) - альтернативный алгоритм
-- avg(type_CITY_MLNR)                  as type_CITY_MLNR, 	-- Тип населенного пункта
-- avg(type_VILLAGE)                    as type_VILLAGE,		-- Тип населенного пункта
-- avg(type_CITY_OTHER)                 as type_CITY_OTHER,	-- Тип населенного пункта
sum(dep_payroll_client_nflag)        as sum_dep_payroll_client_nflag, -- Флаг зарплатного клиента (среднемесячные з/п начисления за 3 мес. хотя бы по одному из счетов > 3000 руб.)
avg(dep_payroll_client_nflag)        as avg_dep_payroll_client_nflag, -- Флаг зарплатного клиента (среднемесячные з/п начисления за 3 мес. хотя бы по одному из счетов > 3000 руб.)
--AVG(dep_inc_avg_risk_rub_amt)        as dep_inc_avg_risk_rub_amt, -- Общий среднемесячный доход (зарплата + соц.начисления) по методике Рисков
AVG(dep_inf_income_rub_amt)          as dep_inf_income_rub_amt,   -- Сумма поступлений на счет (зарплата, пенсии, больничные)
AVG(crd_otf_cash_rub_amt)            as crd_otf_cash_rub_amt, --Общая сумма снятия наличных по всем картам клиента за месяц в рублях
AVG(crd_otf_cash_atm_qty)            as crd_otf_cash_atm_qty, -- Количество снятий наличных в банкомате

AVG(srv_dc_p2p_otf_on_amt)           as srv_dc_p2p_otf_on_amt,  --Сумма переводов другим физ.лицам - клиентам банка по ДК за отчетный месяц (СБОЛ, МБ)
AVG(srv_dc_p2p_inf_on_amt)           as srv_dc_p2p_inf_on_amt, --Сумма переводов от других физ.лиц - клиентов банка по ДК за отчетный месяц (СБОЛ, МБ)
AVG(srv_cc_p2p_otf_on_amt)           as srv_cc_p2p_otf_on_amt, --Сумма переводов другим физ.лицам - клиентам банка по КК за отчетный месяц (СБОЛ, МБ)
AVG(srv_cc_p2p_inf_on_amt)           as srv_cc_p2p_inf_on_amt --Сумма переводов от других физ.лиц - клиентов банка по КК за отчетный месяц (СБОЛ, МБ)

from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 t1
--join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t2
--on t1.epk_id = t2.epk_id
group by Split_1, ntile_1, year_, case when type_CITY_MLNR = 1 then 'CITY_MLNR' when type_VILLAGE = 1 then 'VILLAGE' when type_CITY_OTHER=1 then 'CITY_OTHER' else 'CITY_OTHER' end
order by Split_1, ntile_1, year_--, ntile_pnl_oi_pl_amt
);




----------------------------------------------
-----------------------------------------------------
----------------------------------------------------------------
----------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
--Добавляю ранг в таблицу aka_client_sbp_2021_info_agg_p000

drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000_n;

create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000_n
as 
(select dense_rank() over (partition by Split_1, year_ order by  nasp) as nasp_n_split, * from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000
--order by Split_1, year_ desc, ntile_1, nasp
);
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000
as 
(select * from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000_n);
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000_n;




--1.2 Агрегация по разбиению по общей прибыли для итоговой левой части. 
--Отстутствие разбиения по nasp 

---------------------------------------------- 000 pnl_oi_total_amt
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000_no_nasp;

create  table 
s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000_no_nasp
as
(
select t1.Split_1, t1.year_, t1.ntile_1,
concat('от ',round(avg(ntile_1_min_v), -3), ' до ', round(avg(ntile_1_max_v), -3) ) as ntile_1_max_v,
--T_inf.seg_service_channel_cd, --сервисный канал  
--T_inf.report_dt, --дата  
--prd_client_active_nflag, --Активен ли клиент  
--tp_active_kind_cd, --Название активного пакета услуг  
--tp_active_nflag, --Идентификатор активности клиента
--		count(*) as count_people,
--сумма людей
count(distinct epk_id) as count_people,
avg(pnl_oi_total_amt)                as pnl_oi_total_amt, -- Сумма общего операционного дохода (ОД) по клиенту за отчетный месяц

--ntile_pnl_oi_pl_amt,
--Вклады
--avg(pnl_oi_amt_sum) 	             as pnl_oi_amt_sum, --ОД вклады
--avg(dep_acct_dep_qty_sum)            as count_dep, --Количество вклады

--pnl_oi_td_amt, -- //Сумма операционного дохода (ОД) по клиенту за отчетный месяц по срочным счетам  
--pnl_oi_ca_amt, --//Сумма операционного дохода (ОД) по клиенту за отчетный месяц по текущим счетам  
--dep_acct_dep_td_qty, --СРОЧН Кол-во действующих  счетов срочных вкладов клиента на отчетную дату  
--dep_acct_dep_ca_qty, --ТЕКУЩ Кол-во действующих текущих счетов клиента на отчетную дату  
--avg(dep_acct_dep_tot_bal_rub_amt)    as dep_acct_dep_tot_bal_rub_amt, -- СДО по вкладам / Суммарный баланс по депозитным (текущим и срочным) счетам клиента на отчетную дату в рублях  
--Пк  
--avg(pnl_oi_pl_amt) 					 as pnl_oi_pl_amt, -- ОД ПК      / Сумма операционного дохода (ОД) по клиенту за отчетный месяц по потребительским кредитам  
--avg(lne_pl_debt_os_rub_amt)          as lne_pl_debt_os_rub_amt,   --ПК ср /  
--avg(prd_pl_active_qty)               as prd_pl_active_qty,  --ПК Количество активных потребительских кредитов за 3 мес
  
--ЖК  
--avg(pnl_oi_ml_amt)                   as pnl_oi_ml_amt, -- ОД по ЖК /Сумма операционного дохода (ОД) по клиенту за отчетный месяц по жилищным (ипотечным) кредитам  
--avg(lne_mg_debt_os_rub_amt)          as lne_mg_debt_os_rub_amt,   --ЖК / Суммарный остаток основного долга (срочная часть + просрочка) - ипотека  
--avg(prd_mg_active_qty)               as prd_mg_active_qty, --ЖК Количество активных ипотек за 3 мес
--КК  
--avg(pnl_oi_cc_amt)                   as pnl_oi_cc_amt, -- ОД по КК / Сумма операционного дохода (ОД) по клиенту за отчетный месяц по кредитным картам  
--avg(crd_cc_tot_credit_rub_amt)       as crd_cc_tot_credit_rub_amt,--КК / Сумма полной задолженности по КК (основной долг + % + просрочка + штрафы) на отчетную дату, руб
--avg(crd_cc_active_open_qty)			 as crd_cc_active_open_qty, --КК / Количество активных открытых кредитных карт  
  
--ДК  
--avg(pnl_oi_dc_amt)                   as pnl_oi_dc_amt, -- ОД по картам **дебет/ Сумма операционного дохода (ОД) по клиенту за отчетный месяц по дебетовым картам  
--avg(crd_dc_act_spend_qty)            as crd_dc_act_spend_qty, --ДЕБ. Кол-во активных по расходным операциям дебетовых карт  
--  
--ЭКВ  
--avg(SUM_pnl_T_acquring)				 as SUM_pnl_T_acquring, -- ОД экваринг  
  
--Проч  
--avg(pnl_oi_othr_amt) 				 as pnl_oi_othr_amt, -- ОД по прочим / 
--ОД Инвест  
--avg(SUM_pnl_T_invest) 			     as SUM_pnl_T_invest, --доход по инвест   
--avg(inv_tot_bal_rub_amt) 			 as inv_tot_bal_rub_amt, --Инвесты / Общая сумма активов в инвестиционных продуктах (ПИФ+ДУ+БО) на конец месяца в рублях  
--avg(inv_bo_agrmnt_open_qty)			 as inv_bo_agrmnt_open_qty, --Количество открытых договоров БО на конец месяца  
--ОД Пакет  
  
--Общ  
--avg(dep_acct_tot_bal_rub_amt) as dep_acct_tot_bal_rub_amt, --СДО (пассивы) / Суммарный баланс по всем пассивным счетам клиента на отчетную дату в рублях (без сбер серт)  
--avg(crd_pos_net_rub_amt),      --POS-оборот /Чистая сумма POS-операций (за минусом возвратов)  

--Доп  
--AVG(dep_acct_tot_davg_mnth_rub_amt)  as dep_acct_tot_davg_mnth_rub_amt, --Суммарный среднедневной остаток (СДО) за отчетный месяц
----Инф о человеке1
avg(sd_gender_cd)                    as sd_gender_cd, --Пол
avg(sd_age_yrs_comp_nv)              as sd_age_yrs_comp_nv, -- Возраст клиента, полное  число лет
--max(t2.sd_stlmnt_type_cd)            as t2.sd_stlmnt_type_cd, -- Тип населенного пункта проживания клиента (по финальному адресу) - альтернативный алгоритм
-- avg(type_CITY_MLNR)                  as type_CITY_MLNR, 	-- Тип населенного пункта
-- avg(type_VILLAGE)                    as type_VILLAGE,		-- Тип населенного пункта
-- avg(type_CITY_OTHER)                 as type_CITY_OTHER,	-- Тип населенного пункта
sum(dep_payroll_client_nflag)        as sum_dep_payroll_client_nflag, -- Флаг зарплатного клиента (среднемесячные з/п начисления за 3 мес. хотя бы по одному из счетов > 3000 руб.)
avg(dep_payroll_client_nflag)        as avg_dep_payroll_client_nflag -- Флаг зарплатного клиента (среднемесячные з/п начисления за 3 мес. хотя бы по одному из счетов > 3000 руб.)
--AVG(dep_inc_avg_risk_rub_amt)        as dep_inc_avg_risk_rub_amt, -- Общий среднемесячный доход (зарплата + соц.начисления) по методике Рисков
--AVG(dep_inf_income_rub_amt)          as dep_inf_income_rub_amt,   -- Сумма поступлений на счет (зарплата, пенсии, больничные)
--AVG(crd_otf_cash_rub_amt)            as crd_otf_cash_rub_amt, --Общая сумма снятия наличных по всем картам клиента за месяц в рублях
--AVG(crd_otf_cash_atm_qty)            as crd_otf_cash_atm_qty, -- Количество снятий наличных в банкомате

--AVG(srv_dc_p2p_otf_on_amt)           as srv_dc_p2p_otf_on_amt,  --Сумма переводов другим физ.лицам - клиентам банка по ДК за отчетный месяц (СБОЛ, МБ)
--AVG(srv_dc_p2p_inf_on_amt)           as srv_dc_p2p_inf_on_amt, --Сумма переводов от других физ.лиц - клиентов банка по ДК за отчетный месяц (СБОЛ, МБ)
--AVG(srv_cc_p2p_otf_on_amt)           as srv_cc_p2p_otf_on_amt, --Сумма переводов другим физ.лицам - клиентам банка по КК за отчетный месяц (СБОЛ, МБ)
--AVG(srv_cc_p2p_inf_on_amt)           as srv_cc_p2p_inf_on_amt --Сумма переводов от других физ.лиц - клиентов банка по КК за отчетный месяц (СБОЛ, МБ)

from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 t1
--join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t2
--on t1.epk_id = t2.epk_id
group by Split_1, year_, ntile_1
order by Split_1, year_, ntile_1--, ntile_pnl_oi_pl_amt
);

