
set optimizer = on;
--------------------Создаю список людей
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021
;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021 as (
with
client_mass as (
select epk_id, seg_service_channel_cd from s_grnplm_ld_rozn_electron_daas_core_dm.ft_client_aggr_mnth_epk
where 1 = 1
and seg_service_channel_cd in ('MASS')
and report_dt = '2020-12-31' --дата среза
and prd_1st_prod_open_dt <= '2020-07-01' --дата первого продукта
and epk_id not in (-1, 0)
and epk_id is not null
and prd_client_active_nflag=1
)
select  distinct client_sbp.epk_id
from s_grnplm_ld_rozn_electron_daas_core_dm.ft_client_aggr_mnth_epk client_sbp
inner join client_mass
on client_sbp.epk_id = client_mass.epk_id
where 1 = 1
	and client_sbp.seg_service_channel_cd = 'SBP'
	and client_sbp.report_dt = '2021-01-31'
	and client_sbp.epk_id not in (select epk_id from s_grnplm_ld_rozn_electron_daas_core_dm.ft_client_aggr_mnth_epk where sd_dead_nflag = 1 and report_dt = '2021-12-31') --дроп умерших --!!!правил
);



create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_need_drop as (select T_inf.epk_id
									from s_grnplm_ld_rozn_electron_daas_core_dm.ft_client_aggr_mnth_epk as T_inf
									join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021 t_cl on T_inf.epk_id =t_cl.epk_id
									where 1=1
										and T_inf.report_dt >  '2020-01-01'
										and T_inf.report_dt <  '2022-01-01' 
									group by T_inf.epk_id
									having count(*) < 24) --Проверяю на наличие 24 мес. в последстии обрежу, буру брать 22
;
delete from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021 t1 where epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_need_drop);
drop table s_grnplm_ld_rozn_electron_hq_core_dm.aka_need_drop;
--выкидываем ситуации когда нет 24 мес за 2 года (35 чел. после дропа умерших) --!!!правил (добавил)

--------------------------------------------------
--------------------------------------------------
--------------------------------------------------

drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info_acquring
;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info_acquring
as 
( 
select epk_id, report_dt, sum(pl_metric_val) as SUM_pnl_T_acquring from s_grnplm_ld_rozn_electron_daas_core_dm.ft_pl_epk
where 1=1 
	and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021)
	and report_dt < '2022-01-01'
	and report_dt >= '2020-01-01'
	and pl_product_id in (
							select distinct pl_product_id from s_grnplm_ld_rozn_electron_daas_core_dm.ref_pl_product rpp 
							where pl_product_l2_short_name like '%Эквайринг%'	
						 )		
group by epk_id, report_dt);
--------------------------------------------------
--------------------------------------------------
--------------------------------------------------

drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info_invest
;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info_invest
as 
( 
select epk_id, report_dt, sum(pl_metric_val) as SUM_pnl_T_invest from s_grnplm_ld_rozn_electron_daas_core_dm.ft_pl_epk
where 1=1 
	and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021)
	and report_dt <= '2022-02-01' --25 месяцев для второй выгрузки
	and report_dt >= '2020-01-01'
	and pl_product_id in (
							select distinct pl_product_id from s_grnplm_ld_rozn_electron_daas_core_dm.ref_pl_product rpp 
							where pl_product_l5_full_name like '%инвест%'
						 )		
group by epk_id, report_dt);


--------------------------------------------------
--------------------------------------------------
--------------------------------------------------

drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info
;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info
as (
select  

		 T_inf.epk_id 	                      as epk_id, --id клиента   
	      T_inf.seg_service_channel_cd        as seg_service_channel_cd, --сервисный канал  
		 T_inf.report_dt                      as report_dt, --дата  
COALESCE(prd_client_active_nflag, 0)           as prd_client_active_nflag, --Активен ли клиент  
 		tp_active_kind_cd	                   as tp_active_kind_cd, --Название активного пакета услуг  
COALESCE(tp_active_nflag, 1)                   as tp_active_nflag, --Идентификатор активности клиента  
  
COALESCE(pnl_oi_total_amt, 0)                  as pnl_oi_total_amt, -- Сумма общего операционного дохода (ОД) по клиенту за отчетный месяц  
  
--Вклады  
COALESCE(pnl_oi_td_amt, 0)                     as pnl_oi_td_amt, -- //Сумма операционного дохода (ОД) по клиенту за отчетный месяц по срочным счетам  
COALESCE(pnl_oi_ca_amt, 0)                     as pnl_oi_ca_amt, --//Сумма операционного дохода (ОД) по клиенту за отчетный месяц по текущим счетам  
COALESCE(dep_acct_dep_td_qty, 0)               as dep_acct_dep_td_qty, --СРОЧН Кол-во действующих  счетов срочных вкладов клиента на отчетную дату  
COALESCE(dep_acct_dep_ca_qty, 0)               as dep_acct_dep_ca_qty, --ТЕКУЩ Кол-во действующих текущих счетов клиента на отчетную дату  
COALESCE(dep_acct_dep_tot_bal_rub_amt, 0)      as dep_acct_dep_tot_bal_rub_amt, -- СДО по вкладам / Суммарный баланс по депозитным (текущим и срочным) счетам клиента на отчетную дату в рублях
--dep_acct_dep_td_bal_rub_amt суммарнй баланс по всем вкладм   
--Пк  
COALESCE(pnl_oi_pl_amt, 0)                     as pnl_oi_pl_amt, -- ОД ПК      / Сумма операционного дохода (ОД) по клиенту за отчетный месяц по потребительским кредитам  
COALESCE(lne_pl_debt_os_rub_amt, 0)            as lne_pl_debt_os_rub_amt,   --ПК ср /  Суммарный остаток основного долга (срочная часть + просрочка) - потребительские кредиты (PL)
COALESCE(prd_pl_active_qty, 0)                 as prd_pl_active_qty,  --ПК Количество активных потребительских кредитов за 3 мес.
COALESCE(lne_overdraft_overdue_nflag, 0)       as lne_overdraft_overdue_nflag, --наличие просрочки
  
--ЖК  
COALESCE(pnl_oi_ml_amt, 0)                     as pnl_oi_ml_amt, -- ОД по ЖК /Сумма операционного дохода (ОД) по клиенту за отчетный месяц по жилищным (ипотечным) кредитам  
COALESCE(lne_mg_debt_os_rub_amt, 0)            as lne_mg_debt_os_rub_amt,   --ЖК / Суммарный остаток основного долга (срочная часть + просрочка) - ипотека  
COALESCE(prd_mg_active_qty, 0)                 as prd_mg_active_qty, --ЖК Количество активных ипотек за 3 мес.  
--КК  
COALESCE(pnl_oi_cc_amt, 0)                     as pnl_oi_cc_amt, -- ОД по КК / Сумма операционного дохода (ОД) по клиенту за отчетный месяц по кредитным картам  
COALESCE(crd_cc_tot_credit_rub_amt, 0)         as crd_cc_tot_credit_rub_amt,--КК / Сумма полной задолженности по КК (основной долг + % + просрочка + штрафы) на отчетную дату, руб
COALESCE(crd_cc_active_open_qty, 0)            as crd_cc_active_open_qty, --КК / Количество активных открытых кредитных карт  
--
COALESCE(crd_cc_open_qty, 0)                   as crd_cc_open_qty,
COALESCE(crd_cc_act_spend_qty, 0)              as crd_cc_act_spend_qty,
--
  
--ДК  
COALESCE(pnl_oi_dc_amt, 0)                     as pnl_oi_dc_amt, -- ОД по картам **дебет/ Сумма операционного дохода (ОД) по клиенту за отчетный месяц по дебетовым картам  
COALESCE(crd_dc_act_spend_qty, 0)              as crd_dc_act_spend_qty, --ДЕБ. Кол-во активных по расходным операциям дебетовых карт 
--
COALESCE(crd_dc_open_qty, 0)				   as crd_dc_open_qty,
--  
  
--ЭКВ  
COALESCE(pnl_T_acquring.SUM_pnl_T_acquring, 0) as SUM_pnl_T_acquring, -- ОД экваринг  

COALESCE(crd_pos_auto_3m_qty, 0) as crd_pos_auto_3m_qty, --Количество операций покупки в категории "Авто/мото" за месяц по картам за 3 мес.
COALESCE(crd_pos_money_trf_3m_qty, 0) as crd_pos_money_trf_3m_qty, --Количество операций покупки в категории "Безналичные денежные операции" за месяц по картам за 3 мес.
COALESCE(crd_pos_eat_out_3m_qty, 0) as crd_pos_eat_out_3m_qty, --Количество операций покупки в категории "Кафе/бары/рестораны" за месяц по картам за 3 мес.
COALESCE(crd_pos_utilities_3m_qty, 0) as crd_pos_utilities_3m_qty, --Количество операций покупки в категории "Коммунальные платежи" за месяц по картам за 3 мес.
COALESCE(crd_pos_pc_it_3m_qty, 0) as crd_pos_pc_it_3m_qty, --Количество операций покупки в категории "Компьютеры/Гаджеты/ПО/IT-услуги" за месяц по картам за 3 мес.
COALESCE(crd_pos_beuaty_3m_qty, 0) as crd_pos_beuaty_3m_qty, --Количество операций покупки в категории "Красота и здоровье" за месяц по картам за 3 мес.
COALESCE(crd_pos_healthcare_3m_qty, 0) as crd_pos_healthcare_3m_qty, --Количество операций покупки в категории "Медицина" за месяц по картам за 3 мес.
COALESCE(crd_pos_clothes_3m_qty, 0) as crd_pos_clothes_3m_qty, --Количество операций покупки в категории "Одежда, обувь и аксессуары" за месяц по картам за 3 мес.
COALESCE(crd_pos_leisure_3m_qty, 0) as crd_pos_leisure_3m_qty, --Количество операций покупки в категории "Отдых/развлечения/спорт" за месяц по картам за 3 мес.
COALESCE(crd_pos_groceries_3m_qty, 0) as crd_pos_groceries_3m_qty, --Количество операций покупки в категории "Продукты" за месяц по картам за 3 мес.
COALESCE(crd_pos_othr_3m_qty, 0) as crd_pos_othr_3m_qty, --Количество операций покупки в категории "Прочее" за месяц по картам за 3 мес.
COALESCE(crd_pos_special_3m_qty, 0) as crd_pos_special_3m_qty, --Количество операций покупки в категории "Специализированные товары и услуги" за месяц по картам за 3 мес.
COALESCE(crd_pos_telecom_3m_qty, 0) as crd_pos_telecom_3m_qty, --Количество операций покупки в категории "Телекоммуникационные услуги" за месяц по картам за 3 мес.
COALESCE(crd_pos_home_repair_3m_qty, 0) as crd_pos_home_repair_3m_qty, --Количество операций покупки в категории "Товары для дома/ремонта" за месяц по картам за 3 мес.
COALESCE(crd_pos_tourism_3m_qty, 0) as crd_pos_tourism_3m_qty, --Количество операций покупки в категории "Туризм/транспорт" за месяц по картам за 3 мес.
COALESCE(crd_pos_dept_stores_3m_qty, 0) as crd_pos_dept_stores_3m_qty, --Количество операций покупки в категории "Универсальные магазины" за месяц по картам за 3 мес.
COALESCE(crd_pos_auto_rub_3m_amt, 0) as crd_pos_auto_rub_3m_amt, --Сумма операций покупки в категории "Авто/мото" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_money_trf_rub_3m_amt, 0) as crd_pos_money_trf_rub_3m_amt, --Сумма операций покупки в категории "Безналичные денежные операции" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_eat_out_rub_3m_amt, 0) as crd_pos_eat_out_rub_3m_amt, --Сумма операций покупки в категории "Кафе/бары/рестораны" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_utilities_rub_3m_amt, 0) as crd_pos_utilities_rub_3m_amt, --Сумма операций покупки в категории "Коммунальные платежи" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_pc_it_rub_3m_amt, 0) as crd_pos_pc_it_rub_3m_amt, --Сумма операций покупки в категории "Компьютеры/Гаджеты/ПО/IT-услуги" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_beuaty_rub_3m_amt, 0) as crd_pos_beuaty_rub_3m_amt, --Сумма операций покупки в категории "Красота и здоровье" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_healthcare_rub_3m_amt, 0) as crd_pos_healthcare_rub_3m_amt, --Сумма операций покупки в категории "Медицина" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_clothes_rub_3m_amt, 0) as crd_pos_clothes_rub_3m_amt, --Сумма операций покупки в категории "Одежда, обувь и аксессуары" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_leisure_rub_3m_amt, 0) as crd_pos_leisure_rub_3m_amt, --Сумма операций покупки в категории "Отдых/развлечения/спорт" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_groceries_rub_3m_amt, 0) as crd_pos_groceries_rub_3m_amt, --Сумма операций покупки в категории "Продукты" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_othr_3m_amt, 0) as crd_pos_othr_3m_amt, --Сумма операций покупки в категории "Прочее" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_special_rub_3m_amt, 0) as crd_pos_special_rub_3m_amt, --Сумма операций покупки в категории "Специализированные товары и услуги" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_telecom_rub_3m_amt, 0) as crd_pos_telecom_rub_3m_amt, --Сумма операций покупки в категории "Телекоммуникационные услуги" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_home_repair_rub_3m_amt, 0) as crd_pos_home_repair_rub_3m_amt, --Сумма операций покупки в категории "Товары для дома/ремонта" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_tourism_rub_3m_amt, 0) as crd_pos_tourism_rub_3m_amt, --Сумма операций покупки в категории "Туризм/транспорт" в рублях за месяц по картам за 3 мес.
COALESCE(crd_pos_dept_stores_rub_3m_amt, 0) as crd_pos_dept_stores_rub_3m_amt, --Сумма операций покупки в категории "Универсальные магазины" в рублях за месяц по картам за 3 мес.

  
--Проч  
COALESCE(pnl_oi_othr_amt, 0)                   as pnl_oi_othr_amt, -- ОД по прочим / 
--ОД Инвест  
COALESCE(pnl_T_invest.SUM_pnl_T_invest, 0)     as SUM_pnl_T_invest, --доход по инвест   
COALESCE(inv_tot_bal_rub_amt, 0)               as inv_tot_bal_rub_amt, --Инвесты / Общая сумма активов в инвестиционных продуктах (ПИФ+ДУ+БО) на конец месяца в рублях  
COALESCE(inv_bo_agrmnt_open_qty, 0)            as inv_bo_agrmnt_open_qty, --Количество открытых договоров БО на конец месяца  
--ОД Пакет  
  
--Общ  
COALESCE(dep_acct_tot_bal_rub_amt, 0)          as dep_acct_tot_bal_rub_amt, --СДО (пассивы) / Суммарный баланс по всем пассивным счетам клиента на отчетную дату в рублях (без сбер серт)  
COALESCE(crd_pos_net_rub_amt, 0)               as crd_pos_net_rub_amt,      --POS-оборот /Чистая сумма POS-операций (за минусом возвратов)  
  
--Доп  
COALESCE(dep_acct_tot_davg_mnth_rub_amt, 0)    as dep_acct_tot_davg_mnth_rub_amt, --Суммарный среднедневной остаток (СДО) за отчетный месяц  
----Инф о человеке1  
COALESCE(sd_gender_cd, 'M')                    as sd_gender_cd, --Пол  
COALESCE(sd_age_yrs_comp_nv, 14)               as sd_age_yrs_comp_nv, -- Возраст клиента, полное  число лет  
COALESCE(crd_otf_cash_atm_qty, 0)              as crd_otf_cash_atm_qty, -- Количество снятий наличных в банкомате  
  
  
COALESCE(dep_inc_avg_risk_rub_amt, 0)          as dep_inc_avg_risk_rub_amt, -- Общий среднемесячный доход (зарплата + соц начисления) по методике Рисков  
COALESCE(dep_inf_income_rub_amt, 0)            as dep_inf_income_rub_amt,   -- Сумма поступлений на счет (зарплата, пенсии, больничные)  
			 sd_stlmnt_type_cd                 as sd_stlmnt_type_cd, -- Тип населенного пункта проживания клиента (по финальному адресу) - альтернативный алгоритм  
COALESCE(dep_payroll_client_nflag, 0)          as dep_payroll_client_nflag, --Флаг зарплатного клиента (среднемесячные з/п начисления за 3 мес хотя бы по одному из счетов > 3000 руб)  
COALESCE(crd_otf_cash_rub_amt, 0)              as crd_otf_cash_rub_amt,  --Общая сумма снятия наличных по всем картам клиента за месяц в рублях
  
COALESCE(srv_dc_p2p_otf_on_amt, 0)             as srv_dc_p2p_otf_on_amt,  --Сумма переводов другим физ лицам - клиентам банка по ДК за отчетный месяц (СБОЛ, МБ)  
COALESCE(srv_cc_p2p_otf_on_amt, 0)             as srv_cc_p2p_otf_on_amt,  --Сумма переводов другим физлицам - клиентам банка по КК за отчетный месяц (СБОЛ, МБ)
COALESCE(srv_dc_p2p_inf_on_amt, 0)             as srv_dc_p2p_inf_on_amt,  --Сумма переводов от других физлиц - клиентов банка по ДК за отчетный месяц (СБОЛ, МБ)
COALESCE(srv_cc_p2p_inf_on_amt, 0)             as srv_cc_p2p_inf_on_amt  --Сумма переводов от других физлиц - клиентов банка по КК за отчетный месяц (СБОЛ, МБ)

--Информация и люди	
from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021 as T_clients
inner join s_grnplm_ld_rozn_electron_daas_core_dm.ft_client_aggr_mnth_epk as T_inf
on T_clients.epk_id = T_inf.epk_id
	and T_inf.report_dt > '2020-01-31' --!!!правил 11 мес
	and T_inf.report_dt <= '2021-12-31' 
	and T_inf.report_dt != '2021-01-31'
--экваринг
left join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info_acquring as pnl_T_acquring
on      T_inf.epk_id    = pnl_T_acquring.epk_id 
	and T_inf.report_dt = pnl_T_acquring.report_dt
--Инвесты
left join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info_invest as pnl_T_invest
on      T_inf.epk_id    = pnl_T_invest.epk_id 
	and T_inf.report_dt = pnl_T_invest.report_dt
)