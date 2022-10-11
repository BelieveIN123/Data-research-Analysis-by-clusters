
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_packege
;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_packege
as (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info where 1=1 
																												and tp_active_kind_cd = 'PREMIER'
																												and report_dt >= '2021-01-31'
																												and report_dt <=  '2021-12-31' 
																												group by epk_id
																												having count(*) >= 6 --6 месяцев в премьер
																												) --!!!правил
;

drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_left_sbp
;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_left_sbp
as (select distinct epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info where 1=1 
																												and report_dt > '2021-01-31'
																												and seg_service_channel_cd = 'MASS'
																												)
;


--Добавляю последний населённый пункт.
--проверял у 600 человек поменялось метсо
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_last_nasp_type
;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_last_nasp_type
as (
with last_date_epk as 
	(
	select epk_id, min(report_dt) as report_dt 
	from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info
	where sd_stlmnt_type_cd is not null -- Берём ситуации только когда есть тип. Иначе 2к + не заполненных.
	group by epk_id --временная для учета ситуаций когда человека нет последней даты. Чтобы не стоял ноль подтягиваю по нему последнее место
					--уже не актуальна такая замороченность
	)
select distinct t1.epk_id, t1.sd_stlmnt_type_cd
from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info t1 
join last_date_epk t2
on t1.epk_id = t2.epk_id and t1.report_dt = t2.report_dt
)




;

drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg 
as ( 
select 
	
	
 		t1.epk_id, --id клиента
 		extract(year from t1.report_dt) as year_ , --год
		--seg_service_channel_cd, --сервисный канал --!!!правил
		--prd_client_active_nflag, --Активен ли клиент
		case
			when 1=1 
				and t1.epk_id in (select * from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_packege)
				and t1.epk_id not in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_left_sbp)
			then 'pachege_sbp'
			when t1.epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_left_sbp)
			then 'left_sbp'
			else 'sbp'
			end as Split_1,
		AVG(pnl_oi_total_amt)                as pnl_oi_total_amt, -- Сумма общего операционного дохода (ОД) по клиенту за отчетный месяц
		
		--Вклады
		AVG(pnl_oi_td_amt)                   as pnl_oi_td_amt, -- //Сумма операционного дохода (ОД) по клиенту за отчетный месяц по срочным счетам
		AVG(pnl_oi_ca_amt)                   as pnl_oi_ca_amt, --//Сумма операционного дохода (ОД) по клиенту за отчетный месяц по текущим счетам
		AVG(dep_acct_dep_td_qty)             as dep_acct_dep_td_qty, --СРОЧН Кол-во действующих  счетов срочных вкладов клиента на отчетную дату
		AVG(dep_acct_dep_ca_qty)             as dep_acct_dep_ca_qty, --ТЕКУЩ Кол-во действующих текущих счетов клиента на отчетную дату
		AVG(dep_acct_dep_tot_bal_rub_amt)    as dep_acct_dep_tot_bal_rub_amt, -- СДО по вкладам / Суммарный баланс по депозитным (текущим и срочным) счетам клиента на отчетную дату в рублях
		--Пк
		AVG(pnl_oi_pl_amt)                   as pnl_oi_pl_amt, -- ОД ПК      / Сумма операционного дохода (ОД) по клиенту за отчетный месяц по потребительским кредитам
		AVG(lne_pl_debt_os_rub_amt)          as lne_pl_debt_os_rub_amt,   --ПК сростатки
		AVG(prd_pl_active_qty)               as prd_pl_active_qty, --ПК Количество активных потребительских кредитов за 3 мес.
		avg(lne_overdraft_overdue_nflag)     as lne_overdraft_overdue_nflag, --Наличие просрочки
		
		--ЖК
		AVG(pnl_oi_ml_amt)                   as pnl_oi_ml_amt, -- ОД по ЖК /Сумма операционного дохода (ОД) по клиенту за отчетный месяц по жилищным (ипотечным) кредитам
		AVG(lne_mg_debt_os_rub_amt)          as lne_mg_debt_os_rub_amt,   --ЖК / Суммарный остаток основного долга (срочная часть + просрочка) - ипотека
		AVG(prd_mg_active_qty)               as prd_mg_active_qty, --ЖК Количество активных ипотек за 3 мес.
		--КК
		AVG(pnl_oi_cc_amt)                   as pnl_oi_cc_amt, -- ОД по КК / Сумма операционного дохода (ОД) по клиенту за отчетный месяц по кредитным картам
		AVG(crd_cc_tot_credit_rub_amt)       as crd_cc_tot_credit_rub_amt,--КК / Сумма полной задолженности по КК (основной долг + % + просрочка + штрафы) на отчетную дату, руб.
		AVG(crd_cc_active_open_qty)          as crd_cc_active_open_qty, --КК / Количество активных открытых кредитных карт
		avg(crd_cc_open_qty)         as crd_cc_open_qty,
		avg(crd_cc_act_spend_qty)    as crd_cc_act_spend_qty,
		
		--ДК
		AVG(pnl_oi_dc_amt)                   as pnl_oi_dc_amt, -- ОД по картам **дебет/ Сумма операционного дохода (ОД) по клиенту за отчетный месяц по дебетовым картам
		AVG(crd_dc_act_spend_qty)            as crd_dc_act_spend_qty, --ДЕБ. Кол-во активных по расходным операциям дебетовых карт
		--
		avg(crd_dc_open_qty) 				as crd_dc_open_qty,
		
		--ЭКВ
		AVG(SUM_pnl_T_acquring)              as SUM_pnl_T_acquring, -- ОД экваринг
		
		AVG(crd_pos_auto_3m_qty) as crd_pos_auto_3m_qty, --Количество операций покупки в категории "Авто/мото" за месяц по картам за 3 мес.
		AVG(crd_pos_money_trf_3m_qty) as crd_pos_money_trf_3m_qty, --Количество операций покупки в категории "Безналичные денежные операции" за месяц по картам за 3 мес.
		AVG(crd_pos_eat_out_3m_qty) as crd_pos_eat_out_3m_qty, --Количество операций покупки в категории "Кафе/бары/рестораны" за месяц по картам за 3 мес.
		AVG(crd_pos_utilities_3m_qty) as crd_pos_utilities_3m_qty, --Количество операций покупки в категории "Коммунальные платежи" за месяц по картам за 3 мес.
		AVG(crd_pos_pc_it_3m_qty) as crd_pos_pc_it_3m_qty, --Количество операций покупки в категории "Компьютеры/Гаджеты/ПО/IT-услуги" за месяц по картам за 3 мес.
		AVG(crd_pos_beuaty_3m_qty) as crd_pos_beuaty_3m_qty, --Количество операций покупки в категории "Красота и здоровье" за месяц по картам за 3 мес.
		AVG(crd_pos_healthcare_3m_qty) as crd_pos_healthcare_3m_qty, --Количество операций покупки в категории "Медицина" за месяц по картам за 3 мес.
		AVG(crd_pos_clothes_3m_qty) as crd_pos_clothes_3m_qty, --Количество операций покупки в категории "Одежда, обувь и аксессуары" за месяц по картам за 3 мес.
		AVG(crd_pos_leisure_3m_qty) as crd_pos_leisure_3m_qty, --Количество операций покупки в категории "Отдых/развлечения/спорт" за месяц по картам за 3 мес.
		AVG(crd_pos_groceries_3m_qty) as crd_pos_groceries_3m_qty, --Количество операций покупки в категории "Продукты" за месяц по картам за 3 мес.
		AVG(crd_pos_othr_3m_qty) as crd_pos_othr_3m_qty, --Количество операций покупки в категории "Прочее" за месяц по картам за 3 мес.
		AVG(crd_pos_special_3m_qty) as crd_pos_special_3m_qty, --Количество операций покупки в категории "Специализированные товары и услуги" за месяц по картам за 3 мес.
		AVG(crd_pos_telecom_3m_qty) as crd_pos_telecom_3m_qty, --Количество операций покупки в категории "Телекоммуникационные услуги" за месяц по картам за 3 мес.
		AVG(crd_pos_home_repair_3m_qty) as crd_pos_home_repair_3m_qty, --Количество операций покупки в категории "Товары для дома/ремонта" за месяц по картам за 3 мес.
		AVG(crd_pos_tourism_3m_qty) as crd_pos_tourism_3m_qty, --Количество операций покупки в категории "Туризм/транспорт" за месяц по картам за 3 мес.
		AVG(crd_pos_dept_stores_3m_qty) as crd_pos_dept_stores_3m_qty, --Количество операций покупки в категории "Универсальные магазины" за месяц по картам за 3 мес.
		AVG(crd_pos_auto_rub_3m_amt) as crd_pos_auto_rub_3m_amt, --Сумма операций покупки в категории "Авто/мото" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_money_trf_rub_3m_amt) as crd_pos_money_trf_rub_3m_amt, --Сумма операций покупки в категории "Безналичные денежные операции" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_eat_out_rub_3m_amt) as crd_pos_eat_out_rub_3m_amt, --Сумма операций покупки в категории "Кафе/бары/рестораны" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_utilities_rub_3m_amt) as crd_pos_utilities_rub_3m_amt, --Сумма операций покупки в категории "Коммунальные платежи" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_pc_it_rub_3m_amt) as crd_pos_pc_it_rub_3m_amt, --Сумма операций покупки в категории "Компьютеры/Гаджеты/ПО/IT-услуги" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_beuaty_rub_3m_amt) as crd_pos_beuaty_rub_3m_amt, --Сумма операций покупки в категории "Красота и здоровье" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_healthcare_rub_3m_amt) as crd_pos_healthcare_rub_3m_amt, --Сумма операций покупки в категории "Медицина" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_clothes_rub_3m_amt) as crd_pos_clothes_rub_3m_amt, --Сумма операций покупки в категории "Одежда, обувь и аксессуары" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_leisure_rub_3m_amt) as crd_pos_leisure_rub_3m_amt, --Сумма операций покупки в категории "Отдых/развлечения/спорт" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_groceries_rub_3m_amt) as crd_pos_groceries_rub_3m_amt, --Сумма операций покупки в категории "Продукты" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_othr_3m_amt) as crd_pos_othr_3m_amt, --Сумма операций покупки в категории "Прочее" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_special_rub_3m_amt) as crd_pos_special_rub_3m_amt, --Сумма операций покупки в категории "Специализированные товары и услуги" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_telecom_rub_3m_amt) as crd_pos_telecom_rub_3m_amt, --Сумма операций покупки в категории "Телекоммуникационные услуги" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_home_repair_rub_3m_amt) as crd_pos_home_repair_rub_3m_amt, --Сумма операций покупки в категории "Товары для дома/ремонта" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_tourism_rub_3m_amt) as crd_pos_tourism_rub_3m_amt, --Сумма операций покупки в категории "Туризм/транспорт" в рублях за месяц по картам за 3 мес.
		AVG(crd_pos_dept_stores_rub_3m_amt) as crd_pos_dept_stores_rub_3m_amt, --Сумма операций покупки в категории "Универсальные магазины" в рублях за месяц по картам за 3 мес.

		
		--Проч
		AVG(pnl_oi_othr_amt)                 as pnl_oi_othr_amt, -- ОД по прочим / ...
		--ОД Инвест
		AVG(SUM_pnl_T_invest)                as SUM_pnl_T_invest, --доход по инвест 
		AVG(inv_tot_bal_rub_amt)             as inv_tot_bal_rub_amt, --Инвесты / Общая сумма активов в инвестиционных продуктах (ПИФ+ДУ+БО) на конец месяца в рублях
		AVG(inv_bo_agrmnt_open_qty)          as inv_bo_agrmnt_open_qty, --Количество открытых договоров БО на конец месяца
		--ОД Пакет
		
		--Общ
		AVG(dep_acct_tot_bal_rub_amt)        as dep_acct_tot_bal_rub_amt, --СДО (пассивы) / Суммарный баланс по всем пассивным счетам клиента на отчетную дату в рублях (без сбер.серт)
		AVG(crd_pos_net_rub_amt)             as crd_pos_net_rub_amt,      --POS-оборот /Чистая сумма POS-операций (за минусом возвратов)
		
		--Доп
		AVG(dep_acct_tot_davg_mnth_rub_amt)  as dep_acct_tot_davg_mnth_rub_amt, --Суммарный среднедневной остаток (СДО) за отчетный месяц
		----Инф о человеке1
		max(sd_gender_cd)                    as sd_gender_cd, --Пол
		max(sd_age_yrs_comp_nv)              as sd_age_yrs_comp_nv, -- Возраст клиента, полное  число лет
		max(t2.sd_stlmnt_type_cd)            as sd_stlmnt_type_cd, -- Тип населенного пункта проживания клиента (по финальному адресу) - альтернативный алгоритм
		
		AVG(crd_otf_cash_atm_qty)            as crd_otf_cash_atm_qty, -- Количество снятий наличных в банкомате
		
		
		
		AVG(dep_inc_avg_risk_rub_amt)        as dep_inc_avg_risk_rub_amt, -- Общий среднемесячный доход (зарплата + соц.начисления) по методике Рисков
		AVG(dep_inf_income_rub_amt)          as dep_inf_income_rub_amt,   -- Сумма поступлений на счет (зарплата, пенсии, больничные)
		AVG(dep_payroll_client_nflag)        as dep_payroll_client_nflag, -- Флаг зарплатного клиента (среднемесячные з/п начисления за 3 мес. хотя бы по одному из счетов > 3000 руб.)
		AVG(crd_otf_cash_rub_amt)            as crd_otf_cash_rub_amt, --Общая сумма снятия наличных по всем картам клиента за месяц в рублях
		
		
		AVG(srv_dc_p2p_otf_on_amt)           as srv_dc_p2p_otf_on_amt,  --Сумма переводов другим физ.лицам - клиентам банка по ДК за отчетный месяц (СБОЛ, МБ)
		AVG(srv_cc_p2p_otf_on_amt)           as srv_cc_p2p_otf_on_amt, --Сумма переводов другим физ.лицам - клиентам банка по КК за отчетный месяц (СБОЛ, МБ)
		AVG(srv_dc_p2p_inf_on_amt)           as srv_dc_p2p_inf_on_amt, --Сумма переводов от других физ.лиц - клиентов банка по ДК за отчетный месяц (СБОЛ, МБ)
		AVG(srv_cc_p2p_inf_on_amt)           as srv_cc_p2p_inf_on_amt --Сумма переводов от других физ.лиц - клиентов банка по КК за отчетный месяц (СБОЛ, МБ)


from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info t1
left join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_last_nasp_type t2
on t1.epk_id = t2.epk_id

group by t1.epk_id, extract(year from t1.report_dt)
order by t1.epk_id, year_
);


--Михаил сказал что это отдельный класс
--Предпологаю что его не нужно закадывать в отдельный столбец.
--Пусть он будет "выводимым" т.е. процент можно буедт расчитывать из процента других типов. 

--update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set sd_stlmnt_type_cd ='CITY_OTHER' where sd_stlmnt_type_cd is null
--Заполняю самой популярной группой




---- Добавляю разделённый столбце по типу месности. 
alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
add type_CITY_MLNR int 
default 0;

alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
add type_VILLAGE int
default 0;

alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
add type_CITY_OTHER int
default 0;

------Заполняю новые столбцы
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set type_CITY_MLNR = 1 where sd_stlmnt_type_cd ='CITY_MLNR';
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set type_VILLAGE = 1   where sd_stlmnt_type_cd ='VILLAGE';
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set type_CITY_OTHER = 1 where sd_stlmnt_type_cd ='CITY_OTHER';

-----проверка
-----select sum(type_CITY_MLNR)+sum(type_VILLAGE)+ sum(type_CITY_OTHER) from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg

--Пол
--Обновил на 1 и 0
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set sd_gender_cd = 1 where sd_gender_cd = 'M';
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set sd_gender_cd = 0 where sd_gender_cd = 'F';

alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg alter column sd_gender_cd type integer using sd_gender_cd::integer;
-----проверка
-----select sum(sd_gender_cd) from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg


--Суммирую ВКЛАДЫ
--alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg drop column dep_acct_dep_qty_sum;

alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
add pnl_oi_amt_sum int default 0;

alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
add dep_acct_dep_qty_sum int default 0;

update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set pnl_oi_amt_sum = pnl_oi_td_amt + pnl_oi_ca_amt;
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set dep_acct_dep_qty_sum = dep_acct_dep_td_qty + dep_acct_dep_ca_qty;


--удалю выбросы
--191
delete from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg where epk_id in (
select distinct epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
where pnl_oi_total_amt < -10000
);
--248 - 191
delete from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg where epk_id in (
select distinct epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
where pnl_oi_total_amt > 50000
);




