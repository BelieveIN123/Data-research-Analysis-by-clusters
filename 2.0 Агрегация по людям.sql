
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_packege
;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_packege
as (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info where 1=1 
																												and tp_active_kind_cd = 'PREMIER'
																												and report_dt >= '2021-01-31'
																												and report_dt <=  '2021-12-31' 
																												group by epk_id
																												having count(*) >= 6 --6 ������� � �������
																												) --!!!������
;

drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_left_sbp
;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_left_sbp
as (select distinct epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info where 1=1 
																												and report_dt > '2021-01-31'
																												and seg_service_channel_cd = 'MASS'
																												)
;


--�������� ��������� ��������� �����.
--�������� � 600 ������� ���������� �����
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_last_nasp_type
;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_last_nasp_type
as (
with last_date_epk as 
	(
	select epk_id, min(report_dt) as report_dt 
	from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info
	where sd_stlmnt_type_cd is not null -- ���� �������� ������ ����� ���� ���. ����� 2� + �� �����������.
	group by epk_id --��������� ��� ����� �������� ����� �������� ��� ��������� ����. ����� �� ����� ���� ���������� �� ���� ��������� �����
					--��� �� ��������� ����� ��������������
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
	
	
 		t1.epk_id, --id �������
 		extract(year from t1.report_dt) as year_ , --���
		--seg_service_channel_cd, --��������� ����� --!!!������
		--prd_client_active_nflag, --������� �� ������
		case
			when 1=1 
				and t1.epk_id in (select * from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_packege)
				and t1.epk_id not in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_left_sbp)
			then 'pachege_sbp'
			when t1.epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_epk_left_sbp)
			then 'left_sbp'
			else 'sbp'
			end as Split_1,
		AVG(pnl_oi_total_amt)                as pnl_oi_total_amt, -- ����� ������ ������������� ������ (��) �� ������� �� �������� �����
		
		--������
		AVG(pnl_oi_td_amt)                   as pnl_oi_td_amt, -- //����� ������������� ������ (��) �� ������� �� �������� ����� �� ������� ������
		AVG(pnl_oi_ca_amt)                   as pnl_oi_ca_amt, --//����� ������������� ������ (��) �� ������� �� �������� ����� �� ������� ������
		AVG(dep_acct_dep_td_qty)             as dep_acct_dep_td_qty, --����� ���-�� �����������  ������ ������� ������� ������� �� �������� ����
		AVG(dep_acct_dep_ca_qty)             as dep_acct_dep_ca_qty, --����� ���-�� ����������� ������� ������ ������� �� �������� ����
		AVG(dep_acct_dep_tot_bal_rub_amt)    as dep_acct_dep_tot_bal_rub_amt, -- ��� �� ������� / ��������� ������ �� ���������� (������� � �������) ������ ������� �� �������� ���� � ������
		--��
		AVG(pnl_oi_pl_amt)                   as pnl_oi_pl_amt, -- �� ��      / ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������������� ��������
		AVG(lne_pl_debt_os_rub_amt)          as lne_pl_debt_os_rub_amt,   --�� ���������
		AVG(prd_pl_active_qty)               as prd_pl_active_qty, --�� ���������� �������� ��������������� �������� �� 3 ���.
		avg(lne_overdraft_overdue_nflag)     as lne_overdraft_overdue_nflag, --������� ���������
		
		--��
		AVG(pnl_oi_ml_amt)                   as pnl_oi_ml_amt, -- �� �� �� /����� ������������� ������ (��) �� ������� �� �������� ����� �� �������� (���������) ��������
		AVG(lne_mg_debt_os_rub_amt)          as lne_mg_debt_os_rub_amt,   --�� / ��������� ������� ��������� ����� (������� ����� + ���������) - �������
		AVG(prd_mg_active_qty)               as prd_mg_active_qty, --�� ���������� �������� ������ �� 3 ���.
		--��
		AVG(pnl_oi_cc_amt)                   as pnl_oi_cc_amt, -- �� �� �� / ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������� ������
		AVG(crd_cc_tot_credit_rub_amt)       as crd_cc_tot_credit_rub_amt,--�� / ����� ������ ������������� �� �� (�������� ���� + % + ��������� + ������) �� �������� ����, ���.
		AVG(crd_cc_active_open_qty)          as crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����
		avg(crd_cc_open_qty)         as crd_cc_open_qty,
		avg(crd_cc_act_spend_qty)    as crd_cc_act_spend_qty,
		
		--��
		AVG(pnl_oi_dc_amt)                   as pnl_oi_dc_amt, -- �� �� ������ **�����/ ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������� ������
		AVG(crd_dc_act_spend_qty)            as crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����
		--
		avg(crd_dc_open_qty) 				as crd_dc_open_qty,
		
		--���
		AVG(SUM_pnl_T_acquring)              as SUM_pnl_T_acquring, -- �� ��������
		
		AVG(crd_pos_auto_3m_qty) as crd_pos_auto_3m_qty, --���������� �������� ������� � ��������� "����/����" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_money_trf_3m_qty) as crd_pos_money_trf_3m_qty, --���������� �������� ������� � ��������� "����������� �������� ��������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_eat_out_3m_qty) as crd_pos_eat_out_3m_qty, --���������� �������� ������� � ��������� "����/����/���������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_utilities_3m_qty) as crd_pos_utilities_3m_qty, --���������� �������� ������� � ��������� "������������ �������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_pc_it_3m_qty) as crd_pos_pc_it_3m_qty, --���������� �������� ������� � ��������� "����������/�������/��/IT-������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_beuaty_3m_qty) as crd_pos_beuaty_3m_qty, --���������� �������� ������� � ��������� "������� � ��������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_healthcare_3m_qty) as crd_pos_healthcare_3m_qty, --���������� �������� ������� � ��������� "��������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_clothes_3m_qty) as crd_pos_clothes_3m_qty, --���������� �������� ������� � ��������� "������, ����� � ����������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_leisure_3m_qty) as crd_pos_leisure_3m_qty, --���������� �������� ������� � ��������� "�����/�����������/�����" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_groceries_3m_qty) as crd_pos_groceries_3m_qty, --���������� �������� ������� � ��������� "��������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_othr_3m_qty) as crd_pos_othr_3m_qty, --���������� �������� ������� � ��������� "������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_special_3m_qty) as crd_pos_special_3m_qty, --���������� �������� ������� � ��������� "������������������ ������ � ������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_telecom_3m_qty) as crd_pos_telecom_3m_qty, --���������� �������� ������� � ��������� "�������������������� ������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_home_repair_3m_qty) as crd_pos_home_repair_3m_qty, --���������� �������� ������� � ��������� "������ ��� ����/�������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_tourism_3m_qty) as crd_pos_tourism_3m_qty, --���������� �������� ������� � ��������� "������/���������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_dept_stores_3m_qty) as crd_pos_dept_stores_3m_qty, --���������� �������� ������� � ��������� "������������� ��������" �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_auto_rub_3m_amt) as crd_pos_auto_rub_3m_amt, --����� �������� ������� � ��������� "����/����" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_money_trf_rub_3m_amt) as crd_pos_money_trf_rub_3m_amt, --����� �������� ������� � ��������� "����������� �������� ��������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_eat_out_rub_3m_amt) as crd_pos_eat_out_rub_3m_amt, --����� �������� ������� � ��������� "����/����/���������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_utilities_rub_3m_amt) as crd_pos_utilities_rub_3m_amt, --����� �������� ������� � ��������� "������������ �������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_pc_it_rub_3m_amt) as crd_pos_pc_it_rub_3m_amt, --����� �������� ������� � ��������� "����������/�������/��/IT-������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_beuaty_rub_3m_amt) as crd_pos_beuaty_rub_3m_amt, --����� �������� ������� � ��������� "������� � ��������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_healthcare_rub_3m_amt) as crd_pos_healthcare_rub_3m_amt, --����� �������� ������� � ��������� "��������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_clothes_rub_3m_amt) as crd_pos_clothes_rub_3m_amt, --����� �������� ������� � ��������� "������, ����� � ����������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_leisure_rub_3m_amt) as crd_pos_leisure_rub_3m_amt, --����� �������� ������� � ��������� "�����/�����������/�����" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_groceries_rub_3m_amt) as crd_pos_groceries_rub_3m_amt, --����� �������� ������� � ��������� "��������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_othr_3m_amt) as crd_pos_othr_3m_amt, --����� �������� ������� � ��������� "������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_special_rub_3m_amt) as crd_pos_special_rub_3m_amt, --����� �������� ������� � ��������� "������������������ ������ � ������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_telecom_rub_3m_amt) as crd_pos_telecom_rub_3m_amt, --����� �������� ������� � ��������� "�������������������� ������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_home_repair_rub_3m_amt) as crd_pos_home_repair_rub_3m_amt, --����� �������� ������� � ��������� "������ ��� ����/�������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_tourism_rub_3m_amt) as crd_pos_tourism_rub_3m_amt, --����� �������� ������� � ��������� "������/���������" � ������ �� ����� �� ������ �� 3 ���.
		AVG(crd_pos_dept_stores_rub_3m_amt) as crd_pos_dept_stores_rub_3m_amt, --����� �������� ������� � ��������� "������������� ��������" � ������ �� ����� �� ������ �� 3 ���.

		
		--����
		AVG(pnl_oi_othr_amt)                 as pnl_oi_othr_amt, -- �� �� ������ / ...
		--�� ������
		AVG(SUM_pnl_T_invest)                as SUM_pnl_T_invest, --����� �� ������ 
		AVG(inv_tot_bal_rub_amt)             as inv_tot_bal_rub_amt, --������� / ����� ����� ������� � �������������� ��������� (���+��+��) �� ����� ������ � ������
		AVG(inv_bo_agrmnt_open_qty)          as inv_bo_agrmnt_open_qty, --���������� �������� ��������� �� �� ����� ������
		--�� �����
		
		--���
		AVG(dep_acct_tot_bal_rub_amt)        as dep_acct_tot_bal_rub_amt, --��� (�������) / ��������� ������ �� ���� ��������� ������ ������� �� �������� ���� � ������ (��� ����.����)
		AVG(crd_pos_net_rub_amt)             as crd_pos_net_rub_amt,      --POS-������ /������ ����� POS-�������� (�� ������� ���������)
		
		--���
		AVG(dep_acct_tot_davg_mnth_rub_amt)  as dep_acct_tot_davg_mnth_rub_amt, --��������� ������������� ������� (���) �� �������� �����
		----��� � ��������1
		max(sd_gender_cd)                    as sd_gender_cd, --���
		max(sd_age_yrs_comp_nv)              as sd_age_yrs_comp_nv, -- ������� �������, ������  ����� ���
		max(t2.sd_stlmnt_type_cd)            as sd_stlmnt_type_cd, -- ��� ����������� ������ ���������� ������� (�� ���������� ������) - �������������� ��������
		
		AVG(crd_otf_cash_atm_qty)            as crd_otf_cash_atm_qty, -- ���������� ������ �������� � ���������
		
		
		
		AVG(dep_inc_avg_risk_rub_amt)        as dep_inc_avg_risk_rub_amt, -- ����� �������������� ����� (�������� + ���.����������) �� �������� ������
		AVG(dep_inf_income_rub_amt)          as dep_inf_income_rub_amt,   -- ����� ����������� �� ���� (��������, ������, ����������)
		AVG(dep_payroll_client_nflag)        as dep_payroll_client_nflag, -- ���� ����������� ������� (�������������� �/� ���������� �� 3 ���. ���� �� �� ������ �� ������ > 3000 ���.)
		AVG(crd_otf_cash_rub_amt)            as crd_otf_cash_rub_amt, --����� ����� ������ �������� �� ���� ������ ������� �� ����� � ������
		
		
		AVG(srv_dc_p2p_otf_on_amt)           as srv_dc_p2p_otf_on_amt,  --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
		AVG(srv_cc_p2p_otf_on_amt)           as srv_cc_p2p_otf_on_amt, --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
		AVG(srv_dc_p2p_inf_on_amt)           as srv_dc_p2p_inf_on_amt, --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)
		AVG(srv_cc_p2p_inf_on_amt)           as srv_cc_p2p_inf_on_amt --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)


from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info t1
left join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_last_nasp_type t2
on t1.epk_id = t2.epk_id

group by t1.epk_id, extract(year from t1.report_dt)
order by t1.epk_id, year_
);


--������ ������ ��� ��� ��������� �����
--����������� ��� ��� �� ����� ���������� � ��������� �������.
--����� �� ����� "���������" �.�. ������� ����� ����� ����������� �� �������� ������ �����. 

--update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set sd_stlmnt_type_cd ='CITY_OTHER' where sd_stlmnt_type_cd is null
--�������� ����� ���������� �������




---- �������� ���������� ������� �� ���� ��������. 
alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
add type_CITY_MLNR int 
default 0;

alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
add type_VILLAGE int
default 0;

alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
add type_CITY_OTHER int
default 0;

------�������� ����� �������
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set type_CITY_MLNR = 1 where sd_stlmnt_type_cd ='CITY_MLNR';
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set type_VILLAGE = 1   where sd_stlmnt_type_cd ='VILLAGE';
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set type_CITY_OTHER = 1 where sd_stlmnt_type_cd ='CITY_OTHER';

-----��������
-----select sum(type_CITY_MLNR)+sum(type_VILLAGE)+ sum(type_CITY_OTHER) from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg

--���
--������� �� 1 � 0
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set sd_gender_cd = 1 where sd_gender_cd = 'M';
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set sd_gender_cd = 0 where sd_gender_cd = 'F';

alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg alter column sd_gender_cd type integer using sd_gender_cd::integer;
-----��������
-----select sum(sd_gender_cd) from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg


--�������� ������
--alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg drop column dep_acct_dep_qty_sum;

alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
add pnl_oi_amt_sum int default 0;

alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg
add dep_acct_dep_qty_sum int default 0;

update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set pnl_oi_amt_sum = pnl_oi_td_amt + pnl_oi_ca_amt;
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg set dep_acct_dep_qty_sum = dep_acct_dep_td_qty + dep_acct_dep_ca_qty;


--����� �������
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




