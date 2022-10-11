
set optimizer = on;
--------------------������ ������ �����
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021
;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021 as (
with
client_mass as (
select epk_id, seg_service_channel_cd from s_grnplm_ld_rozn_electron_daas_core_dm.ft_client_aggr_mnth_epk
where 1 = 1
and seg_service_channel_cd in ('MASS')
and report_dt = '2020-12-31' --���� �����
and prd_1st_prod_open_dt <= '2020-07-01' --���� ������� ��������
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
	and client_sbp.epk_id not in (select epk_id from s_grnplm_ld_rozn_electron_daas_core_dm.ft_client_aggr_mnth_epk where sd_dead_nflag = 1 and report_dt = '2021-12-31') --���� ������� --!!!������
);



create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_need_drop as (select T_inf.epk_id
									from s_grnplm_ld_rozn_electron_daas_core_dm.ft_client_aggr_mnth_epk as T_inf
									join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021 t_cl on T_inf.epk_id =t_cl.epk_id
									where 1=1
										and T_inf.report_dt >  '2020-01-01'
										and T_inf.report_dt <  '2022-01-01' 
									group by T_inf.epk_id
									having count(*) < 24) --�������� �� ������� 24 ���. � ���������� ������, ���� ����� 22
;
delete from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021 t1 where epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_need_drop);
drop table s_grnplm_ld_rozn_electron_hq_core_dm.aka_need_drop;
--���������� �������� ����� ��� 24 ��� �� 2 ���� (35 ���. ����� ����� �������) --!!!������ (�������)

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
							where pl_product_l2_short_name like '%���������%'	
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
	and report_dt <= '2022-02-01' --25 ������� ��� ������ ��������
	and report_dt >= '2020-01-01'
	and pl_product_id in (
							select distinct pl_product_id from s_grnplm_ld_rozn_electron_daas_core_dm.ref_pl_product rpp 
							where pl_product_l5_full_name like '%������%'
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

		 T_inf.epk_id 	                      as epk_id, --id �������   
	      T_inf.seg_service_channel_cd        as seg_service_channel_cd, --��������� �����  
		 T_inf.report_dt                      as report_dt, --����  
COALESCE(prd_client_active_nflag, 0)           as prd_client_active_nflag, --������� �� ������  
 		tp_active_kind_cd	                   as tp_active_kind_cd, --�������� ��������� ������ �����  
COALESCE(tp_active_nflag, 1)                   as tp_active_nflag, --������������� ���������� �������  
  
COALESCE(pnl_oi_total_amt, 0)                  as pnl_oi_total_amt, -- ����� ������ ������������� ������ (��) �� ������� �� �������� �����  
  
--������  
COALESCE(pnl_oi_td_amt, 0)                     as pnl_oi_td_amt, -- //����� ������������� ������ (��) �� ������� �� �������� ����� �� ������� ������  
COALESCE(pnl_oi_ca_amt, 0)                     as pnl_oi_ca_amt, --//����� ������������� ������ (��) �� ������� �� �������� ����� �� ������� ������  
COALESCE(dep_acct_dep_td_qty, 0)               as dep_acct_dep_td_qty, --����� ���-�� �����������  ������ ������� ������� ������� �� �������� ����  
COALESCE(dep_acct_dep_ca_qty, 0)               as dep_acct_dep_ca_qty, --����� ���-�� ����������� ������� ������ ������� �� �������� ����  
COALESCE(dep_acct_dep_tot_bal_rub_amt, 0)      as dep_acct_dep_tot_bal_rub_amt, -- ��� �� ������� / ��������� ������ �� ���������� (������� � �������) ������ ������� �� �������� ���� � ������
--dep_acct_dep_td_bal_rub_amt �������� ������ �� ���� ������   
--��  
COALESCE(pnl_oi_pl_amt, 0)                     as pnl_oi_pl_amt, -- �� ��      / ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������������� ��������  
COALESCE(lne_pl_debt_os_rub_amt, 0)            as lne_pl_debt_os_rub_amt,   --�� �� /  ��������� ������� ��������� ����� (������� ����� + ���������) - ��������������� ������� (PL)
COALESCE(prd_pl_active_qty, 0)                 as prd_pl_active_qty,  --�� ���������� �������� ��������������� �������� �� 3 ���.
COALESCE(lne_overdraft_overdue_nflag, 0)       as lne_overdraft_overdue_nflag, --������� ���������
  
--��  
COALESCE(pnl_oi_ml_amt, 0)                     as pnl_oi_ml_amt, -- �� �� �� /����� ������������� ������ (��) �� ������� �� �������� ����� �� �������� (���������) ��������  
COALESCE(lne_mg_debt_os_rub_amt, 0)            as lne_mg_debt_os_rub_amt,   --�� / ��������� ������� ��������� ����� (������� ����� + ���������) - �������  
COALESCE(prd_mg_active_qty, 0)                 as prd_mg_active_qty, --�� ���������� �������� ������ �� 3 ���.  
--��  
COALESCE(pnl_oi_cc_amt, 0)                     as pnl_oi_cc_amt, -- �� �� �� / ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������� ������  
COALESCE(crd_cc_tot_credit_rub_amt, 0)         as crd_cc_tot_credit_rub_amt,--�� / ����� ������ ������������� �� �� (�������� ���� + % + ��������� + ������) �� �������� ����, ���
COALESCE(crd_cc_active_open_qty, 0)            as crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����  
--
COALESCE(crd_cc_open_qty, 0)                   as crd_cc_open_qty,
COALESCE(crd_cc_act_spend_qty, 0)              as crd_cc_act_spend_qty,
--
  
--��  
COALESCE(pnl_oi_dc_amt, 0)                     as pnl_oi_dc_amt, -- �� �� ������ **�����/ ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������� ������  
COALESCE(crd_dc_act_spend_qty, 0)              as crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ���� 
--
COALESCE(crd_dc_open_qty, 0)				   as crd_dc_open_qty,
--  
  
--���  
COALESCE(pnl_T_acquring.SUM_pnl_T_acquring, 0) as SUM_pnl_T_acquring, -- �� ��������  

COALESCE(crd_pos_auto_3m_qty, 0) as crd_pos_auto_3m_qty, --���������� �������� ������� � ��������� "����/����" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_money_trf_3m_qty, 0) as crd_pos_money_trf_3m_qty, --���������� �������� ������� � ��������� "����������� �������� ��������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_eat_out_3m_qty, 0) as crd_pos_eat_out_3m_qty, --���������� �������� ������� � ��������� "����/����/���������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_utilities_3m_qty, 0) as crd_pos_utilities_3m_qty, --���������� �������� ������� � ��������� "������������ �������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_pc_it_3m_qty, 0) as crd_pos_pc_it_3m_qty, --���������� �������� ������� � ��������� "����������/�������/��/IT-������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_beuaty_3m_qty, 0) as crd_pos_beuaty_3m_qty, --���������� �������� ������� � ��������� "������� � ��������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_healthcare_3m_qty, 0) as crd_pos_healthcare_3m_qty, --���������� �������� ������� � ��������� "��������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_clothes_3m_qty, 0) as crd_pos_clothes_3m_qty, --���������� �������� ������� � ��������� "������, ����� � ����������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_leisure_3m_qty, 0) as crd_pos_leisure_3m_qty, --���������� �������� ������� � ��������� "�����/�����������/�����" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_groceries_3m_qty, 0) as crd_pos_groceries_3m_qty, --���������� �������� ������� � ��������� "��������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_othr_3m_qty, 0) as crd_pos_othr_3m_qty, --���������� �������� ������� � ��������� "������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_special_3m_qty, 0) as crd_pos_special_3m_qty, --���������� �������� ������� � ��������� "������������������ ������ � ������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_telecom_3m_qty, 0) as crd_pos_telecom_3m_qty, --���������� �������� ������� � ��������� "�������������������� ������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_home_repair_3m_qty, 0) as crd_pos_home_repair_3m_qty, --���������� �������� ������� � ��������� "������ ��� ����/�������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_tourism_3m_qty, 0) as crd_pos_tourism_3m_qty, --���������� �������� ������� � ��������� "������/���������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_dept_stores_3m_qty, 0) as crd_pos_dept_stores_3m_qty, --���������� �������� ������� � ��������� "������������� ��������" �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_auto_rub_3m_amt, 0) as crd_pos_auto_rub_3m_amt, --����� �������� ������� � ��������� "����/����" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_money_trf_rub_3m_amt, 0) as crd_pos_money_trf_rub_3m_amt, --����� �������� ������� � ��������� "����������� �������� ��������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_eat_out_rub_3m_amt, 0) as crd_pos_eat_out_rub_3m_amt, --����� �������� ������� � ��������� "����/����/���������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_utilities_rub_3m_amt, 0) as crd_pos_utilities_rub_3m_amt, --����� �������� ������� � ��������� "������������ �������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_pc_it_rub_3m_amt, 0) as crd_pos_pc_it_rub_3m_amt, --����� �������� ������� � ��������� "����������/�������/��/IT-������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_beuaty_rub_3m_amt, 0) as crd_pos_beuaty_rub_3m_amt, --����� �������� ������� � ��������� "������� � ��������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_healthcare_rub_3m_amt, 0) as crd_pos_healthcare_rub_3m_amt, --����� �������� ������� � ��������� "��������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_clothes_rub_3m_amt, 0) as crd_pos_clothes_rub_3m_amt, --����� �������� ������� � ��������� "������, ����� � ����������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_leisure_rub_3m_amt, 0) as crd_pos_leisure_rub_3m_amt, --����� �������� ������� � ��������� "�����/�����������/�����" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_groceries_rub_3m_amt, 0) as crd_pos_groceries_rub_3m_amt, --����� �������� ������� � ��������� "��������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_othr_3m_amt, 0) as crd_pos_othr_3m_amt, --����� �������� ������� � ��������� "������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_special_rub_3m_amt, 0) as crd_pos_special_rub_3m_amt, --����� �������� ������� � ��������� "������������������ ������ � ������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_telecom_rub_3m_amt, 0) as crd_pos_telecom_rub_3m_amt, --����� �������� ������� � ��������� "�������������������� ������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_home_repair_rub_3m_amt, 0) as crd_pos_home_repair_rub_3m_amt, --����� �������� ������� � ��������� "������ ��� ����/�������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_tourism_rub_3m_amt, 0) as crd_pos_tourism_rub_3m_amt, --����� �������� ������� � ��������� "������/���������" � ������ �� ����� �� ������ �� 3 ���.
COALESCE(crd_pos_dept_stores_rub_3m_amt, 0) as crd_pos_dept_stores_rub_3m_amt, --����� �������� ������� � ��������� "������������� ��������" � ������ �� ����� �� ������ �� 3 ���.

  
--����  
COALESCE(pnl_oi_othr_amt, 0)                   as pnl_oi_othr_amt, -- �� �� ������ / 
--�� ������  
COALESCE(pnl_T_invest.SUM_pnl_T_invest, 0)     as SUM_pnl_T_invest, --����� �� ������   
COALESCE(inv_tot_bal_rub_amt, 0)               as inv_tot_bal_rub_amt, --������� / ����� ����� ������� � �������������� ��������� (���+��+��) �� ����� ������ � ������  
COALESCE(inv_bo_agrmnt_open_qty, 0)            as inv_bo_agrmnt_open_qty, --���������� �������� ��������� �� �� ����� ������  
--�� �����  
  
--���  
COALESCE(dep_acct_tot_bal_rub_amt, 0)          as dep_acct_tot_bal_rub_amt, --��� (�������) / ��������� ������ �� ���� ��������� ������ ������� �� �������� ���� � ������ (��� ���� ����)  
COALESCE(crd_pos_net_rub_amt, 0)               as crd_pos_net_rub_amt,      --POS-������ /������ ����� POS-�������� (�� ������� ���������)  
  
--���  
COALESCE(dep_acct_tot_davg_mnth_rub_amt, 0)    as dep_acct_tot_davg_mnth_rub_amt, --��������� ������������� ������� (���) �� �������� �����  
----��� � ��������1  
COALESCE(sd_gender_cd, 'M')                    as sd_gender_cd, --���  
COALESCE(sd_age_yrs_comp_nv, 14)               as sd_age_yrs_comp_nv, -- ������� �������, ������  ����� ���  
COALESCE(crd_otf_cash_atm_qty, 0)              as crd_otf_cash_atm_qty, -- ���������� ������ �������� � ���������  
  
  
COALESCE(dep_inc_avg_risk_rub_amt, 0)          as dep_inc_avg_risk_rub_amt, -- ����� �������������� ����� (�������� + ��� ����������) �� �������� ������  
COALESCE(dep_inf_income_rub_amt, 0)            as dep_inf_income_rub_amt,   -- ����� ����������� �� ���� (��������, ������, ����������)  
			 sd_stlmnt_type_cd                 as sd_stlmnt_type_cd, -- ��� ����������� ������ ���������� ������� (�� ���������� ������) - �������������� ��������  
COALESCE(dep_payroll_client_nflag, 0)          as dep_payroll_client_nflag, --���� ����������� ������� (�������������� �/� ���������� �� 3 ��� ���� �� �� ������ �� ������ > 3000 ���)  
COALESCE(crd_otf_cash_rub_amt, 0)              as crd_otf_cash_rub_amt,  --����� ����� ������ �������� �� ���� ������ ������� �� ����� � ������
  
COALESCE(srv_dc_p2p_otf_on_amt, 0)             as srv_dc_p2p_otf_on_amt,  --����� ��������� ������ ��� ����� - �������� ����� �� �� �� �������� ����� (����, ��)  
COALESCE(srv_cc_p2p_otf_on_amt, 0)             as srv_cc_p2p_otf_on_amt,  --����� ��������� ������ �������� - �������� ����� �� �� �� �������� ����� (����, ��)
COALESCE(srv_dc_p2p_inf_on_amt, 0)             as srv_dc_p2p_inf_on_amt,  --����� ��������� �� ������ ������ - �������� ����� �� �� �� �������� ����� (����, ��)
COALESCE(srv_cc_p2p_inf_on_amt, 0)             as srv_cc_p2p_inf_on_amt  --����� ��������� �� ������ ������ - �������� ����� �� �� �� �������� ����� (����, ��)

--���������� � ����	
from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021 as T_clients
inner join s_grnplm_ld_rozn_electron_daas_core_dm.ft_client_aggr_mnth_epk as T_inf
on T_clients.epk_id = T_inf.epk_id
	and T_inf.report_dt > '2020-01-31' --!!!������ 11 ���
	and T_inf.report_dt <= '2021-12-31' 
	and T_inf.report_dt != '2021-01-31'
--��������
left join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info_acquring as pnl_T_acquring
on      T_inf.epk_id    = pnl_T_acquring.epk_id 
	and T_inf.report_dt = pnl_T_acquring.report_dt
--�������
left join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_mass_2020_sbp_2021_info_invest as pnl_T_invest
on      T_inf.epk_id    = pnl_T_invest.epk_id 
	and T_inf.report_dt = pnl_T_invest.report_dt
)