--�������� ����� (�������� ���������� �������) � ������� ������ ������ ��������
--���������� ������� �� �������� � ���� ������ ��� ������� �� ����� ��������
--dep_acct_dep_qty_sum - ���������� ������� (1)
--(2) prd_pl_active_qty,  --�� ���������� �������� ��������������� �������� �� 3 ��� 
--(3) prd_mg_active_qty, --�� ���������� �������� ������ �� 3 ��� (3)
--(4) crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����   (4)
--(5) crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����  (5)
--(6) SUM_pnl_T_acquring, -- �� �������� ( ����� ������ �� �� �� ��������, ����� ������ �� �������) (6)
--(7) ������ drop (7)
--(8) inv_bo_agrmnt_open_qty, --���������� �������� ��������� �� �� ����� ������  (8)


drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_;
create table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_
as 
(select epk_id, year_ from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1);





--1
alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_
add ntile_pnl_oi_amt_sum int
default Null;

--2
alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_
add ntile_pnl_oi_pl_amt int
default Null;

--3
alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_
add ntile_pnl_oi_ml_amt int
default Null;
--4
alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_
add ntile_pnl_oi_cc_amt int
default Null;
--5
alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_
add ntile_pnl_oi_dc_amt int
default Null;

--6
alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_
add ntile_SUM_pnl_T_acquring int
default Null;
--8
alter table s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_
add ntile_SUM_pnl_T_invest int
default Null;



--1 ����� �� pnl_oi_amt_sum, ������ ������� ntile_pnl_oi_amt_sum
--��� ������� �� �������� dep_acct_dep_qty_sum
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t1 set ntile_pnl_oi_amt_sum = 
(select distinct ntile_
from 
       (
       select epk_id,
       ntile(4) over (partition by split_1, ntile_1 order by pnl_oi_amt_sum) as ntile_

       from (select epk_id, ntile_1, Split_1,
             pnl_oi_amt_sum, pnl_oi_ml_amt, pnl_oi_pl_amt, pnl_oi_cc_amt, pnl_oi_dc_amt, SUM_pnl_T_acquring, SUM_pnl_T_invest,
             --
             dep_acct_dep_qty_sum, --���������� ������� (1)
             prd_pl_active_qty,  --�� ���������� �������� ��������������� �������� �� 3 ��� (2)
             prd_mg_active_qty, --�� ���������� �������� ������ �� 3 ��� (3)
             crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����   (4)
             crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����  (5)
             --SUM_pnl_T_acquring, -- �� �������� ( ����� ������ �� �� �� ��������, ����� ������ �� �������) (6)
             inv_bo_agrmnt_open_qty --���������� �������� ��������� �� �� ����� ������  (8)  
             from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and dep_acct_dep_qty_sum > 0) T2

       ) t22
       where t1.epk_id = t22.epk_id);

    

    
--2 ����� �� pnl_oi_pl_amt, ������ ������� ntile_pnl_oi_pl_amt
--��� ������� �� �������� prd_pl_active_qty
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t1 set ntile_pnl_oi_pl_amt = 
(select distinct ntile_
from 
       (
       select epk_id,
       ntile(4) over (partition by split_1, ntile_1 order by pnl_oi_pl_amt) as ntile_

       from (select epk_id, ntile_1, Split_1,
             pnl_oi_amt_sum, pnl_oi_ml_amt, pnl_oi_pl_amt, pnl_oi_cc_amt, pnl_oi_dc_amt, SUM_pnl_T_acquring, SUM_pnl_T_invest,
             --
             dep_acct_dep_qty_sum, --���������� ������� (1)
             prd_pl_active_qty,  --�� ���������� �������� ��������������� �������� �� 3 ��� (2)
             prd_mg_active_qty, --�� ���������� �������� ������ �� 3 ��� (3)
             crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����   (4)
             crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����  (5)
             --SUM_pnl_T_acquring, -- �� �������� ( ����� ������ �� �� �� ��������, ����� ������ �� �������) (6)
             inv_bo_agrmnt_open_qty --���������� �������� ��������� �� �� ����� ������  (8)  
             from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and prd_pl_active_qty > 0) T2

       ) t22
       where t1.epk_id = t22.epk_id);

    

    
--3 ����� �� pnl_oi_ml_amt, ������ ������� ntile_pnl_oi_ml_amt
--��� ������� �� �������� prd_mg_active_qty
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t1 set ntile_pnl_oi_ml_amt = 
(select distinct ntile_
from 
       (
       select epk_id,
       ntile(4) over (partition by split_1, ntile_1 order by pnl_oi_ml_amt) as ntile_

       from (select epk_id, ntile_1, Split_1,
             pnl_oi_amt_sum, pnl_oi_ml_amt, pnl_oi_pl_amt, pnl_oi_cc_amt, pnl_oi_dc_amt, SUM_pnl_T_acquring, SUM_pnl_T_invest,
             --
             dep_acct_dep_qty_sum, --���������� ������� (1)
             prd_pl_active_qty,  --�� ���������� �������� ��������������� �������� �� 3 ��� (2)
             prd_mg_active_qty, --�� ���������� �������� ������ �� 3 ��� (3)
             crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����   (4)
             crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����  (5)
             --SUM_pnl_T_acquring, -- �� �������� ( ����� ������ �� �� �� ��������, ����� ������ �� �������) (6)
             inv_bo_agrmnt_open_qty --���������� �������� ��������� �� �� ����� ������  (8)  
             from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and prd_mg_active_qty > 0) T2

       ) t22
       where t1.epk_id = t22.epk_id);

    

    
--4 ����� �� pnl_oi_cc_amt, ������ ������� ntile_pnl_oi_cc_amt
--��� ������� �� �������� crd_cc_active_open_qty
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t1 set ntile_pnl_oi_cc_amt = 
(select distinct ntile_
from 
       (
       select epk_id,
       ntile(4) over (partition by split_1, ntile_1 order by pnl_oi_cc_amt) as ntile_

       from (select epk_id, ntile_1, Split_1,
             pnl_oi_amt_sum, pnl_oi_ml_amt, pnl_oi_pl_amt, pnl_oi_cc_amt, pnl_oi_dc_amt, SUM_pnl_T_acquring, SUM_pnl_T_invest,
             --
             dep_acct_dep_qty_sum, --���������� ������� (1)
             prd_pl_active_qty,  --�� ���������� �������� ��������������� �������� �� 3 ��� (2)
             prd_mg_active_qty, --�� ���������� �������� ������ �� 3 ��� (3)
             crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����   (4)
             crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����  (5)
             --SUM_pnl_T_acquring, -- �� �������� ( ����� ������ �� �� �� ��������, ����� ������ �� �������) (6)
             inv_bo_agrmnt_open_qty --���������� �������� ��������� �� �� ����� ������  (8)  
             from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and crd_cc_active_open_qty > 0) T2

       ) t22
       where t1.epk_id = t22.epk_id);

    

    
--5 ����� �� pnl_oi_dc_amt, ������ ������� ntile_pnl_oi_dc_amt
--��� ������� �� �������� crd_dc_act_spend_qty
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t1 set ntile_pnl_oi_dc_amt = 
(select distinct ntile_
from 
       (
       select epk_id,
       ntile(4) over (partition by split_1, ntile_1 order by pnl_oi_dc_amt) as ntile_

       from (select epk_id, ntile_1, Split_1,
             pnl_oi_amt_sum, pnl_oi_ml_amt, pnl_oi_pl_amt, pnl_oi_cc_amt, pnl_oi_dc_amt, SUM_pnl_T_acquring, SUM_pnl_T_invest,
             --
             dep_acct_dep_qty_sum, --���������� ������� (1)
             prd_pl_active_qty,  --�� ���������� �������� ��������������� �������� �� 3 ��� (2)
             prd_mg_active_qty, --�� ���������� �������� ������ �� 3 ��� (3)
             crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����   (4)
             crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����  (5)
             --SUM_pnl_T_acquring, -- �� �������� ( ����� ������ �� �� �� ��������, ����� ������ �� �������) (6)
             inv_bo_agrmnt_open_qty --���������� �������� ��������� �� �� ����� ������  (8)  
             from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and crd_dc_act_spend_qty > 0) T2

       ) t22
       where t1.epk_id = t22.epk_id);

    

    
--6 ����� �� SUM_pnl_T_acquring, ������ ������� ntile_SUM_pnl_T_acquring
--��� ������� �� �������� SUM_pnl_T_acquring
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t1 set ntile_SUM_pnl_T_acquring = 
(select distinct ntile_
from 
       (
       select epk_id,
       ntile(4) over (partition by split_1, ntile_1 order by SUM_pnl_T_acquring) as ntile_

       from (select epk_id, ntile_1, Split_1,
             pnl_oi_amt_sum, pnl_oi_ml_amt, pnl_oi_pl_amt, pnl_oi_cc_amt, pnl_oi_dc_amt, SUM_pnl_T_acquring, SUM_pnl_T_invest,
             --
             dep_acct_dep_qty_sum, --���������� ������� (1)
             prd_pl_active_qty,  --�� ���������� �������� ��������������� �������� �� 3 ��� (2)
             prd_mg_active_qty, --�� ���������� �������� ������ �� 3 ��� (3)
             crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����   (4)
             crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����  (5)
             --SUM_pnl_T_acquring, -- �� �������� ( ����� ������ �� �� �� ��������, ����� ������ �� �������) (6)
             inv_bo_agrmnt_open_qty --���������� �������� ��������� �� �� ����� ������  (8)  
             from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and SUM_pnl_T_acquring > 0) T2

       ) t22
       where t1.epk_id = t22.epk_id);

    

    
--8 ����� �� SUM_pnl_T_invest, ������ ������� ntile_SUM_pnl_T_invest
--��� ������� �� �������� inv_bo_agrmnt_open_qty
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t1 set ntile_SUM_pnl_T_invest = 
(select distinct ntile_
from 
       (
       select epk_id,
       ntile(4) over (partition by split_1, ntile_1 order by SUM_pnl_T_invest) as ntile_

       from (select epk_id, ntile_1, Split_1,
             pnl_oi_amt_sum, pnl_oi_ml_amt, pnl_oi_pl_amt, pnl_oi_cc_amt, pnl_oi_dc_amt, SUM_pnl_T_acquring, SUM_pnl_T_invest,
             --
             dep_acct_dep_qty_sum, --���������� ������� (1)
             prd_pl_active_qty,  --�� ���������� �������� ��������������� �������� �� 3 ��� (2)
             prd_mg_active_qty, --�� ���������� �������� ������ �� 3 ��� (3)
             crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����   (4)
             crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����  (5)
             --SUM_pnl_T_acquring, -- �� �������� ( ����� ������ �� �� �� ��������, ����� ������ �� �������) (6)
             inv_bo_agrmnt_open_qty --���������� �������� ��������� �� �� ����� ������  (8)  
             from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and inv_bo_agrmnt_open_qty > 0) T2

       ) t22
       where t1.epk_id = t22.epk_id);









	
--������ ��������� ����� 25 � 50
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_amt_sum = 2 where ntile_pnl_oi_amt_sum = 3; --1
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_pl_amt = 2 where ntile_pnl_oi_pl_amt = 3; --2
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_ml_amt = 2 where ntile_pnl_oi_ml_amt = 3; --3
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_cc_amt = 2 where ntile_pnl_oi_cc_amt = 3; --4
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_dc_amt = 2 where ntile_pnl_oi_dc_amt = 3; --5
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_SUM_pnl_T_acquring = 2 where ntile_SUM_pnl_T_acquring = 3; --6
--update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_othr_amt = 2 where ntile_pnl_oi_othr_amt = 3; --7
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_SUM_pnl_T_invest = 2 where ntile_SUM_pnl_T_invest = 3; --8

--����� ��� ��������� �������� 4 �� 3
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_amt_sum = 3 where ntile_pnl_oi_amt_sum = 4; --1
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_pl_amt = 3 where ntile_pnl_oi_pl_amt = 4; --2
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_ml_amt = 3 where ntile_pnl_oi_ml_amt = 4; --3
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_cc_amt = 3 where ntile_pnl_oi_cc_amt = 4; --4
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_dc_amt = 3 where ntile_pnl_oi_dc_amt = 4; --5
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_SUM_pnl_T_acquring = 3 where ntile_SUM_pnl_T_acquring = 4; --6
--update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_othr_amt = 3 where ntile_pnl_oi_othr_amt = 4; --7
update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_SUM_pnl_T_invest = 3 where ntile_SUM_pnl_T_invest = 4; --8






--�������� ����� (�������� ���������� �������) � ������� ������ ������ ��������
--���������� ������� �� �������� � ���� ������ ��� ������� �� ����� ��������
--dep_acct_dep_qty_sum - ���������� ������� (1)
--(2) prd_pl_active_qty,  --�� ���������� �������� ��������������� �������� �� 3 ��� 
--(3) prd_mg_active_qty, --�� ���������� �������� ������ �� 3 ��� (3)
--(4) crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����   (4)
--(5) crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����  (5)
--(6) SUM_pnl_T_acquring, -- �� �������� ( ����� ������ �� �� �� ��������, ����� ������ �� �������) (6)
--(7) ������ drop (7)
--(8) inv_bo_agrmnt_open_qty, --���������� �������� ��������� �� �� ����� ������  (8)




--�������� ��������� 5 ��� 2020�. (� 2020 �. ��� �������.) (� 2021 ���� �� �� ������� ������� � 1-3 �� �� ���� ��� ���� �������)
--1 �������� ntile_pnl_oi_amt_sum
--��� ������� �� �������� dep_acct_dep_qty_sum �� 2021 ��� � �������� �� 2020

update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_amt_sum = 5 
where 1=1 
             and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1
                           where 1=1 
                                  and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and dep_acct_dep_qty_sum > 0)
                                  and year_ = 2020 and dep_acct_dep_qty_sum = 0
                           )
             and year_ = 2020;
    

    
--2 �������� ntile_pnl_oi_pl_amt
--��� ������� �� �������� prd_pl_active_qty �� 2021 ��� � �������� �� 2020

update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_pl_amt = 5 
where 1=1 
             and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1
                           where 1=1 
                                  and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and prd_pl_active_qty > 0)
                                  and year_ = 2020 and dep_acct_dep_qty_sum = 0
                           )
             and year_ = 2020;
    

    
--3 �������� ntile_pnl_oi_ml_amt
--��� ������� �� �������� prd_mg_active_qty �� 2021 ��� � �������� �� 2020

update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_ml_amt = 5 
where 1=1 
             and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1
                           where 1=1 
                                  and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and prd_mg_active_qty > 0)
                                  and year_ = 2020 and dep_acct_dep_qty_sum = 0
                           )
             and year_ = 2020;
    

    
--4 �������� ntile_pnl_oi_cc_amt
--��� ������� �� �������� crd_cc_active_open_qty �� 2021 ��� � �������� �� 2020

update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_cc_amt = 5 
where 1=1 
             and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1
                           where 1=1 
                                  and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and crd_cc_active_open_qty > 0)
                                  and year_ = 2020 and dep_acct_dep_qty_sum = 0
                           )
             and year_ = 2020;
    

    
--5 �������� ntile_pnl_oi_dc_amt
--��� ������� �� �������� crd_dc_act_spend_qty �� 2021 ��� � �������� �� 2020

update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_pnl_oi_dc_amt = 5 
where 1=1 
             and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1
                           where 1=1 
                                  and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and crd_dc_act_spend_qty > 0)
                                  and year_ = 2020 and dep_acct_dep_qty_sum = 0
                           )
             and year_ = 2020;
    

    
--6 �������� ntile_SUM_pnl_T_acquring
--��� ������� �� �������� SUM_pnl_T_acquring �� 2021 ��� � �������� �� 2020

update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_SUM_pnl_T_acquring = 5 
where 1=1 
             and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1
                           where 1=1 
                                  and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and SUM_pnl_T_acquring > 0)
                                  and year_ = 2020 and dep_acct_dep_qty_sum = 0
                           )
             and year_ = 2020;
    

    
--8 �������� ntile_SUM_pnl_T_invest
--��� ������� �� �������� inv_bo_agrmnt_open_qty �� 2021 ��� � �������� �� 2020

update s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ set ntile_SUM_pnl_T_invest = 5 
where 1=1 
             and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1
                           where 1=1 
                                  and epk_id in (select epk_id from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 where year_ = 2021 and inv_bo_agrmnt_open_qty > 0)
                                  and year_ = 2020 and dep_acct_dep_qty_sum = 0
                           )
             and year_ = 2020;





	
--�������� ����������� �������
--����� 
--Null ����� = �� ���� ������ ��������
--5 ����� = ���� � 2021 ���� ������ �� ��� �������� � 2020 ����
			
--ntile_pnl_oi_cc_amt  --4 
--ntile_pnl_oi_dc_amt  --5
--ntile_SUM_pnl_T_acquring --6
--ntile_SUM_pnl_T_invest --8


--(4) crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����   (4)
--(5) crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����  (5)
--(6) SUM_pnl_T_acquring, -- �� �������� ( ����� ������ �� �� �� ��������, ����� ������ �� �������) (6)
--(7) ������ drop (7)
--(8) inv_bo_agrmnt_open_qty, --���������� �������� ��������� �� �� ����� ������  (8)


--select ntile_pnl_oi_amt_sum, count(*) from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_1
--group by ntile_pnl_oi_amt_sum

----------------------1_1 ntile_pnl_oi_pl_amt
--
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_1;

create  table 
s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_1
as
(
select 
	t1.Split_1, 
	t1.ntile_1,
	t1.year_, 
--avg(ntile_1_max_v) as ntile_1_max_v,
--T_inf.seg_service_channel_cd, --��������� �����  
--T_inf.report_dt, --����  
--prd_client_active_nflag, --������� �� ������  
--tp_active_kind_cd, --�������� ��������� ������ �����  
--tp_active_nflag, --������������� ���������� �������
count(distinct t1.epk_id) as count_people,

--avg(pnl_oi_total_amt)                as pnl_oi_total_amt, -- ����� ������ ������������� ������ (��) �� ������� �� �������� �����

ntile_pnl_oi_amt_sum,
--������ (1)
avg(pnl_oi_amt_sum) 	             as pnl_oi_amt_sum, --�� ������
avg(dep_acct_dep_qty_sum)            as count_dep, --���������� ������

--pnl_oi_td_amt, -- //����� ������������� ������ (��) �� ������� �� �������� ����� �� ������� ������  
--pnl_oi_ca_amt, --//����� ������������� ������ (��) �� ������� �� �������� ����� �� ������� ������  
--dep_acct_dep_td_qty, --����� ���-�� �����������  ������ ������� ������� ������� �� �������� ����  
--dep_acct_dep_ca_qty, --����� ���-�� ����������� ������� ������ ������� �� �������� ����  
avg(dep_acct_dep_tot_bal_rub_amt)    as dep_acct_dep_tot_bal_rub_amt, -- ��� �� ������� / ��������� ������ �� ���������� (������� � �������) ������ ������� �� �������� ���� � ������  
 
--���  
avg(dep_acct_tot_bal_rub_amt) as dep_acct_tot_bal_rub_amt, --��� (�������) / ��������� ������ �� ���� ��������� ������ ������� �� �������� ���� � ������ (��� ���� ����)  
--avg(crd_pos_net_rub_amt) as crd_pos_net_rub_amt,      --POS-������ /������ ����� POS-�������� (�� ������� ���������)  

--���  
AVG(dep_acct_tot_davg_mnth_rub_amt)  as dep_acct_tot_davg_mnth_rub_amt, --��������� ������������� ������� (���) �� �������� �����
----��� � ��������1
avg(sd_gender_cd)                    as sd_gender_cd, --���
avg(sd_age_yrs_comp_nv)              as sd_age_yrs_comp_nv, -- ������� �������, ������  ����� ���
--max(t2.sd_stlmnt_type_cd)            as t2.sd_stlmnt_type_cd, -- ��� ����������� ������ ���������� ������� (�� ���������� ������) - �������������� ��������
avg(type_CITY_MLNR)                  as type_CITY_MLNR, 	-- ��� ����������� ������
avg(type_VILLAGE)                    as type_VILLAGE,		-- ��� ����������� ������
avg(type_CITY_OTHER)                 as type_CITY_OTHER,	-- ��� ����������� ������

--AVG(crd_otf_cash_atm_qty)            as crd_otf_cash_atm_qty, -- ���������� ������ �������� � ���������

--AVG(dep_inc_avg_risk_rub_amt)        as dep_inc_avg_risk_rub_amt, -- ����� �������������� ����� (�������� + ���.����������) �� �������� ������
AVG(dep_inf_income_rub_amt)          as dep_inf_income_rub_amt,   -- ����� ����������� �� ���� (��������, ������, ����������)
AVG(dep_payroll_client_nflag)        as dep_payroll_client_nflag -- ���� ����������� ������� (�������������� �/� ���������� �� 3 ���. ���� �� �� ������ �� ������ > 3000 ���.)
--AVG(crd_otf_cash_rub_amt)            as crd_otf_cash_rub_amt, --����� ����� ������ �������� �� ���� ������ ������� �� ����� � ������


--AVG(srv_dc_p2p_otf_on_amt)           as srv_dc_p2p_otf_on_amt,  --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_cc_p2p_otf_on_amt)           as srv_cc_p2p_otf_on_amt, --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_dc_p2p_inf_on_amt)           as srv_dc_p2p_inf_on_amt, --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_cc_p2p_inf_on_amt)           as srv_cc_p2p_inf_on_amt --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)



from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 t1
join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t2
on t1.epk_id = t2.epk_id and t1.year_ = t2.year_
group by Split_1, ntile_1, t1.year_, ntile_pnl_oi_amt_sum
order by Split_1, ntile_1, t1.year_, ntile_pnl_oi_amt_sum
);



----------------------------------------------1 2 pnl_oi_pl_amt
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_2;

create  table 
s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_2
as
(
select 
	t1.Split_1, 
	t1.ntile_1,
	t1.year_, 
--avg(ntile_1_max_v) as ntile_1_max_v,
--T_inf.seg_service_channel_cd, --��������� �����  
--T_inf.report_dt, --����  
--prd_client_active_nflag, --������� �� ������  
--tp_active_kind_cd, --�������� ��������� ������ �����  
--tp_active_nflag, --������������� ���������� �������
	count(distinct t1.epk_id) as count_people,

--avg(pnl_oi_total_amt)                as pnl_oi_total_amt, -- ����� ������ ������������� ������ (��) �� ������� �� �������� �����
ntile_pnl_oi_pl_amt,
--������
--avg(pnl_oi_amt_sum) 	             as pnl_oi_amt_sum, --�� ������
--avg(dep_acct_dep_qty_sum)            as count_dep, --���������� ������
-- (1)
--pnl_oi_td_amt, -- //����� ������������� ������ (��) �� ������� �� �������� ����� �� ������� ������  
--pnl_oi_ca_amt, --//����� ������������� ������ (��) �� ������� �� �������� ����� �� ������� ������  
--dep_acct_dep_td_qty, --����� ���-�� �����������  ������ ������� ������� ������� �� �������� ����  
--dep_acct_dep_ca_qty, --����� ���-�� ����������� ������� ������ ������� �� �������� ����  
--avg(dep_acct_dep_tot_bal_rub_amt)    as dep_acct_dep_tot_bal_rub_amt, -- ��� �� ������� / ��������� ������ �� ���������� (������� � �������) ������ ������� �� �������� ���� � ������  
--��  (2)
avg(pnl_oi_pl_amt) 					 as pnl_oi_pl_amt, -- �� ��      / ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������������� ��������  
avg(lne_pl_debt_os_rub_amt)          as lne_pl_debt_os_rub_amt,   --�� �� / ��������� ������� ��������� ����� (������� ����� + ���������) - ��������������� ������� (PL)
avg(prd_pl_active_qty)               as prd_pl_active_qty,  --�� ���������� �������� ��������������� �������� �� 3 ���
avg(lne_overdraft_overdue_nflag)     as lne_overdraft_overdue_nflag, --������� ��������� --��������

--��� 
--avg(dep_acct_tot_bal_rub_amt) as dep_acct_tot_bal_rub_amt, --��� (�������) / ��������� ������ �� ���� ��������� ������ ������� �� �������� ���� � ������ (��� ���� ����)  
--avg(crd_pos_net_rub_amt) as crd_pos_net_rub_amt,      --POS-������ /������ ����� POS-�������� (�� ������� ���������)  

--���  
--AVG(dep_acct_tot_davg_mnth_rub_amt)  as dep_acct_tot_davg_mnth_rub_amt, --��������� ������������� ������� (���) �� �������� �����
----��� � ��������1
avg(sd_gender_cd)                    as sd_gender_cd, --���
avg(sd_age_yrs_comp_nv)              as sd_age_yrs_comp_nv, -- ������� �������, ������  ����� ���
--max(t2.sd_stlmnt_type_cd)            as t2.sd_stlmnt_type_cd, -- ��� ����������� ������ ���������� ������� (�� ���������� ������) - �������������� ��������
avg(type_CITY_MLNR)                  as type_CITY_MLNR, 	-- ��� ����������� ������
avg(type_VILLAGE)                    as type_VILLAGE,		-- ��� ����������� ������
avg(type_CITY_OTHER)                 as type_CITY_OTHER,	-- ��� ����������� ������

--AVG(crd_otf_cash_atm_qty)            as crd_otf_cash_atm_qty, -- ���������� ������ �������� � ���������

--AVG(dep_inc_avg_risk_rub_amt)        as dep_inc_avg_risk_rub_amt, -- ����� �������������� ����� (�������� + ���.����������) �� �������� ������
AVG(dep_inf_income_rub_amt)          as dep_inf_income_rub_amt,   -- ����� ����������� �� ���� (��������, ������, ����������)
AVG(dep_payroll_client_nflag)        as dep_payroll_client_nflag, -- ���� ����������� ������� (�������������� �/� ���������� �� 3 ���. ���� �� �� ������ �� ������ > 3000 ���.)
AVG(crd_otf_cash_rub_amt)            as crd_otf_cash_rub_amt--, --����� ����� ������ �������� �� ���� ������ ������� �� ����� � ������


--AVG(srv_dc_p2p_otf_on_amt)           as srv_dc_p2p_otf_on_amt,  --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_cc_p2p_otf_on_amt)           as srv_cc_p2p_otf_on_amt, --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_dc_p2p_inf_on_amt)           as srv_dc_p2p_inf_on_amt, --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_cc_p2p_inf_on_amt)           as srv_cc_p2p_inf_on_amt --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)

from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 t1
join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t2
on t1.epk_id = t2.epk_id and t1.year_ = t2.year_
group by Split_1, ntile_1, t1.year_, ntile_pnl_oi_pl_amt
order by Split_1, ntile_1, t1.year_, ntile_pnl_oi_pl_amt
);





---------------------------------------------- 3 ntile_pnl_oi_ml_amt
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_3;

create  table 
s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_3
as
(
select t1.Split_1, t1.ntile_1, t1.year_,
	avg(ntile_1_max_v) as ntile_1_max_v,
--T_inf.seg_service_channel_cd, --��������� �����  
--T_inf.report_dt, --����  
--prd_client_active_nflag, --������� �� ������  
--tp_active_kind_cd, --�������� ��������� ������ �����  
--tp_active_nflag, --������������� ���������� �������
	count(distinct t1.epk_id) as count_people,

avg(pnl_oi_total_amt)                as pnl_oi_total_amt, -- ����� ������ ������������� ������ (��) �� ������� �� �������� �����

ntile_pnl_oi_ml_amt,
--avg(pnl_oi_amt_sum) 	             as pnl_oi_amt_sum, --�� ������
--avg(dep_acct_dep_qty_sum)            as count_dep, --���������� ������
-- (1)
--pnl_oi_td_amt, -- //����� ������������� ������ (��) �� ������� �� �������� ����� �� ������� ������  
--pnl_oi_ca_amt, --//����� ������������� ������ (��) �� ������� �� �������� ����� �� ������� ������  
--dep_acct_dep_td_qty, --����� ���-�� �����������  ������ ������� ������� ������� �� �������� ����  
--dep_acct_dep_ca_qty, --����� ���-�� ����������� ������� ������ ������� �� �������� ����  
--avg(dep_acct_dep_tot_bal_rub_amt)    as dep_acct_dep_tot_bal_rub_amt, -- ��� �� ������� / ��������� ������ �� ���������� (������� � �������) ������ ������� �� �������� ���� � ������  
--�� (2)
--avg(pnl_oi_pl_amt) 					 as pnl_oi_pl_amt, -- �� ��      / ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������������� ��������  
--avg(lne_pl_debt_os_rub_amt)          as lne_pl_debt_os_rub_amt,   --�� �� /  
--avg(prd_pl_active_qty)               as prd_pl_active_qty,  --�� ���������� �������� ��������������� �������� �� 3 ���
  
--�� (3)
avg(pnl_oi_ml_amt)                   as pnl_oi_ml_amt, -- �� �� �� /����� ������������� ������ (��) �� ������� �� �������� ����� �� �������� (���������) ��������  
avg(lne_mg_debt_os_rub_amt)          as lne_mg_debt_os_rub_amt,   --�� / ��������� ������� ��������� ����� (������� ����� + ���������) - �������  
avg(prd_mg_active_qty)               as prd_mg_active_qty, --�� ���������� �������� ������ �� 3 ���
--��  
--avg(pnl_oi_cc_amt)                   as pnl_oi_cc_amt, -- �� �� �� / ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������� ������  
--avg(crd_cc_tot_credit_rub_amt)       as crd_cc_tot_credit_rub_amt,--�� / ����� ������ ������������� �� �� (�������� ���� + % + ��������� + ������) �� �������� ����, ���
--avg(crd_cc_active_open_qty)			 as crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����  
  
--��  
--avg(pnl_oi_dc_amt)                   as pnl_oi_dc_amt, -- �� �� ������ **�����/ ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������� ������  
--avg(crd_dc_act_spend_qty)            as crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����  
--  
--���  
--avg(SUM_pnl_T_acquring)				 as SUM_pnl_T_acquring, -- �� ��������  
  
--����  
--avg(pnl_oi_othr_amt) 				 as pnl_oi_othr_amt, -- �� �� ������ / 
--�� ������  
--avg(SUM_pnl_T_invest) 			     as SUM_pnl_T_invest, --����� �� ������   
--avg(inv_tot_bal_rub_amt) 			 as inv_tot_bal_rub_amt, --������� / ����� ����� ������� � �������������� ��������� (���+��+��) �� ����� ������ � ������  
--avg(inv_bo_agrmnt_open_qty)			 as inv_bo_agrmnt_open_qty, --���������� �������� ��������� �� �� ����� ������  
--�� �����  
  
--���  
--avg(dep_acct_tot_bal_rub_amt) as dep_acct_tot_bal_rub_amt, --��� (�������) / ��������� ������ �� ���� ��������� ������ ������� �� �������� ���� � ������ (��� ���� ����)  
--avg(crd_pos_net_rub_amt) as crd_pos_net_rub_amt,      --POS-������ /������ ����� POS-�������� (�� ������� ���������)  

--���  
--AVG(dep_acct_tot_davg_mnth_rub_amt)  as dep_acct_tot_davg_mnth_rub_amt, --��������� ������������� ������� (���) �� �������� �����
----��� � ��������1
avg(sd_gender_cd)                    as sd_gender_cd, --���
avg(sd_age_yrs_comp_nv)              as sd_age_yrs_comp_nv, -- ������� �������, ������  ����� ���
--max(t2.sd_stlmnt_type_cd)            as t2.sd_stlmnt_type_cd, -- ��� ����������� ������ ���������� ������� (�� ���������� ������) - �������������� ��������
avg(type_CITY_MLNR)                  as type_CITY_MLNR, 	-- ��� ����������� ������
avg(type_VILLAGE)                    as type_VILLAGE,		-- ��� ����������� ������
avg(type_CITY_OTHER)                 as type_CITY_OTHER,	-- ��� ����������� ������

--AVG(crd_otf_cash_atm_qty)            as crd_otf_cash_atm_qty, -- ���������� ������ �������� � ���������



AVG(dep_inc_avg_risk_rub_amt)        as dep_inc_avg_risk_rub_amt, -- ����� �������������� ����� (�������� + ���.����������) �� �������� ������
AVG(dep_inf_income_rub_amt)          as dep_inf_income_rub_amt,   -- ����� ����������� �� ���� (��������, ������, ����������)
AVG(dep_payroll_client_nflag)        as dep_payroll_client_nflag, -- ���� ����������� ������� (�������������� �/� ���������� �� 3 ���. ���� �� �� ������ �� ������ > 3000 ���.)
AVG(crd_otf_cash_rub_amt)            as crd_otf_cash_rub_amt --����� ����� ������ �������� �� ���� ������ ������� �� ����� � ������


--AVG(srv_dc_p2p_otf_on_amt)           as srv_dc_p2p_otf_on_amt,  --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_cc_p2p_otf_on_amt)           as srv_cc_p2p_otf_on_amt, --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_dc_p2p_inf_on_amt)           as srv_dc_p2p_inf_on_amt, --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_cc_p2p_inf_on_amt)           as srv_cc_p2p_inf_on_amt --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)

from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 t1
join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t2
on t1.epk_id = t2.epk_id and t1.year_ = t2.year_
group by Split_1, ntile_1, t1.year_, ntile_pnl_oi_ml_amt
);




---------------------------------------------- 4 ntile_pnl_oi_cc_amt
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_4;

create  table 
s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_4
as
(
select t1.Split_1, t1.year_, t1.ntile_1,
--avg(ntile_1_max_v) as ntile_1_max_v,
--T_inf.seg_service_channel_cd, --��������� �����  
--T_inf.report_dt, --����  
--prd_client_active_nflag, --������� �� ������  
--tp_active_kind_cd, --�������� ��������� ������ �����  
--tp_active_nflag, --������������� ���������� �������
	count(distinct t1.epk_id) as count_people,

avg(pnl_oi_total_amt)                as pnl_oi_total_amt, -- ����� ������ ������������� ������ (��) �� ������� �� �������� �����

ntile_pnl_oi_cc_amt,
--�����(1)
--�� (2)  
--�� (3)  
--�� (4) 
avg(pnl_oi_cc_amt)                   as pnl_oi_cc_amt, -- �� �� �� / ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������� ������  
avg(crd_cc_tot_credit_rub_amt)       as crd_cc_tot_credit_rub_amt,--�� / ����� ������ ������������� �� �� (�������� ���� + % + ��������� + ������) �� �������� ����, ���
avg(crd_cc_active_open_qty)			 as crd_cc_active_open_qty, --�� / ���������� �������� �������� ��������� ����  
avg(crd_cc_open_qty) as crd_cc_open_qty,
avg(crd_cc_act_spend_qty) as crd_cc_act_spend_qty,
--��  (5)
--���  (6)
--���� (7)
--�� ������ (8)
--���  
--AVG(dep_acct_tot_davg_mnth_rub_amt)  as dep_acct_tot_davg_mnth_rub_amt, --��������� ������������� ������� (���) �� �������� �����
----��� � ��������1
avg(sd_gender_cd)                    as sd_gender_cd, --���
avg(sd_age_yrs_comp_nv)              as sd_age_yrs_comp_nv, -- ������� �������, ������  ����� ���
--max(t2.sd_stlmnt_type_cd)            as t2.sd_stlmnt_type_cd, -- ��� ����������� ������ ���������� ������� (�� ���������� ������) - �������������� ��������
avg(type_CITY_MLNR)                  as type_CITY_MLNR, 	-- ��� ����������� ������
avg(type_VILLAGE)                    as type_VILLAGE,		-- ��� ����������� ������
avg(type_CITY_OTHER)                 as type_CITY_OTHER,	-- ��� ����������� ������

AVG(crd_otf_cash_atm_qty)            as crd_otf_cash_atm_qty, -- ���������� ������ �������� � ���������



AVG(dep_inc_avg_risk_rub_amt)        as dep_inc_avg_risk_rub_amt, -- ����� �������������� ����� (�������� + ���.����������) �� �������� ������
AVG(dep_inf_income_rub_amt)          as dep_inf_income_rub_amt,   -- ����� ����������� �� ���� (��������, ������, ����������)
AVG(dep_payroll_client_nflag)        as dep_payroll_client_nflag, -- ���� ����������� ������� (�������������� �/� ���������� �� 3 ���. ���� �� �� ������ �� ������ > 3000 ���.)
AVG(crd_otf_cash_rub_amt)            as crd_otf_cash_rub_amt, --����� ����� ������ �������� �� ���� ������ ������� �� ����� � ������


--AVG(srv_dc_p2p_otf_on_amt)           as srv_dc_p2p_otf_on_amt,  --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
AVG(srv_cc_p2p_otf_on_amt)           as srv_cc_p2p_otf_on_amt --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_dc_p2p_inf_on_amt)           as srv_dc_p2p_inf_on_amt, --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_cc_p2p_inf_on_amt)           as srv_cc_p2p_inf_on_amt --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)

from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 t1
join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t2
on t1.epk_id = t2.epk_id and t1.year_ = t2.year_
group by Split_1, t1.year_, ntile_1, ntile_pnl_oi_cc_amt
order by Split_1, t1.year_, ntile_1, ntile_pnl_oi_cc_amt
);


---------------------------------------------- 5 ntile_pnl_oi_dc_amt
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_5;

create  table 
s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_5
as
(
select t1.Split_1, t1.year_, t1.ntile_1,
avg(ntile_1_max_v) as ntile_1_max_v,
--T_inf.seg_service_channel_cd, --��������� �����  
--T_inf.report_dt, --����  
--prd_client_active_nflag, --������� �� ������  
--tp_active_kind_cd, --�������� ��������� ������ �����  
--tp_active_nflag, --������������� ���������� �������
count(distinct t1.epk_id) as count_people,

avg(pnl_oi_total_amt)                as pnl_oi_total_amt, -- ����� ������ ������������� ������ (��) �� ������� �� �������� �����

ntile_pnl_oi_dc_amt,
--avg(pnl_oi_amt_sum) 	             as pnl_oi_amt_sum, --�� ������
--avg(dep_acct_dep_qty_sum)            as count_dep, --���������� ������
--(1)
--��  (2)
--�� (3)            
--�� (4) 
--��  (5)
avg(pnl_oi_dc_amt)                   as pnl_oi_dc_amt, -- �� �� ������ **�����/ ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������� ������  
avg(crd_dc_act_spend_qty)            as crd_dc_act_spend_qty, --���. ���-�� �������� �� ��������� ��������� ��������� ����  
avg(crd_dc_open_qty) 				as crd_dc_open_qty,
--  
--���  (6)
--avg(SUM_pnl_T_acquring)				 as SUM_pnl_T_acquring, -- �� ��������  
  
--����  (7)
--�� ������  (8)
--���  
avg(dep_acct_tot_bal_rub_amt) as dep_acct_tot_bal_rub_amt, --��� (�������) / ��������� ������ �� ���� ��������� ������ ������� �� �������� ���� � ������ (��� ���� ����)  
avg(crd_pos_net_rub_amt) as crd_pos_net_rub_amt,      --POS-������ /������ ����� POS-�������� (�� ������� ���������)  
--��� 
--AVG(dep_acct_tot_davg_mnth_rub_amt)  as dep_acct_tot_davg_mnth_rub_amt, --��������� ������������� ������� (���) �� �������� �����
----��� � ��������1
avg(sd_gender_cd)                    as sd_gender_cd, --���
avg(sd_age_yrs_comp_nv)              as sd_age_yrs_comp_nv, -- ������� �������, ������  ����� ���
--max(t2.sd_stlmnt_type_cd)            as t2.sd_stlmnt_type_cd, -- ��� ����������� ������ ���������� ������� (�� ���������� ������) - �������������� ��������
avg(type_CITY_MLNR)                  as type_CITY_MLNR, 	-- ��� ����������� ������
avg(type_VILLAGE)                    as type_VILLAGE,		-- ��� ����������� ������
avg(type_CITY_OTHER)                 as type_CITY_OTHER,	-- ��� ����������� ������
AVG(crd_otf_cash_atm_qty)            as crd_otf_cash_atm_qty, -- ���������� ������ �������� � ���������

--AVG(dep_inc_avg_risk_rub_amt)        as dep_inc_avg_risk_rub_amt, -- ����� �������������� ����� (�������� + ���.����������) �� �������� ������
AVG(dep_inf_income_rub_amt)          as dep_inf_income_rub_amt,   -- ����� ����������� �� ���� (��������, ������, ����������)
AVG(dep_payroll_client_nflag)        as dep_payroll_client_nflag, -- ���� ����������� ������� (�������������� �/� ���������� �� 3 ���. ���� �� �� ������ �� ������ > 3000 ���.)
AVG(crd_otf_cash_rub_amt)            as crd_otf_cash_rub_amt --����� ����� ������ �������� �� ���� ������ ������� �� ����� � ������

--AVG(srv_dc_p2p_otf_on_amt)           as srv_dc_p2p_otf_on_amt,  --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_cc_p2p_otf_on_amt)           as srv_cc_p2p_otf_on_amt, --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_dc_p2p_inf_on_amt)           as srv_dc_p2p_inf_on_amt, --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)
--AVG(srv_cc_p2p_inf_on_amt)           as srv_cc_p2p_inf_on_amt --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)

from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 t1
join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t2
on t1.epk_id = t2.epk_id and t1.year_ = t2.year_
group by Split_1, T1.year_, ntile_1, ntile_pnl_oi_dc_amt
order by Split_1, T1.year_, ntile_1, ntile_pnl_oi_dc_amt
);



---------------------------------------------- 6 ntile_SUM_pnl_T_acquring
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_6;

create  table 
s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_6
as
(
select t1.Split_1, t1.year_, t1.ntile_1,
avg(ntile_1_max_v) as ntile_1_max_v,
--T_inf.seg_service_channel_cd, --��������� �����  
--T_inf.report_dt, --����  
--prd_client_active_nflag, --������� �� ������  
--tp_active_kind_cd, --�������� ��������� ������ �����  
--tp_active_nflag, --������������� ���������� �������
count(distinct t1.epk_id) as count_people,

avg(pnl_oi_total_amt)                as pnl_oi_total_amt, -- ����� ������ ������������� ������ (��) �� ������� �� �������� �����

ntile_SUM_pnl_T_acquring,
--avg(pnl_oi_amt_sum) 	             as pnl_oi_amt_sum, --�� ������
--avg(dep_acct_dep_qty_sum)            as count_dep, --���������� ������
--(1)
--avg(pnl_oi_pl_amt) (2)               as pnl_oi_pl_amt, -- �� ��      / ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������������� ��������  

--�� (3)  
--�� (4) 
--��  (5)  
--���  (6)
avg(SUM_pnl_T_acquring)				as SUM_pnl_T_acquring, -- �� ��������  
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

--���� (7) 
--avg(pnl_oi_othr_amt) 				    as pnl_oi_othr_amt, -- �� �� ������ / 
--�� ������  
--avg(SUM_pnl_T_invest) --(8)			    as SUM_pnl_T_invest, --����� �� ������   
--avg(inv_tot_bal_rub_amt) 			    as inv_tot_bal_rub_amt, --������� / ����� ����� ������� � �������������� ��������� (���+��+��) �� ����� ������ � ������  
--avg(inv_bo_agrmnt_open_qty)			 as inv_bo_agrmnt_open_qty, --���������� �������� ��������� �� �� ����� ������  
--�� �����  
  
--���  
avg(dep_acct_tot_bal_rub_amt) as dep_acct_tot_bal_rub_amt, --��� (�������) / ��������� ������ �� ���� ��������� ������ ������� �� �������� ���� � ������ (��� ���� ����)  
avg(crd_pos_net_rub_amt) as crd_pos_net_rub_amt,      --POS-������ /������ ����� POS-�������� (�� ������� ���������)  

--���  
AVG(dep_acct_tot_davg_mnth_rub_amt)  as dep_acct_tot_davg_mnth_rub_amt, --��������� ������������� ������� (���) �� �������� �����
----��� � ��������1
avg(sd_gender_cd)                    as sd_gender_cd, --���
avg(sd_age_yrs_comp_nv)              as sd_age_yrs_comp_nv, -- ������� �������, ������  ����� ���
--max(t2.sd_stlmnt_type_cd)            as t2.sd_stlmnt_type_cd, -- ��� ����������� ������ ���������� ������� (�� ���������� ������) - �������������� ��������
avg(type_CITY_MLNR)                  as type_CITY_MLNR, 	-- ��� ����������� ������
avg(type_VILLAGE)                    as type_VILLAGE,		-- ��� ����������� ������
avg(type_CITY_OTHER)                 as type_CITY_OTHER,	-- ��� ����������� ������

AVG(crd_otf_cash_atm_qty)            as crd_otf_cash_atm_qty, -- ���������� ������ �������� � ���������


AVG(dep_inc_avg_risk_rub_amt)        as dep_inc_avg_risk_rub_amt, -- ����� �������������� ����� (�������� + ���.����������) �� �������� ������
AVG(dep_inf_income_rub_amt)          as dep_inf_income_rub_amt,   -- ����� ����������� �� ���� (��������, ������, ����������)
AVG(dep_payroll_client_nflag)        as dep_payroll_client_nflag, -- ���� ����������� ������� (�������������� �/� ���������� �� 3 ���. ���� �� �� ������ �� ������ > 3000 ���.)
AVG(crd_otf_cash_rub_amt)            as crd_otf_cash_rub_amt, --����� ����� ������ �������� �� ���� ������ ������� �� ����� � ������


AVG(srv_dc_p2p_otf_on_amt)           as srv_dc_p2p_otf_on_amt,  --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
AVG(srv_cc_p2p_otf_on_amt)           as srv_cc_p2p_otf_on_amt, --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
AVG(srv_dc_p2p_inf_on_amt)           as srv_dc_p2p_inf_on_amt, --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)
AVG(srv_cc_p2p_inf_on_amt)           as srv_cc_p2p_inf_on_amt --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)

from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 t1
join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t2
on t1.epk_id = t2.epk_id and t1.year_ = t2.year_
group by Split_1, t1.year_, ntile_1, ntile_SUM_pnl_T_acquring
order by Split_1, t1.year_, ntile_1, ntile_SUM_pnl_T_acquring
);





---------------------------------------------- 8 ntile_SUM_pnl_T_invest
drop table if exists s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_8;

create  table 
s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_8
as
(
select t1.Split_1, t1.year_, t1.ntile_1,
avg(ntile_1_max_v) as ntile_1_max_v,
count(distinct t1.epk_id) as count_people,

avg(pnl_oi_total_amt)                as pnl_oi_total_amt, -- ����� ������ ������������� ������ (��) �� ������� �� �������� �����

ntile_SUM_pnl_T_invest,
--avg(pnl_oi_amt_sum) 	             as pnl_oi_amt_sum, --�� ������
--avg(dep_acct_dep_qty_sum)            as count_dep, --���������� ������
-- (1)�����
--��  
--avg(pnl_oi_pl_amt) (2)               as pnl_oi_pl_amt, -- �� ��      / ����� ������������� ������ (��) �� ������� �� �������� ����� �� ��������������� ��������  
--�� (3)  
--�� (4) 
--��  (5)
--���  (6) 
--���� (7) 
-- avg(pnl_oi_othr_amt) 				    as pnl_oi_othr_amt, -- �� �� ������ / 
--�� ������  (8)
avg(SUM_pnl_T_invest)    		    as SUM_pnl_T_invest, --����� �� ������   
avg(inv_tot_bal_rub_amt) 			    as inv_tot_bal_rub_amt, --������� / ����� ����� ������� � �������������� ��������� (���+��+��) �� ����� ������ � ������  
avg(inv_bo_agrmnt_open_qty)			 as inv_bo_agrmnt_open_qty, --���������� �������� ��������� �� �� ����� ������  
--�� �����  
  
--���  
avg(dep_acct_tot_bal_rub_amt) as dep_acct_tot_bal_rub_amt, --��� (�������) / ��������� ������ �� ���� ��������� ������ ������� �� �������� ���� � ������ (��� ���� ����)  
avg(crd_pos_net_rub_amt) as crd_pos_net_rub_amt,      --POS-������ /������ ����� POS-�������� (�� ������� ���������)  

--���  
AVG(dep_acct_tot_davg_mnth_rub_amt)  as dep_acct_tot_davg_mnth_rub_amt, --��������� ������������� ������� (���) �� �������� �����
----��� � ��������1
avg(sd_gender_cd)                    as sd_gender_cd, --���
avg(sd_age_yrs_comp_nv)              as sd_age_yrs_comp_nv, -- ������� �������, ������  ����� ���
--max(t2.sd_stlmnt_type_cd)            as t2.sd_stlmnt_type_cd, -- ��� ����������� ������ ���������� ������� (�� ���������� ������) - �������������� ��������
avg(type_CITY_MLNR)                  as type_CITY_MLNR, 	-- ��� ����������� ������
avg(type_VILLAGE)                    as type_VILLAGE,		-- ��� ����������� ������
avg(type_CITY_OTHER)                 as type_CITY_OTHER,	-- ��� ����������� ������

AVG(crd_otf_cash_atm_qty)            as crd_otf_cash_atm_qty, -- ���������� ������ �������� � ���������



AVG(dep_inc_avg_risk_rub_amt)        as dep_inc_avg_risk_rub_amt, -- ����� �������������� ����� (�������� + ���.����������) �� �������� ������
AVG(dep_inf_income_rub_amt)          as dep_inf_income_rub_amt,   -- ����� ����������� �� ���� (��������, ������, ����������)
AVG(dep_payroll_client_nflag)        as dep_payroll_client_nflag, -- ���� ����������� ������� (�������������� �/� ���������� �� 3 ���. ���� �� �� ������ �� ������ > 3000 ���.)
AVG(crd_otf_cash_rub_amt)            as crd_otf_cash_rub_amt, --����� ����� ������ �������� �� ���� ������ ������� �� ����� � ������


AVG(srv_dc_p2p_otf_on_amt)           as srv_dc_p2p_otf_on_amt,  --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
AVG(srv_cc_p2p_otf_on_amt)           as srv_cc_p2p_otf_on_amt, --����� ��������� ������ ���.����� - �������� ����� �� �� �� �������� ����� (����, ��)
AVG(srv_dc_p2p_inf_on_amt)           as srv_dc_p2p_inf_on_amt, --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)
AVG(srv_cc_p2p_inf_on_amt)           as srv_cc_p2p_inf_on_amt --����� ��������� �� ������ ���.��� - �������� ����� �� �� �� �������� ����� (����, ��)

from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1 t1
join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_123_ t2
on t1.epk_id = t2.epk_id and t1.year_ = t2.year_
group by Split_1, t1.year_, ntile_1, ntile_SUM_pnl_T_invest
order by Split_1, t1.year_, ntile_1, ntile_SUM_pnl_T_invest
);


----------------------------------------------------------
----------------------------------------------------------
----------------------------------------------------------
----------------------------------------------------------
----------------------------------------------------------




--�� ������ �� (��������� �� ����)
---------------------------
select 
case 
	when split_1 = 'left_sbp'
	then '��������� �� �������� ������� (�� 31.12.2021)'
	when split_1 = 'pachege_sbp'
	then '����� �������� ������� (������� 6 ���)'
	when split_1 = 'sbp'
	then '�������� ������� (������� �� 31.01.2021)'
	end as split_1,
ntile_1,
replace(replace(ntile_1_max_v, '2000', '2500'), '3000', '2500') as ntile_1_max_v,
year_,
unique_epk_id,
nasp,
pnl_oi_total_amt,
dep_acct_tot_davg_mnth_rub_amt,
sd_gender_cd,
sd_age_yrs_comp_nv,
sum_dep_payroll_client_nflag,
avg_dep_payroll_client_nflag,
dep_inf_income_rub_amt,
crd_otf_cash_rub_amt,
crd_otf_cash_atm_qty,
srv_dc_p2p_otf_on_amt,
srv_dc_p2p_inf_on_amt,
srv_cc_p2p_otf_on_amt,
srv_cc_p2p_inf_on_amt 
from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000
order by split_1, ntile_1, year_ desc, nasp;
---------------------------
------------------------------------------------------
---------------------------------------------------------------------------------

--�� ������ �� 
select 	
		case 
			when t1.split_1 = 'left_sbp'
			then '��������� �� �������� ������� (�� 31.12.2021)'
			when t1.split_1 = 'pachege_sbp'
			then '����� �������� ������� (������� 6 ���)'
			when t1.split_1 = 'sbp'
			then '�������� ������� (������� �� 31.01.2021)'
		end as split_1,
		T1.ntile_1,
		replace(replace(T1.ntile_1_max_v, '2000', '2500'), '3000', '2500') as ntile_1_max_v,
		T1.year_, 
		T1.count_people,
		T1.pnl_oi_total_amt,
		T1.sd_gender_cd,
		T1.sd_age_yrs_comp_nv,
		T1.sum_dep_payroll_client_nflag,
		T1.avg_dep_payroll_client_nflag,

		
--		t11.split_1,
--		t11.ntile_1,
--		t11.year_,
		case 
			when ntile_pnl_oi_amt_sum = 1
			then '�� 25%'
			when ntile_pnl_oi_amt_sum = 2
			then '25-75%'
			when ntile_pnl_oi_amt_sum = 3
			then '75+%'
			when ntile_pnl_oi_amt_sum = 5
			then '��� pnl � 2020�. ���� pnl � 2021'
		end as persentile_pnl,
		'�� ������ ->' as sep_,

		t11.count_people,
		--t11.ntile_pnl_oi_amt_sum,
		t11.pnl_oi_amt_sum,
		t11.count_dep,
		t11.dep_acct_dep_tot_bal_rub_amt,
		t11.dep_acct_tot_bal_rub_amt,
		t11.dep_acct_tot_davg_mnth_rub_amt,
		t11.sd_gender_cd,
		t11.sd_age_yrs_comp_nv,
		t11.type_city_mlnr,
		t11.type_village,
		t11.type_city_other,
		t11.dep_inf_income_rub_amt,
		t11.dep_payroll_client_nflag,
		
		'�� �� ->' as sep_,
				
		T12.count_people,
		--T12.ntile_pnl_oi_pl_amt,
		T12.pnl_oi_pl_amt,
		T12.lne_pl_debt_os_rub_amt,
		T12.prd_pl_active_qty,
		T12.sd_gender_cd,
		T12.sd_age_yrs_comp_nv,
		T12.type_city_mlnr,
		T12.type_village,
		T12.type_city_other,
		T12.dep_inf_income_rub_amt,
		T12.dep_payroll_client_nflag,
		
		'�� �� ->' as sep_,
		

		T13.count_people,
		--T13.pnl_oi_total_amt,
		--T13.ntile_pnl_oi_ml_amt,
		T13.pnl_oi_ml_amt,
		T13.lne_mg_debt_os_rub_amt,
		T13.prd_mg_active_qty,
		T13.sd_gender_cd,
		T13.sd_age_yrs_comp_nv,
		T13.type_city_mlnr,
		T13.type_village,
		T13.type_city_other,
		T13.dep_inf_income_rub_amt,
		T13.dep_payroll_client_nflag,
		'�� �� ->' as sep_,
		
		T14.count_people,
		T14.pnl_oi_cc_amt,
		T14.crd_cc_tot_credit_rub_amt,
		T14.crd_cc_active_open_qty,
		T14.crd_cc_open_qty,
		T14.sd_gender_cd, 
		T14.sd_age_yrs_comp_nv,
		T14.type_city_mlnr,
		T14.type_village,
		T14.type_city_other,
		T14.dep_inf_income_rub_amt,
		T14.dep_payroll_client_nflag,
		T14.srv_cc_p2p_otf_on_amt,

		'�� �� ->' as sep_,
		T15.count_people,
		T15.pnl_oi_dc_amt,
		T15.dep_acct_tot_bal_rub_amt,
		T15.crd_pos_net_rub_amt,
		T15.sd_gender_cd,
		T15.sd_age_yrs_comp_nv,
		T15.type_city_mlnr,
		T15.type_village,
		T15.type_city_other,
		T15.crd_otf_cash_atm_qty,
		T15.dep_inf_income_rub_amt,
		T15.dep_payroll_client_nflag,
		T15.crd_otf_cash_rub_amt,
		T15.dep_inf_income_rub_amt,
		T15.dep_payroll_client_nflag,
		T15.crd_otf_cash_rub_amt,
		
		'�� ����-� ->' as sep_,

		T16.count_people,
		T16.sum_pnl_t_acquring,	
		T16.crd_pos_groceries_3m_qty,
		T16.crd_pos_groceries_rub_3m_amt,
		T16.crd_pos_groceries_rub_3m_amt / (T16.crd_pos_auto_rub_3m_amt + T16.crd_pos_money_trf_rub_3m_amt + T16.crd_pos_eat_out_rub_3m_amt + T16.crd_pos_utilities_rub_3m_amt + T16.crd_pos_pc_it_rub_3m_amt + T16.crd_pos_beuaty_rub_3m_amt + T16.crd_pos_healthcare_rub_3m_amt + T16.crd_pos_clothes_rub_3m_amt + T16.crd_pos_leisure_rub_3m_amt + T16.crd_pos_groceries_rub_3m_amt + T16.crd_pos_othr_3m_amt + T16.crd_pos_special_rub_3m_amt + T16.crd_pos_telecom_rub_3m_amt + T16.crd_pos_home_repair_rub_3m_amt + T16.crd_pos_tourism_rub_3m_amt + T16.crd_pos_dept_stores_rub_3m_amt) as crd_pos_groceries_rub_3m_amt_percent,
		T16.crd_pos_eat_out_3m_qty,
		T16.crd_pos_eat_out_rub_3m_amt,
		T16.crd_pos_eat_out_rub_3m_amt / (T16.crd_pos_auto_rub_3m_amt + T16.crd_pos_money_trf_rub_3m_amt + T16.crd_pos_eat_out_rub_3m_amt + T16.crd_pos_utilities_rub_3m_amt + T16.crd_pos_pc_it_rub_3m_amt + T16.crd_pos_beuaty_rub_3m_amt + T16.crd_pos_healthcare_rub_3m_amt + T16.crd_pos_clothes_rub_3m_amt + T16.crd_pos_leisure_rub_3m_amt + T16.crd_pos_groceries_rub_3m_amt + T16.crd_pos_othr_3m_amt + T16.crd_pos_special_rub_3m_amt + T16.crd_pos_telecom_rub_3m_amt + T16.crd_pos_home_repair_rub_3m_amt + T16.crd_pos_tourism_rub_3m_amt + T16.crd_pos_dept_stores_rub_3m_amt) as crd_pos_eat_out_rub_3m_amt_percent,
		T16.crd_pos_tourism_3m_qty,
		T16.crd_pos_tourism_rub_3m_amt,
		T16.crd_pos_tourism_rub_3m_amt / (T16.crd_pos_auto_rub_3m_amt + T16.crd_pos_money_trf_rub_3m_amt + T16.crd_pos_eat_out_rub_3m_amt + T16.crd_pos_utilities_rub_3m_amt + T16.crd_pos_pc_it_rub_3m_amt + T16.crd_pos_beuaty_rub_3m_amt + T16.crd_pos_healthcare_rub_3m_amt + T16.crd_pos_clothes_rub_3m_amt + T16.crd_pos_leisure_rub_3m_amt + T16.crd_pos_groceries_rub_3m_amt + T16.crd_pos_othr_3m_amt + T16.crd_pos_special_rub_3m_amt + T16.crd_pos_telecom_rub_3m_amt + T16.crd_pos_home_repair_rub_3m_amt + T16.crd_pos_tourism_rub_3m_amt + T16.crd_pos_dept_stores_rub_3m_amt) AS crd_pos_tourism_rub_3m_amt_percent,
		T16.crd_pos_special_3m_qty,
		T16.crd_pos_special_rub_3m_amt,
		T16.crd_pos_special_rub_3m_amt / (T16.crd_pos_auto_rub_3m_amt + T16.crd_pos_money_trf_rub_3m_amt + T16.crd_pos_eat_out_rub_3m_amt + T16.crd_pos_utilities_rub_3m_amt + T16.crd_pos_pc_it_rub_3m_amt + T16.crd_pos_beuaty_rub_3m_amt + T16.crd_pos_healthcare_rub_3m_amt + T16.crd_pos_clothes_rub_3m_amt + T16.crd_pos_leisure_rub_3m_amt + T16.crd_pos_groceries_rub_3m_amt + T16.crd_pos_othr_3m_amt + T16.crd_pos_special_rub_3m_amt + T16.crd_pos_telecom_rub_3m_amt + T16.crd_pos_home_repair_rub_3m_amt + T16.crd_pos_tourism_rub_3m_amt + T16.crd_pos_dept_stores_rub_3m_amt) AS crd_pos_special_rub_3m_amt_percent,
		T16.crd_pos_auto_3m_qty,
		T16.crd_pos_auto_rub_3m_amt,
		T16.crd_pos_auto_rub_3m_amt / (T16.crd_pos_auto_rub_3m_amt + T16.crd_pos_money_trf_rub_3m_amt + T16.crd_pos_eat_out_rub_3m_amt + T16.crd_pos_utilities_rub_3m_amt + T16.crd_pos_pc_it_rub_3m_amt + T16.crd_pos_beuaty_rub_3m_amt + T16.crd_pos_healthcare_rub_3m_amt + T16.crd_pos_clothes_rub_3m_amt + T16.crd_pos_leisure_rub_3m_amt + T16.crd_pos_groceries_rub_3m_amt + T16.crd_pos_othr_3m_amt + T16.crd_pos_special_rub_3m_amt + T16.crd_pos_telecom_rub_3m_amt + T16.crd_pos_home_repair_rub_3m_amt + T16.crd_pos_tourism_rub_3m_amt + T16.crd_pos_dept_stores_rub_3m_amt) AS crd_pos_auto_rub_3m_amt_percent,

		T16.sd_gender_cd,
		T16.sd_age_yrs_comp_nv,
		T16.type_city_mlnr,
		T16.type_village,
		T16.type_city_other,
		T16.dep_inc_avg_risk_rub_amt,
		T16.dep_payroll_client_nflag,

		'�� ������. ->' as sep_,

		T18.count_people,
		T18.sum_pnl_t_invest,
		T18.inv_tot_bal_rub_amt,
		T18.inv_bo_agrmnt_open_qty,
		T18.sd_gender_cd,
		T18.sd_age_yrs_comp_nv,
		T18.type_city_mlnr,
		T18.type_village,
		T18.type_city_other,
		T18.dep_inc_avg_risk_rub_amt,
		T18.dep_payroll_client_nflag

from s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p000_no_nasp t1
join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_2 t12
on t1.Split_1=t12.split_1 and t1.year_=t12.year_ and t1.ntile_1 = t12.ntile_1
left join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_1 t11
on t11.Split_1=t12.Split_1 and t11.year_=t12.year_ and t11.ntile_1=t12.ntile_1 and t11.ntile_pnl_oi_amt_sum = t12.ntile_pnl_oi_pl_amt 
left join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_3 t13
on t11.Split_1=t13.Split_1 and t11.year_=t13.year_ and t11.ntile_1=t13.ntile_1 and t11.ntile_pnl_oi_amt_sum = t13.ntile_pnl_oi_ml_amt
left join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_4 t14
on t11.Split_1=t14.Split_1 and t11.year_=t14.year_ and t11.ntile_1=t14.ntile_1 and t11.ntile_pnl_oi_amt_sum = t14.ntile_pnl_oi_cc_amt
left join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_5 t15
on t11.Split_1=t15.Split_1 and t11.year_=t15.year_ and t11.ntile_1=t15.ntile_1 and t11.ntile_pnl_oi_amt_sum = t15.ntile_pnl_oi_dc_amt
left join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_6 t16
on t11.Split_1=t16.Split_1 and t11.year_=t16.year_ and t11.ntile_1=t16.ntile_1 and t11.ntile_pnl_oi_amt_sum = t16.ntile_SUM_pnl_T_acquring
left join s_grnplm_ld_rozn_electron_hq_core_dm.aka_client_sbp_2021_info_agg_p1_8 t18
on t11.Split_1=t18.Split_1 and t11.year_=t18.year_ and t11.ntile_1=t18.ntile_1 and t11.ntile_pnl_oi_amt_sum = t18.ntile_SUM_pnl_T_invest
where 1=1 
    and t12.ntile_pnl_oi_pl_amt is not null
    and t13.ntile_pnl_oi_ml_amt is not null
    and t14.ntile_pnl_oi_cc_amt is not null
    and t15.ntile_pnl_oi_dc_amt is not null
    and t16.ntile_SUM_pnl_T_acquring is not null
    and case 
    	when t18.Split_1 is not null
    	then t18.ntile_SUM_pnl_T_invest is not null
    	else 1=1		
    end --� �������� �� ������. ����������� ����� 5. �� ����� ����� ����� �������
order by t11.split_1, t11.ntile_1,t11.year_ desc, ntile_pnl_oi_amt_sum
    