select a,rpt_value,c,d
from oi_table oi
left join
(select
a,sum(b)/1000 as rpt_value
from winnow_rpt
where trim(process_date)='20190930'
and trim(data_src)='CARDS'
and trim(source_country_code)='AE'
and prd_segmt_cd='Cards'
 and b>0
group by psegmt) tmp
on tmp.psegmt=oi.pip_prop_segmt
left join allprod_mfu allprod
on allprod.fieldid=oi.fieldid
and allprod.product='CC'
and allprod.ods='2019_09_30_001'
where trim(oi.fieldid) in ('fieldid_3');

union all

select a,rpt_value,c,d
from oi_table oi
left join
(select
a,sum(a)/1000 as rpt_value
from winnow_rpt
where trim(process_date)='20190930'
and trim(data_src)='CARDS'
and trim(source_country_code)='AE'
and prd_segmt_cd='Cards'
 and a>0
group by psegmt) tmp
on tmp.psegmt=oi.pip_prop_segmt
left join allprod_mfu allprod
on allprod.fieldid=oi.fieldid
and allprod.product='CC'
and allprod.ods='2019_09_30_001'
where trim(oi.fieldid) in ('fieldid_2');

union all

select a,rpt_value,c,d
from oi_table oi
left join
(select
a,sum(c)/1000 as rpt_value
from winnow_rpt
where trim(process_date)='20190930'
and trim(data_src)='CARDS'
and trim(source_country_code)='AE'
and prd_segmt_cd='Cards'

group by psegmt) tmp
on tmp.psegmt=oi.pip_prop_segmt
left join allprod_mfu allprod
on allprod.fieldid=oi.fieldid
and allprod.product='CC'
and allprod.ods='2019_09_30_001'
where trim(oi.fieldid) in ('fieldid_7');