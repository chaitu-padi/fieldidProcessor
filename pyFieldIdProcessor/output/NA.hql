select a,b,c,d
from oi_table oi
left join allprod_mfu allprod
on allprod.fieldid=oi.fieldid
and allprod.product='CC'
and allprod.ods='2019_09_30_001'
where trim(oi.fieldid) in ('fieldid','fieldid_6','fieldid_5','fieldid_4')