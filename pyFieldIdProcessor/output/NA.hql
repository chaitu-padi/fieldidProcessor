select a,b,c,d
from oi_table oi
left join allprod_mfu allprod
on allprod.fieldid=oi.fieldid
and allprod.product='CC'
and allprod.ods='2019_09_30_001'
where trim(oi.fieldid) in ('fieldid_0001','fieldid_0006','fieldid_0004','fieldid_0005','fieldid_2004','fieldid_2005','fieldid_2001','fieldid_2002','fieldid_2003')