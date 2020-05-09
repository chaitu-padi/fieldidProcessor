select 'WINNOW' as use_case, null as act_bud_ind, cast('2019-09-30' as DATE) as snap_dt, Cards as pip_prod_segmt,
OI.FIELDID, '' as FIELD_DESC, OI.MEASURE, OI.RPT_UNIT, 'AED' as RPTG_cury, nvl(temp.RPT_LCY_VALUE,0) as RPT_LCY_VALUE,
COALESCE(MFU.budget_year_end, '') as budget,'' as EXCHG_RATE, '' as RPT_USD_VALUE, COALESCE(MFU.card_hudle_sign,'') as cad_hurd_sign,
'USD' as CAD_HURD_RPTUNIT, COALESCE(MFU.cad_hurdle_value,'') as CAD_HURD_RPTVALUE, 'CC' as RPTSHEET_NAME, OI.LOGIC
FROM oi_table OI
LEFT JOIN (
SELECT PIP_PROD_SEGMT , sum(a)/1000 as rpt_lcy_value
FROM winnow_rpt where trim(process_Date) = '20190930' and trim(data_src) = 'CARDS'
and source_country_code = 'AE' and report_rundate='2020032010823'
and PIP_PROD_SEGMT = 'Cards'
 and c > 10 and b>0 and a>0
group by PIP_PROD_SEGMT) TEMP
on TEMP.PIP_PROD_SEGMT = OI.PIP_PROD_SEGMT

LEFT JOIN allprod_mfu MFU
on MFU.FIELD_ID = OI.FIELD_ID
AND MFU.PRODUCT = 'CC'
and MFU.ODS='2019_09_30_001'
where OI.FIELDID='fieldid_0000'
