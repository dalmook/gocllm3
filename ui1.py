### PART 1/3
# ui.py
import base64
import gzip
import struct
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import store
from dateutil.relativedelta import *

raw_dates = datetime.now()
m0 = (raw_dates + relativedelta(months=0)).strftime("%m")+"월"
m1 = (raw_dates + relativedelta(months=1)).strftime("%m")+"월"
m2 = (raw_dates + relativedelta(months=2)).strftime("%m")+"월"
m3 = (raw_dates + relativedelta(months=3)).strftime("%m")+"월"
m4 = (raw_dates + relativedelta(months=4)).strftime("%m")+"월"
m5 = (raw_dates + relativedelta(months=5)).strftime("%m")+"월"
m6 = (raw_dates + relativedelta(months=6)).strftime("%m")+"월"
m7 = (raw_dates + relativedelta(months=7)).strftime("%m")+"월"
m8 = (raw_dates + relativedelta(months=8)).strftime("%m")+"월"
m9 = (raw_dates + relativedelta(months=9)).strftime("%m")+"월"
# =========================
# Oracle SQL (기본)
# =========================
SQL_WARN = "SELECT SYSDATE FROM DUAL"
SQL_QUERY_1 = "SELECT SYSDATE FROM DUAL"
SQL_QUERY_2 = "SELECT SYSDATE FROM DUAL"

SQL_YIELD_BY_VERSION = """
SELECT
AA.월, AA.FAM6, AA.VERSION, AA.FABLINE, BB.MEASURE, AA.NETDIE, AA.GOODDIE,
ROUND(
  DECODE(BB.MEASURE,
    '1)TG_FL제외', AA."1)TG_FL제외",
    '2)EDS',      AA."2)EDS",
    '3)ASY',      AA."3)ASY",
    '4)TST',      AA."4)TST",
    '5)MOD',      AA."5)MOD",
    BB.MEASURE
  ) * 100, 2
) YIELD
FROM
(
  SELECT
      SUBSTR(A.YEARMONTH,3,2)||'.'||SUBSTR(A.YEARMONTH,-2)||'월' 월,
      A.FAMILY6 FAM6,
      B.VERSION,
      A.LINE_CODE FABLINE,
      B.NETDIE NETDIE,
      ROUND(NVL(SUM(CASE WHEN A.AREA = 'CUM' THEN A.ADJ_VALUE*B.NETDIE END),0),0) "GOODDIE",
      NVL(SUM(CASE WHEN A.AREA = 'CUM' THEN A.ADJ_VALUE END),0) "1)TG_FL제외",
      NVL(SUM(CASE WHEN A.AREA = 'EDS' THEN A.ADJ_VALUE END),0) "2)EDS",
      NVL(SUM(CASE WHEN A.AREA = 'ASY' THEN A.ADJ_VALUE END),0) "3)ASY",
      NVL(SUM(CASE WHEN A.AREA = 'TST' THEN A.ADJ_VALUE END),0) "4)TST",
      NVL(SUM(CASE WHEN A.AREA = 'MOD' THEN A.ADJ_VALUE END),0) "5)MOD"
  FROM
      mempm_sdb.exp_yield a,
      (
        SELECT
          ITEM FAMILY6, LEVEL6 FAMILY1, DR, VERSION,
          max(NETDIE) netdie, max(DENSITY) density, max(EQQTY) eqqty
        FROM MEMPM_SDB.EXP_ITEM
        WHERE ITEM_STAGE = 'FAM6'
          AND PLAN_VERSION = (SELECT MAX(PLAN_VERSION) FROM MEMPM_SDB.EXP_ITEM)
          AND netdie > 0
        GROUP BY ITEM, LEVEL6, DR, VERSION
      ) B
  WHERE 1=1
    AND SUBSTR(A.PLAN_VERSION, 1, 8) = (SELECT MAX(SUBSTR(PLAN_VERSION, 1,8)) FROM mempm_sdb.exp_yield)
    AND A.FAMILY6 = B.family6 (+)
    AND A.AREA NOT IN ('TTL CUM','FAB')
    AND B.VERSION LIKE :version
  GROUP BY
      SUBSTR(A.YEARMONTH,3,2)||'.'||SUBSTR(A.YEARMONTH,-2)||'월',
      A.FAMILY6,
      B.VERSION,
      A.LINE_CODE,
      B.NETDIE
) AA,
(
  SELECT '1)TG_FL제외' MEASURE FROM DUAL UNION ALL
  SELECT '2)EDS' MEASURE FROM DUAL UNION ALL
  SELECT '3)ASY' MEASURE FROM DUAL UNION ALL
  SELECT '4)TST' MEASURE FROM DUAL UNION ALL
  SELECT '5)MOD' MEASURE FROM DUAL
) BB
"""

SQL_CODE_FINDER = """
WITH version as (
SELECT
AA.ITEM,
AA.fam6_code,
DECODE(NVL(BB.VER,DD.DRAM_VER),'-',SUBSTR(AA.FAM6_CODE,-3,2),NVL(BB.VER,DD.DRAM_VER)) DRAMVER,
NVL(CC.VER,DD.NAND_VER) NANDVER
FROM 
(select
a.SALES_CODE ITEM,
a.fam6_code
from V_IF_MDM_C_BS_CM_SALES_FAMILY a
) AA, (select A.item, max(version) ver from MST_PAX_ITEM a where 1=1 group by A.item) BB, (SELECT PARTNO ITEM, MAX(VERSION) VER  FROM MST_PKGMASTERFLASH A  WHERE 1=1  AND A.ISVALID = 'Y'  GROUP BY PARTNO) CC
,(SELECT DISTINCT ITEM,DRAM_VER, NAND_VER FROM MEMPM_SDB.EXP_ITEM A WHERE 1=1 AND a.plan_version  = (select max(plan_version) from  MEMPM_SDB.EXP_ITEM )) DD
WHERE AA.item = BB.ITEM (+)
AND AA.item = CC.ITEM (+)
AND AA.item = DD.ITEM (+)
),
MOQ AS 
(select
a.item,
a.lboxqty,
a.sboxqty,
a.lboxqty * a.sboxqty moq
from tar_moq a
where 1=1
and a.gbm = 'MEM'
GROUP BY
a.item,
a.lboxqty,
a.sboxqty,
a.lboxqty * a.sboxqty)
       SELECT
           AA.ITEM,
           AA.DENSITY,
           AA.APPLICATION,
           AA.PRODUCT,
           AA.MODEL,
           MAX(AA.DRAMVER) DRAMVER,
           MAX(NVL(AA.NANDVER,BB.NANDVER)) NANDVER,
           AA.MULTIGUBUN,
           AA.FF,
           AA.IF, 
           AA.CATEGORY1,
           AA.CATEGORY2,
           AA.CONTROLLER,
           AA.NANDDR, 
           AA.BIT,
           AA.NANDDEN,
           AA.NANDCOMPEQ,
           AA.NANDCOMP,
           AA.NANDEQ,
           AA.DRAMDR,
           AA.DRAMSUB, 
           AA.DRAMDEN,
           AA.DRAMCOMPEQ,
           AA.DRAMCOMP,
           AA.DRAMEQ,
           CC.LBOXQTY,
           CC.SBOXQTY,
           CC.MOQ
       FROM
           (SELECT
               A.ITEM,
               A.DENSITY,
               A.APPLICATION,
               A.PRODUCT,
               A.MODEL,
               B.DRAMVER,
               '' NANDVER,
               A.MULTIGUBUN,
               A.FF,
               A.IF, 
               A.CATEGORY1,
               A.CATEGORY2,
               A.CONTROLLER,
               A.NANDDR, 
               A.BIT,
               A.NANDDENSITY NANDDEN,
               A.NANDCOMPEQQTY NANDCOMPEQ,
               A.NANDCOMPQTY NANDCOMP,
               A.NANDEQQTY NANDEQ,
               A.DRAMDR,
               A.DRAMSUBPRODUCT DRAMSUB, 
               A.DRAMDENSITY DRAMDEN,
               A.DRAMCOMPEQQTY DRAMCOMPEQ,
               A.DRAMCOMPQTY DRAMCOMP,
               A.DRAMEQQTY DRAMEQ
           FROM
               MST_PAX_ITEM A
               LEFT JOIN VERSION B ON A.ITEM = B.ITEM
           UNION ALL
           SELECT
               A.ITEM,
               A.DENSITY,
               A.APPLICATION,
               A.PRODUCT,
               A.MODEL,
               '' DRAMVER,
               C.VERSION,
               A.MULTIGUBUN,
               A.FF,
               A.IF, 
               A.CATEGORY1,
               A.CATEGORY2,
               A.CONTROLLER,
               A.NANDDR, 
               A.BIT,
               A.NANDDENSITY NANDDEN,
               A.NANDCOMPEQQTY NANDCOMPEQ,
               A.NANDCOMPQTY NANDCOMP,
               A.NANDEQQTY NANDEQ,
               A.DRAMDR,
               A.DRAMSUBPRODUCT DRAMSUB, 
               A.DRAMDENSITY DRAMDEN,
               A.DRAMCOMPEQQTY DRAMCOMPEQ,
               A.DRAMCOMPQTY DRAMCOMP,
               A.DRAMEQQTY DRAMEQ
           FROM
               MST_PAX_ITEM A
               LEFT JOIN MST_WHINMASTERFLASH C ON A.ITEM = C.SALESCODE
           ) AA LEFT JOIN VERSION BB ON AA.ITEM = BB.ITEM
           LEFT OUTER JOIN MOQ CC ON AA.ITEM = CC.ITEM
       WHERE
           AA.ITEM LIKE :item
           AND ROWNUM <= 100
       GROUP BY
           AA.ITEM,
           AA.DENSITY,
           AA.APPLICATION,
           AA.PRODUCT,
           AA.MODEL,
           AA.MULTIGUBUN,
           AA.FF,
           AA.IF, 
           AA.CATEGORY1,
           AA.CATEGORY2,
           AA.CONTROLLER,
           AA.NANDDR, 
           AA.BIT,
           AA.NANDDEN,
           AA.NANDCOMPEQ,
           AA.NANDCOMP,
           AA.NANDEQ,
           AA.DRAMDR,
           AA.DRAMSUB, 
           AA.DRAMDEN,
           AA.DRAMCOMPEQ,
           AA.DRAMCOMP,
           AA.DRAMEQ,
           CC.LBOXQTY,
           CC.SBOXQTY,
           CC.MOQ    

"""

SQL_IPGO = """
WITH version as (
SELECT
AA.ITEM,
AA.fam6_code,
DECODE(NVL(BB.VER,DD.DRAM_VER),'-',SUBSTR(AA.FAM6_CODE,-3,2),NVL(BB.VER,DD.DRAM_VER)) DRAMVER,
NVL(CC.VER,DD.NAND_VER) NANDVER
FROM 
(select
a.SALES_CODE ITEM,
a.fam6_code
from V_IF_MDM_C_BS_CM_SALES_FAMILY a
) AA, (select A.item, max(version) ver from MST_PAX_ITEM a where 1=1 group by A.item) BB, (SELECT PARTNO ITEM, MAX(VERSION) VER  FROM MST_PKGMASTERFLASH A  WHERE 1=1  AND A.ISVALID = 'Y'  GROUP BY PARTNO) CC
,(SELECT DISTINCT ITEM,DRAM_VER, NAND_VER FROM MEMPM_SDB.EXP_ITEM A WHERE 1=1 AND a.plan_version  = (select max(plan_version) from  MEMPM_SDB.EXP_ITEM )) DD
WHERE AA.item = BB.ITEM (+)
AND AA.item = CC.ITEM (+)
AND AA.item = DD.ITEM (+)
)
-------------
SELECT
  AA.ATTB13,
  case when AA.ATTB13 = 'FLASH' AND AA.PRODUCT NOT IN ('eMCP','uMCP') THEN CC.VERSION ELSE BB.DRAMVER END VERSION,
  AA.GUIDEFAMILY,
  to_char(SUM(AA.M0PLAN),'FM999,999,999') "{} PLAN(KPKG)",
  to_char(SUM(AA.M0ACT),'FM999,999,999') "{} ACT",
  to_char(SUM(AA.remain),'FM999,999,999') remain, 
  to_char(SUM(AA.M1PLAN),'FM999,999,999') "{}",
  to_char(SUM(AA.M2PLAN),'FM999,999,999') "{}",
  to_char(SUM(AA.M3PLAN),'FM999,999,999') "{}",
  to_char(SUM(AA.M4PLAN),'FM999,999,999') "{}",
  to_char(SUM(AA.M5PLAN),'FM999,999,999') "{}"
FROM
(
SELECT
  A.ATTB13,
  A.PRODUCT,
  A.GUIDEFAMILY,
  MAX(A.ITEM) REF_CODE,
  ROUND(NVL(sum(case when a.month = TO_CHAR(ADD_MONTHS(SYSDATE - 1, 0), 'yyyymm') then A.PLANQTY end)/1000,0)) M0PLAN,
  ROUND(NVL(sum(case when a.month = TO_CHAR(ADD_MONTHS(SYSDATE - 1, 0), 'yyyymm') then A.ACTUALQTY end)/1000,0)) M0ACT,
  ROUND(NVL(sum(case when a.month = TO_CHAR(ADD_MONTHS(SYSDATE - 1, 0), 'yyyymm') then A.ACTUALQTY end)/1000,0))-
  ROUND(NVL(sum(case when a.month = TO_CHAR(ADD_MONTHS(SYSDATE - 1, 0), 'yyyymm') then A.PLANQTY end)/1000,0)) remain, 
  ROUND(NVL(sum(case when a.month = TO_CHAR(ADD_MONTHS(SYSDATE - 1, 1), 'yyyymm') then A.PLANQTY end)/1000,0)) M1PLAN,
  ROUND(NVL(sum(case when a.month = TO_CHAR(ADD_MONTHS(SYSDATE - 1, 2), 'yyyymm') then A.PLANQTY end)/1000,0)) M2PLAN,
  ROUND(NVL(sum(case when a.month = TO_CHAR(ADD_MONTHS(SYSDATE - 1, 3), 'yyyymm') then A.PLANQTY end)/1000,0)) M3PLAN,
  ROUND(NVL(sum(case when a.month = TO_CHAR(ADD_MONTHS(SYSDATE - 1, 4), 'yyyymm') then A.PLANQTY end)/1000,0)) M4PLAN,
  ROUND(NVL(sum(case when a.month = TO_CHAR(ADD_MONTHS(SYSDATE - 1, 5), 'yyyymm') then A.PLANQTY end)/1000,0)) M5PLAN
FROM
  GUI_CPLN_PROD_PROGRESS A
WHERE
  1 = 1
  AND A.CATEGORY = 'PRODUCTION'
  AND A.PLANID = TO_CHAR(SYSDATE, 'iyyyiw') --'202413'                                  
  AND A.WORKDATE = TO_CHAR(SYSDATE, 'yyyymmdd') --'20240325'  
  AND A.MONTH >= TO_CHAR(ADD_MONTHS(SYSDATE - 1, 0), 'yyyymm')   
GROUP BY
 A.ATTB13,A.GUIDEFAMILY,A.PRODUCT
  ) AA,VERSION BB,MST_WHINMASTERFLASH CC
  WHERE 1=1
  AND AA.REF_CODE = BB.ITEM (+)
  and AA.REF_CODE = CC.salescode (+)
  AND case when AA.ATTB13 = 'FLASH' AND AA.PRODUCT NOT IN ('eMCP','uMCP') THEN CC.VERSION ELSE BB.DRAMVER END LIKE :item
GROUP BY
  rollup((AA.ATTB13,
  case when AA.ATTB13 = 'FLASH' AND AA.PRODUCT NOT IN ('eMCP','uMCP') THEN CC.VERSION ELSE BB.DRAMVER END,
  AA.GUIDEFAMILY))
  """.format(m0,m0,m1,m2,m3,m4,m5)  

SQL_RIGHTPERSON_ORACLE = """
SELECT DISTINCT
  AA.부서,
  LISTAGG(AA.담당제품, ',') WITHIN GROUP (ORDER BY AA.담당제품)
    OVER (PARTITION BY AA.실무담당자, AA.팀장, AA.PL, AA.TL) AS 담당제품,
  AA.팀장, AA.PL, AA.TL, AA.실무담당자,
  AA.비고
FROM (
  select DISTINCT
    '공급망운영' 부서,
    PRODUCT_NAME 담당제품,
    '정성원' 팀장,
    '이준철' PL,
    case
      when a.prod_part = 'MOBILE파트' then '김수연'
      when a.prod_part = 'FLASH파트' then '김천'
      when a.prod_part = 'EDP파트' then '한선옥'
      else 'none'
    end TL,
    a.PROD_USER_NAME 실무담당자,
    '' 비고
  from MTA_BI_PLANNER_MASTER a
  WHERE 1=1
    AND a.gubun = 'WHS'
) AA
"""

SQL_ONEVIEW_SHIP_BASE = """
WITH version as (
  SELECT
    AA.ITEM,
    AA.fam6_code,
    DECODE(NVL(BB.VER,DD.DRAM_VER),'-',SUBSTR(AA.FAM6_CODE,-3,2),NVL(BB.VER,DD.DRAM_VER)) DRAMVER,
    NVL(CC.VER,DD.NAND_VER) NANDVER
  FROM
    (select a.SALES_CODE ITEM, a.fam6_code from V_IF_MDM_C_BS_CM_SALES_FAMILY a) AA,
    (select A.item, max(version) ver from MST_PAX_ITEM a group by A.item) BB,
    (SELECT PARTNO ITEM, MAX(VERSION) VER FROM MST_PKGMASTERFLASH A WHERE A.ISVALID='Y' GROUP BY PARTNO) CC,
    (SELECT DISTINCT ITEM,DRAM_VER, NAND_VER FROM MEMPM_SDB.EXP_ITEM
      WHERE plan_version=(select max(plan_version) from MEMPM_SDB.EXP_ITEM)) DD
  WHERE AA.item = BB.ITEM (+)
    AND AA.item = CC.ITEM (+)
    AND AA.item = DD.ITEM (+)
)
SELECT
  NVL(SUBSTR(a.ha_date,1,6),'TTL') hamonth,
  NVL(A.NG1,' ') NG1,
  NVL(A.NG2,' ') NG2,
  NVL(A.NG3,' ') NG3,
  NVL(b.dramver,' ') dramver,
  NVL(b.nandver,' ') nandver,
  NVL(A.SALE_ITEM_CODE,' ') SALE_ITEM_CODE,
  NVL(CASE WHEN a.sample_no like 'S%' THEN 'SAMPLE' ELSE '-' END,' ') SAMPLE,
  NVL(A.GC_CODE,' ') GC_CODE,
  NVL(A.GC_NAME,' ') GC_NAME,
  NVL(A.WH_NUM,' ') WH_NUM,
  NVL(A.INCO1_BR,' ') INCO1_BR,
  NVL(A.NET_VALUE_USD,' ') NET_PRICE,
  TO_CHAR(SUM(A.DLVRY_QTY),'FM999,999,999') QTY,
  TO_CHAR(SUM(A.AMT_BY_USD)/1000,'FM999,999,999') KAMT,
  SUM(A.CARTON_NO) BOX
FROM tar_dw_d_dlvry_oneview a
JOIN version b ON a.sale_item_code = b.item
WHERE 1=1
  AND a.corp_code in ('U001')
  AND a.sale_doc_code like '%Y%'
  AND a.sale_doc_code not like '%YF01%'
  AND a.sold_to <> 'C6S1-00'
  AND SUBSTR(A.HA_DATE,1,6) BETWEEN :smon AND :emon
  AND {filter_clause}
GROUP BY
  ROLLUP((
    SUBSTR(a.ha_date,1,6),
    A.NG1, A.NG2, A.NG3,
    b.dramver, b.nandver,
    A.SALE_ITEM_CODE,
    CASE WHEN a.sample_no like 'S%' THEN 'SAMPLE' ELSE '-' END,
    A.GC_CODE, A.GC_NAME,
    A.WH_NUM, A.INCO1_BR,
    A.NET_VALUE_USD
  ))
ORDER BY
  SUBSTR(a.ha_date,1,6) DESC,
  A.NG1, A.NG2, A.NG3,
  A.SALE_ITEM_CODE,
  CASE WHEN a.sample_no like 'S%' THEN 'SAMPLE' ELSE '-' END,
  A.GC_CODE, A.GC_NAME,
  A.WH_NUM, A.INCO1_BR
"""

# (바로 위 코드 1줄 - 찾기용)
SQL_PKGCODE_BASE = """
WITH PCBQTY AS (
  SELECT
    AA.MATERIAL,
    AA.SITE,
    SUM(AA.STOCKQTY) STOCKQTY,
    SUM(AA.LINEQTY)  LINEQTY,
    SUM(AA.POREMAIN) POREMAIN
  FROM (
    SELECT
      A.MATNR AS MATERIAL,
      A.SITE,
      SUM(STOCKQTY) STOCKQTY,
      SUM(LINEQTY)  LINEQTY,
      0 POREMAIN
    FROM (
      SELECT
        DECODE(WERKS, 'P1C2', 'OY', 'P6C1', 'SU', 'P690', 'XA', 'ZA', 'OY', WERKS) AS SITE,
        MATNR,
        'N' AS INFINITE,
        0 AS STOCKQTY,
        STKQTY AS LINEQTY
      FROM TAR_AGINGRPTIF
      WHERE KYDAT = TO_CHAR(SYSDATE - 1, 'YYYYMMDD')
        AND BKLAS IN ('9999')
        AND MATNR LIKE 'LA41%'
      UNION ALL
      SELECT
        DECODE(WERKS, 'P1C2', 'OY', 'P6C1', 'SU', 'P690', 'XA', 'ZA', 'OY', WERKS) AS SITE,
        MATNR,
        'N' AS INFINITE,
        STKQTY AS STOCKQTY,
        0 AS LINEQTY
      FROM TAR_AGINGRPTIF
      WHERE KYDAT = TO_CHAR(SYSDATE - 1, 'YYYYMMDD')
        AND BKLAS IN ('3000','8000')
        AND MATNR LIKE 'LA41%'
    ) A
    GROUP BY A.MATNR, A.SITE
    HAVING SUM(STOCKQTY) + SUM(LINEQTY) > 0

    UNION ALL
    SELECT
      MATNR AS MATERIAL,
      DECODE(WERKS, 'P1C2', 'OY', 'P6C1', 'SU', 'P690', 'XA', 'ZA', 'OY', WERKS) AS SITE,
      0 STOCKQTY,
      0 LINEQTY,
      SUM(RMQTY) QIQTY
    FROM MTA_MATPOREMAIN
    WHERE WORKDATE = TO_CHAR(SYSDATE, 'YYYYMMDD')
      AND MATNR LIKE 'LA41%'
    GROUP BY MATNR,
      DECODE(WERKS, 'P1C2', 'OY', 'P6C1', 'SU', 'P690', 'XA', 'ZA', 'OY', WERKS)
  ) AA
  GROUP BY AA.MATERIAL, AA.SITE
),
INFO AS (
  SELECT DISTINCT
    A.DBTABLENAME,
    SUBSTR(A.F13,-3,2) VERSION,
    A.F18,
    A.F17,
    A.F1  DR,
    A.F24 DENSITY,
    B.PCBCODE,
    B.LINE,
    B.STOCK,
    B.POREMAIN
  FROM TAR_ASSYSPEC_SEMI A,
  (
    SELECT
      SUBSTR(A.PROD_CODE,-3) PKGCODE,
      MAX(A.PIECE_PART_NO)  PCBCODE,
      MAX(B.STOCKQTY)       STOCK,
      MAX(B.LINEQTY)        LINE,
      MAX(B.POREMAIN)       POREMAIN
    FROM TAR_MATSPARTBOM A, PCBQTY B
    WHERE SUBSTR(A.PIECE_PART_NO,1,4) IN ('LA14','LA41','LA47')
      AND A.DEL_FLAG = 'N'
      AND A.PROD_CODE LIKE 'K%'
      AND A.PIECE_PART_NO = B.MATERIAL (+)
    GROUP BY SUBSTR(A.PROD_CODE,-3)
  ) B
  WHERE 1=1
    AND A.F18 = B.PKGCODE (+)
    AND A.F24 IS NOT NULL
    AND A.F18 IS NOT NULL
)
SELECT
  B.VERSION,
  A.PACK_CODE,
  B.DR,
  B.PCBCODE,
  TO_CHAR(B.LINE,'FM999,999,999')    LINE,
  TO_CHAR(B.STOCK,'FM999,999,999')   STOCK,
  TO_CHAR(B.POREMAIN,'FM999,999,999') POREMAIN,
  B.DENSITY,
  A.PKG_NAME,
  A.REMARK,
  A.BODY_SIZE,
  A.TERMINAL_PITCH,
  A.PKG_SPEC
FROM TAR_BSCMPACKAGEMASTER A, INFO B
WHERE 1=1
  AND A.PACK_CODE = B.F18 (+)
  AND A.DEL_FLAG = 'N'
  AND {where_clause}
"""

SQL_PS_COMP_BASE = """
WITH version as (
SELECT
AA.ITEM,
SUBSTR(AA.item,1,INSTR(AA.item,'-',1)-1) item2,
AA.fam6_code,
DECODE(NVL(BB.VER,DD.DRAM_VER),'-',SUBSTR(AA.FAM6_CODE,-3,2),NVL(BB.VER,DD.DRAM_VER)) DRAMVER,
NVL(CC.VER,DD.NAND_VER) NANDVER
FROM 
(select
a.SALES_CODE ITEM,
a.fam6_code
from V_IF_MDM_C_BS_CM_SALES_FAMILY a
) AA, (select A.item, max(version) ver from MST_PAX_ITEM a where 1=1 group by A.item) BB, (SELECT PARTNO ITEM, MAX(VERSION) VER  FROM MST_PKGMASTERFLASH A  WHERE 1=1  AND A.ISVALID = 'Y'  GROUP BY PARTNO) CC
,(SELECT DISTINCT ITEM,DRAM_VER, NAND_VER FROM MEMPM_SDB.EXP_ITEM A WHERE 1=1 AND a.plan_version  = (select max(plan_version) from  MEMPM_SDB.EXP_ITEM )) DD
WHERE AA.item = BB.ITEM (+)
AND AA.item = CC.ITEM (+)
AND AA.item = DD.ITEM (+)
)
--------
SELECT
*
FROM
(SELECT
B.DRAMVER,
B.NANDVER,
A.FOUT_CODE FAB_CODE,
A.EFU_CODE EDS_CODE,
A.ABD_CODE ASY_CODE,
A.TFN_CODE TST_CODE
FROM MEMPM_SDB.PMC_CODE_ROUTE A, VERSION B
WHERE 1=1
and SUBSTR(A.TFN_CODE,1,INSTR(A.TFN_CODE,'-',1)-1) = b.item2 (+)
AND A.TFN_CODE NOT LIKE 'KM%'
AND {where_clause}
GROUP BY
B.DRAMVER,
B.NANDVER,
A.FOUT_CODE,
A.EFU_CODE,
A.ABD_CODE,
A.TFN_CODE
ORDER BY
A.FOUT_CODE,
A.EFU_CODE,
A.ABD_CODE,
A.TFN_CODE) AA
WHERE 1=1
"""

SQL_PS_MODULE_BASE = """
SELECT
B.FAB_CODE, B.EFU_CODE EDS_CODE, B.ABD_CODE ASY_CODE, A.COMPCODE, A.PRODCODE
FROM TAR_MODULEBOM A,(SELECT SUBSTR(TFN_CODE,1,18) TFN_CODE, ABD_CODE, EFU_CODE, FOUT_CODE FAB_CODE FROM MEMPM_SDB.PMC_CODE_ROUTE GROUP BY SUBSTR(TFN_CODE,1,18), ABD_CODE, EFU_CODE, FOUT_CODE) B
WHERE 1=1
AND A.COMPCODE = B.TFN_CODE (+)
AND {where_clause}
GROUP BY
B.FAB_CODE, B.EFU_CODE, B.ABD_CODE, A.COMPCODE, A.PRODCODE
ORDER BY 
B.FAB_CODE, B.EFU_CODE, B.ABD_CODE, A.COMPCODE, A.PRODCODE
"""

SQL_PS_MCP_BASE = """
WITH version as (
SELECT
AA.ITEM,
SUBSTR(AA.item,1,INSTR(AA.item,'-',1)-1) item2,
AA.fam6_code,
DECODE(NVL(BB.VER,DD.DRAM_VER),'-',SUBSTR(AA.FAM6_CODE,-3,2),NVL(BB.VER,DD.DRAM_VER)) DRAMVER,
NVL(CC.VER,DD.NAND_VER) NANDVER
FROM 
(select
a.SALES_CODE ITEM,
a.fam6_code
from V_IF_MDM_C_BS_CM_SALES_FAMILY a
) AA, (select A.item, max(version) ver from MST_PAX_ITEM a where 1=1 group by A.item) BB, (SELECT PARTNO ITEM, MAX(VERSION) VER  FROM MST_PKGMASTERFLASH A  WHERE 1=1  AND A.ISVALID = 'Y'  GROUP BY PARTNO) CC
,(SELECT DISTINCT ITEM,DRAM_VER, NAND_VER FROM MEMPM_SDB.EXP_ITEM A WHERE 1=1 AND a.plan_version  = (select max(plan_version) from  MEMPM_SDB.EXP_ITEM )) DD
WHERE AA.item = BB.ITEM (+)
AND AA.item = CC.ITEM (+)
AND AA.item = DD.ITEM (+)
)
-----------
SELECT
E.DRAMVER,E.NANDVER,D.FAB_CODE, AA.EDS_CODE, AA.SEQUENCE, AA.CONSUMEQTY, AA.ASYIN_CODE, AA.ASYOUT_CODE, AA.TST_CODE
FROM
(
SELECT
B.FOUT_CODE FAB_CODE, A.CHIPCODE EDS_CODE, A.SEQUENCE, A.CONSUMEQTY, A.ASSYCODE ASYIN_CODE, A.PRODCODE ASYOUT_CODE, C.TFN_CODE TST_CODE 

FROM TAR_MULTIBOM a, MEMPM_SDB.PMC_CODE_ROUTE B, TAR_PSROUTE C
where 1=1 
AND (C.TFN_CODE like 'K5%' OR C.TFN_CODE like 'KM%')
AND {where_clause}
AND A.ASSYCODE = B.ABD_CODE (+)
AND A.PRODCODE = C.ABD_CODE (+)
) AA, TAR_PSROUTE D,VERSION E
WHERE 1=1
AND AA.EDS_CODE = D.EFU_CODE (+)
AND SUBSTR(AA.EDS_CODE,1,10) = E.ITEM2 (+)
GROUP BY
E.DRAMVER,E.NANDVER,D.FAB_CODE, AA.EDS_CODE, AA.SEQUENCE, AA.CONSUMEQTY, AA.ASYIN_CODE, AA.ASYOUT_CODE, AA.TST_CODE
ORDER BY
AA.SEQUENCE
"""

def _likeify(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    return v if ("%" in v or "_" in v) else f"%{v}%"

# --- Query Registry ---
QUERY_REGISTRY: Dict[str, Dict[str, Any]] = {
    "QUERY_1": {
        "title": "조회 1",
        "run_action": "QUERY_1",
        "form_action": None,
        "sql": SQL_QUERY_1,
        "fields": [],
        "params_builder": None,
        "output": "MSG7_TABLE",
    },
    "QUERY_2": {
        "title": "조회 2",
        "run_action": "QUERY_2",
        "form_action": None,
        "sql": SQL_QUERY_2,
        "fields": [],
        "params_builder": None,
        "output": "MSG7_TABLE",
    },
    "YIELD_BY_VERSION": {
        "title": "수율 조회(Version)",
        "form_action": "YIELD_FORM",
        "run_action":  "YIELD_RUN",
        "sql": SQL_YIELD_BY_VERSION,
        "fields": [
            {"id":"version","label":"VERSION","placeholder":"예: VTB 또는 %VTB%","required":True}
        ],
        "params_builder": lambda payload: {"version": _likeify(payload.get("version", "")).upper()},
        "output": "MSG7_TABLE",
    },
    "CODE_FINDER": {
        "title": "코드조회",
        "form_action": "CODE_FINDER_FORM",
        "run_action":  "CODE_FINDER_RUN",
        "sql": SQL_CODE_FINDER,
        "fields": [
            {"id":"item","label":"ITEM","placeholder":"예: 파트넘버 KLM%","required":True}
        ],
        "params_builder": lambda payload: {"item": _likeify(payload.get("item", "")).upper()},
        "output": "MSG7_TABLE",
    },
    "IPGO": {
        "title": "입고계획",
        "form_action": "IPGO_FORM",
        "run_action":  "IPGO_RUN",
        "sql": SQL_IPGO,
        "fields": [
            {"id":"item","label":"VERSION","placeholder":"예: VERSION VL","required":True}
        ],
        "params_builder": lambda payload: {"item": _likeify(payload.get("item", "")).upper()},
        "output": "MSG7_TABLE",
    },
    "RIGHTPERSON": {
        "title": "담당조회",
        "form_action": "RIGHTPERSON_FORM",
        "run_action":  "RIGHTPERSON_RUN",
        "runner": "RIGHTPERSON",
        "edit_url": "http://gocinfo.samsungds.net:7000/",  # ✅ 추가
        "fields": [
            {"id":"q","label":"검색어","placeholder":"예: MOBILE / 조경원 / 공급망운영 / HBM ...", "required": True}
        ],
        "params_builder": lambda payload: {"q": (payload.get("q", "") or "").strip()},
        "output": "MSG7_TABLE",
    },
    "TERM": {
        "title": "용어검색",
        "form_action": "TERM_FORM",
        "run_action":  "TERM_RUN",
        "runner": "TERM_SEARCH",
        "fields": [
            {"id":"q","label":"검색어","placeholder":"예: PO / COF / PSI / MOQ ...", "required": True}
        ],
        "params_builder": lambda payload: {"q": (payload.get("q", "") or "").strip()},
        "output": "ADAPTIVE_CARD",   # ✅ 결과는 표가 아니라 카드로 보냄
    },

    "SHIP": {
        "title": "출하조회",
        "form_action": "SHIP_FORM",
        "run_action":  "SHIP_RUN",
        "runner": "ONEVIEW_SHIP",
        "form_type": "ONEVIEW_SHIP",  # ✅ build_query_form_card에서 커스텀 폼 렌더
        "fields": [
            {"id":"hastartmon01","label":"조회 시작월(YYYYMM)","required": True},
            {"id":"haendmon01",  "label":"조회 종료월(YYYYMM)","required": True},
            {"id":"hachoiceset01","label":"조회기준","required": True},
            {"id":"hainputtext01","label":"검색값","required": True},
        ],
        "params_builder": lambda payload: {
            "smon": (payload.get("hastartmon01","") or "").strip(),
            "emon": (payload.get("haendmon01","") or "").strip(),
            "conv": (payload.get("hachoiceset01","") or "").strip(),
            "q":    (payload.get("hainputtext01","") or "").strip(),
        },
        "output": "MSG7_TABLE",
    },  
    "PKGCODE": {
    "title": "PKGCODE 조회",
    "form_action": "PKGCODE_FORM",
    "run_action":  "PKGCODE_RUN",
    "runner": "PKGCODE",
    "form_type": "PKGCODE",
    "fields": [
        {"id":"pkgcode3341","label":"검색어","placeholder":"ex) WL / 9N7 / LA41-12223A", "required": True}
    ],
    "params_builder": lambda payload: {"q": (payload.get("pkgcode3341","") or "").strip()},
    "output": "MSG7_TABLE",
  },
  # ✅ (추가) PS 조회
    "PS": {
        "title": "PS조회",
        "form_action": "PS_FORM",
        "run_action":  "PS_RUN",
        "runner": "PS_QUERY",
        "form_type": "PS",
        "fields": [
            {"id":"psgubun01","label":"코드구분(COMP/MODULE/MCP)","required": True},
            {"id":"psconv01","label":"조회기준(FAB/EDS/ASY/TST/MOD)","required": True},
            {"id":"psver01","label":"코드입력","required": True},
        ],
        "params_builder": lambda payload: {
            "gubun": (payload.get("psgubun01","") or "").strip(),
            "conv":  (payload.get("psconv01","") or "").strip(),
            "q":     (payload.get("psver01","") or "").strip(),
        },
        "output": "MSG7_TABLE",
    },

}

ACTION_TO_QUERY: Dict[str, Tuple[str, str]] = {}
for qk, spec in QUERY_REGISTRY.items():
    if spec.get("form_action"):
        ACTION_TO_QUERY[spec["form_action"]] = ("FORM", qk)
    if spec.get("run_action"):
        ACTION_TO_QUERY[spec["run_action"]] = ("RUN", qk)

# =========================
# msgType=7 CSV payload
# =========================
def format_df_brief(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df is None or df.empty:
        return "조회 결과: 0건"
    return df.head(max_rows).to_string(index=False)

def df_to_knox_csv_msg7(df: pd.DataFrame, title: str = "조회 결과") -> str:
    if df is None or df.empty:
        table_html = '<table border="1" class="dataframe"><thead><tr><th>Result</th></tr></thead><tbody><tr><td>조회 결과: 0건</td></tr></tbody></table>'
    else:
        table_html = df.to_html(index=False, border=1, classes="dataframe")

    html = (
        "<style>"
        "th {background-color:#BBE9F0; font-size:13px; text-align:center;}"
        "td {font-size:13px; text-align:center;}"
        "table {border-collapse:collapse;}"
        "</style>"
        f'<div style="overflow:auto; width: 2200px; font-size:13px; display:flex; text-align:center;">'
        f"{table_html}"
        "</div>"
    )

    raw = html.encode("utf-8")
    prefix = struct.pack("<I", len(raw))
    gz = gzip.compress(raw)
    payload = prefix + gz
    b64 = base64.b64encode(payload).decode("ascii")

    header = '<!-- {"COMMAND":"SNDCL", "SNDCL":{"KND":"CLDT", "TYPE":"CSV"}} -->'
    return header + b64

# =========================
# Dashboard HTML
