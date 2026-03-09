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
# =========================
DASHBOARD_LOGIN_HTML = r"""
<!doctype html><html lang="ko"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>__DASHBOARD_TITLE__</title>
<style>
body{font-family:system-ui,Segoe UI,Apple SD Gothic Neo,Malgun Gothic,sans-serif;margin:0;background:#0b1020;color:#e8ecff}
.wrap{max-width:520px;margin:0 auto;padding:28px}
.card{margin-top:64px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.10);border-radius:16px;padding:18px}
.title{font-size:20px;font-weight:900;margin-bottom:8px}
.small{font-size:12px;opacity:.85}
input{width:100%;margin-top:12px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14);color:#e8ecff;border-radius:12px;padding:12px 12px;font-size:14px}
button{margin-top:12px;width:100%;background:#4aa3ff;border:0;color:#061024;border-radius:12px;padding:12px 12px;font-weight:900;cursor:pointer}
.err{margin-top:10px;color:#ffb3b4;font-size:13px;min-height:18px}
</style></head><body>
<div class="wrap"><div class="card">
<div class="title">🔐 대시보드 접근 코드 _ 조성묵에게 문의</div>
<div class="small">코드를 입력해야 대시보드로 이동합니다.</div>
<input id="tok" type="password" placeholder="접근 코드 입력" autocomplete="off"/>
<button onclick="go()">접속</button>
<div class="err" id="err"></div>
<div class="small" style="margin-top:10px;opacity:.7;">* 코드가 틀리면 대시보드에서 401이 나며 다시 이 화면으로 돌아옵니다.</div>
</div></div>
<script>
function go(){const t=(document.getElementById("tok").value||"").trim();
if(!t){document.getElementById("err").innerText="코드를 입력해 주세요.";return;}
location.href="/dashboard?token="+encodeURIComponent(t);}
document.getElementById("tok").addEventListener("keydown",(e)=>{if(e.key==="Enter") go();});
</script></body></html>
"""

DASHBOARD_HTML = r"""
<!doctype html><html lang="ko"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>__DASHBOARD_TITLE__</title>
<style>
body{font-family:system-ui,Segoe UI,Apple SD Gothic Neo,Malgun Gothic,sans-serif;margin:0;background:#0b1020;color:#e8ecff}
header{padding:18px 20px;border-bottom:1px solid rgba(255,255,255,.08);position:sticky;top:0;background:#0b1020;z-index:10}
.wrap{max-width:1200px;margin:0 auto;padding:18px}
.kpis{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:10px}
.card{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);border-radius:14px;padding:12px}
.label{opacity:.8;font-size:12px}
.value{font-size:22px;font-weight:800;margin-top:6px}
.value.red{color:#ff4d4f}
.value.yellow{color:#ffcc00}
.value.blue{color:#4aa3ff}
.grid{display:grid;grid-template-columns:1.2fr .8fr;gap:12px;margin-top:12px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}
.barrow{display:flex;align-items:center;gap:10px;margin:8px 0}
.bar{flex:1;height:10px;background:rgba(255,255,255,.08);border-radius:999px;overflow:hidden}
.bar>div{height:100%;background:#4aa3ff}
.small{font-size:12px;opacity:.85}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:10px;border-bottom:1px solid rgba(255,255,255,.08);vertical-align:top}
th{opacity:.85;text-align:left;font-weight:700}
a{color:#8fd3ff;text-decoration:none}
.pill{display:inline-block;padding:2px 8px;border-radius:999px;font-size:12px;border:1px solid rgba(255,255,255,.14);opacity:.9}
.pill.red{border-color:rgba(255,77,79,.6);color:#ffb3b4}
.pill.gray{opacity:.65}
.controls{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}
input,select{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12);color:#e8ecff;border-radius:10px;padding:8px 10px}
select option{color:#0b1020;background:#e8ecff}
button{background:#4aa3ff;border:0;color:#061024;border-radius:10px;padding:8px 12px;font-weight:800;cursor:pointer}
button.secondary{background:rgba(255,255,255,.08);color:#e8ecff}
@media (max-width:1100px){.kpis{grid-template-columns:repeat(3,minmax(0,1fr))}.grid{grid-template-columns:1fr}}
</style></head><body>
<header><div class="wrap" style="padding:0 18px;"><div style="display:flex;justify-content:space-between;align-items:center;gap:10px;">
<div><div style="font-size:18px;font-weight:900;">__DASHBOARD_TITLE__</div><div class="small" id="sub"></div></div>
<div class="small" id="health"></div></div></div></header>

<div class="wrap">
<div class="card" style="margin-bottom:12px;"><div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
<div style="font-weight:900;">🏷️ 공지방(대화방) 선택</div><select id="f_room" onchange="onRoomChange()"></select>
<div class="small" id="roominfo"></div></div></div>

<div class="kpis">
<div class="card"><div class="label">🔥 Red Alert (Overdue + D-3)</div><div class="value red" id="k_red">-</div></div>
<div class="card"><div class="label">⏰ Overdue</div><div class="value red" id="k_overdue">-</div></div>
<div class="card"><div class="label">📅 D-7 이내</div><div class="value yellow" id="k_due7">-</div></div>
<div class="card"><div class="label">📌 OPEN 총계</div><div class="value blue" id="k_open">-</div></div>
<div class="card"><div class="label">🧊 14일+ 장기</div><div class="value" id="k_long">-</div></div>
<div class="card"><div class="label">🕳 목표일 미입력</div><div class="value" id="k_notarget">-</div></div>
</div>

<div class="grid">
<div class="card">
<div style="display:flex;justify-content:space-between;align-items:center;">
<div style="font-weight:900;">🚀 최근 8주 신규/완료 추이</div>
<div class="small">평균 처리기간(60d Closed): <b id="k_cycle">-</b>일</div></div>
<div class="small" style="margin-top:8px;opacity:.9;">(간단 막대 형태로 표시)</div><div id="trend"></div></div>

<div class="card"><div style="font-weight:900;">👤 담당자별 OPEN 상위</div><div id="owners"></div></div>
</div>

<div class="grid2">
<div class="card"><div style="font-weight:900;">🔥 가장 급한 TOP 10 (D-day)</div><div id="urgent"></div></div>
<div class="card"><div style="font-weight:900;">🧊 가장 오래된 TOP 10 (Aging)</div><div id="old"></div></div>
</div>

<div class="card" style="margin-top:12px;">
<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;">
<div style="font-weight:900;">📋 이슈 목록</div><div class="small">필터 후 새로고침</div></div>
<div class="controls">
<select id="f_status"><option value="OPEN">OPEN</option><option value="CLOSED">CLOSED</option><option value="ALL">ALL</option></select>
<input id="f_owner" placeholder="담당자(owner) 검색"/>
<input id="f_q" placeholder="제목/내용 검색" style="min-width:260px;"/>
<button onclick="loadIssues(0)">조회</button>
<button class="secondary" onclick="loadIssues(curPage-1)">이전</button>
<button class="secondary" onclick="loadIssues(curPage+1)">다음</button>
<span class="small" id="pageinfo"></span>
</div>

<div style="overflow:auto;margin-top:10px;">
<table><thead><tr>
<th style="width:70px;">ID</th><th>제목</th><th style="width:120px;">담당</th><th style="width:110px;">목표일</th>
<th style="width:70px;">D-day</th><th style="width:70px;">Aging</th><th style="width:70px;">링크</th>
</tr></thead><tbody id="tbody"></tbody></table></div></div></div>

<script>
const TOKEN="__TOKEN__";let curPage=0;
function esc(s){s=(s??"").toString();return s.replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#39;");}
async function loadRooms(){
  const url=`/api/dashboard/rooms?token=${encodeURIComponent(TOKEN)}`;const r=await fetch(url);if(!r.ok) return;
  const data=await r.json();const rooms=data.rooms||[];
  const sel=document.getElementById("f_room");
  sel.innerHTML=`<option value="">ALL(전체)</option>`+rooms.map(x=>`<option value="${esc(x.room_id)}">${esc(x.title)}</option>`).join("");
  document.getElementById("roominfo").innerText=`총 ${rooms.length}개 방`;
}
function onRoomChange(){loadSummary();loadIssues(0);}
function pillD(d){
  if(d===null||d===undefined) return `<span class="pill gray">-</span>`;
  if(d<0) return `<span class="pill red">D${d}</span>`;
  if(d<=3) return `<span class="pill red">D-${d}</span>`;
  if(d<=7) return `<span class="pill">D-${d}</span>`;
  return `<span class="pill gray">D-${d}</span>`;
}
function barRow(name,val,max){
  const w=max?Math.round((val/max)*100):0;
  return `<div class="barrow"><div style="width:130px;font-weight:700;">${esc(name)}</div>
    <div class="bar"><div style="width:${w}%"></div></div>
    <div style="width:44px;text-align:right;font-weight:800;">${val}</div></div>`;
}
async function loadSummary(){
  const room_id=(document.getElementById("f_room")?.value||"").trim();
  const url=`/api/dashboard/summary?token=${encodeURIComponent(TOKEN)}${room_id?`&room_id=${encodeURIComponent(room_id)}`:""}`;
  const r=await fetch(url);
  if(!r.ok){document.getElementById("health").innerText="접근 불가(토큰 확인)";return;}
  const data=await r.json();const k=data.kpi;
  document.getElementById("sub").innerText=`기준일: ${k.today}${room_id?` · room_id: ${room_id}`:""}`;
  document.getElementById("k_red").innerText=k.red_alert;
  document.getElementById("k_overdue").innerText=k.overdue;
  document.getElementById("k_due7").innerText=k.due_7;
  document.getElementById("k_open").innerText=k.open_total;
  document.getElementById("k_long").innerText=k.long_open_14;
  document.getElementById("k_notarget").innerText=k.no_target;
  document.getElementById("k_cycle").innerText=(k.avg_cycle_days_60d ?? "-");

  const labels=data.series.labels;const created=data.series.created;const closed=data.series.closed;
  const maxv=Math.max(...created,...closed,1);
  let html=`<div class="small">주차</div>`;
  for(let i=0;i<labels.length;i++){
    html+=`<div class="barrow"><div style="width:90px;" class="small">${labels[i]}</div>
      <div class="bar"><div style="width:${Math.round(created[i]/maxv*100)}%"></div></div>
      <div style="width:48px;" class="small">신규 ${created[i]}</div>
      <div class="bar"><div style="width:${Math.round(closed[i]/maxv*100)}%;background:#ffcc00"></div></div>
      <div style="width:48px;" class="small">완료 ${closed[i]}</div></div>`;
  }
  document.getElementById("trend").innerHTML=html;

  const owners=data.owner_top||[];const omax=Math.max(...owners.map(x=>x.open_cnt),1);
  document.getElementById("owners").innerHTML=owners.map(x=>barRow(x.owner,x.open_cnt,omax)).join("")||`<div class="small">데이터 없음</div>`;

  const urg=data.urgent_top10||[];
  document.getElementById("urgent").innerHTML=
    `<table><thead><tr><th>ID</th><th>제목</th><th>D</th><th>담당</th><th>링크</th></tr></thead><tbody>`+
    urg.map(x=>`<tr><td>#${x.issue_id}</td><td>${esc(x.title)}</td><td>${pillD(x.d_day)}</td><td>${esc(x.owner||"")}</td><td>${x.url?`<a href="${esc(x.url)}" target="_blank">열기</a>`:"-"}</td></tr>`).join("")+
    `</tbody></table>`;

  const old=data.old_top10||[];
  document.getElementById("old").innerHTML=
    `<table><thead><tr><th>ID</th><th>제목</th><th>Aging</th><th>담당</th><th>링크</th></tr></thead><tbody>`+
    old.map(x=>`<tr><td>#${x.issue_id}</td><td>${esc(x.title)}</td><td><span class="pill">${x.age_days}d</span></td><td>${esc(x.owner||"")}</td><td>${x.url?`<a href="${esc(x.url)}" target="_blank">열기</a>`:"-"}</td></tr>`).join("")+
    `</tbody></table>`;

  document.getElementById("health").innerText="● LIVE";
}
async function loadIssues(page){
  if(page<0) page=0;
  const status=document.getElementById("f_status").value;
  const owner=document.getElementById("f_owner").value.trim();
  const q=document.getElementById("f_q").value.trim();
  const room_id=(document.getElementById("f_room")?.value||"").trim();
  const size=50;
  const url=`/api/dashboard/issues?token=${encodeURIComponent(TOKEN)}&status=${encodeURIComponent(status)}&owner=${encodeURIComponent(owner)}&q=${encodeURIComponent(q)}&page=${page}&size=${size}`+(room_id?`&room_id=${encodeURIComponent(room_id)}`:"");
  const r=await fetch(url);if(!r.ok) return;
  const data=await r.json();curPage=data.page;
  document.getElementById("pageinfo").innerText=`page ${data.page+1} / total ${data.total}`;
  const rows=data.items||[];
  document.getElementById("tbody").innerHTML=rows.map(x=>`
    <tr><td>#${x.issue_id}</td>
    <td>${esc(x.title)}<div class="small">${esc(x.content||"")}</div></td>
    <td>${esc(x.owner||"")}</td><td>${esc(x.target_date||"")}</td>
    <td>${pillD(x.d_day)}</td><td><span class="pill gray">${x.age_days}d</span></td>
    <td>${x.url?`<a href="${esc(x.url)}" target="_blank">열기</a>`:"-"}</td></tr>`).join("");
}
(async()=>{await loadRooms();loadSummary();loadIssues(0);setInterval(loadSummary,30000);})();
</script></body></html>
"""

# =========================
# Adaptive Cards
# =========================
def build_issue_deadline_reminder_card(items: List[tuple], today_str: str):
    items = sorted(items, key=lambda x: (x[0], int(x[1]["issue_id"])))
    body = [
        {"type":"TextBlock","text":"🚨 목표일 임박 이슈 알림", "size":"Large", "weight":"Bolder", "color":"Attention", "wrap":True},
        {"type":"TextBlock","text":f"기준: {today_str} 09:00", "wrap":True, "spacing":"Small", "isSubtle": True},
        {"type":"TextBlock","text":"", "separator": True, "spacing":"Medium"},
    ]
    for d, it in items[:10]:
        title = it.get("title","")
        owner = it.get("owner","")
        td    = it.get("target_date","")
        content = it.get("content","")
        url   = (it.get("url") or "").strip()

        body.append({
            "type":"Container",
            "style":"emphasis",
            "items":[
                {"type":"ColumnSet","columns":[
                    {"type":"Column","width":"stretch","items":[{"type":"TextBlock","text":f"#{it['issue_id']} {title}","weight":"Bolder","wrap":True}]},
                    {"type":"Column","width":"auto","items":[{"type":"TextBlock","text":f"D-{d}","weight":"Bolder","color":"Attention","wrap":True,"horizontalAlignment":"Right"}]}
                ]},
                {"type":"TextBlock","text":f"목표일: {td} / 담당: {owner}", "wrap":True, "spacing":"Small"},
                *([{"type":"TextBlock","text":f"내용: {content}", "wrap":True, "spacing":"Small"}] if content else []),
                *([
                    {"type":"TextBlock","text":f"LINK: {url}", "wrap":True, "spacing":"Small", "isSubtle": True},
                    {"type":"ActionSet","actions":[{"type":"Action.OpenUrl","title":"🔗 링크 열기","url": url}], "spacing":"Small"}
                ] if url else []),
            ]
        })
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"📋 현재 이슈", "data":{"action":"ISSUE_LIST"}},
            {"type":"Action.Submit","title":"🕓 이슈 이력", "data":{"action":"ISSUE_HISTORY", "page": 0}},
            {"type":"Action.Submit","title":"홈", "data":{"action":"HOME"}},
        ]
    }

def build_home_card(dashboard_url: str, infocenter_url: str):
    def btn(title: str, action: str | None = None, style: str | None = None, url: str | None = None) -> dict:
        if url:
            return {"type":"Action.OpenUrl","title":title,"url":url}
        a = {"type":"Action.Submit","title":title,"data":{"action":action}}
        if style:
            a["style"] = style
        return a

    def col_button(title: str | None, action: str | None = None, style: str | None = None, url: str | None = None) -> dict:
        if not title:
            return {"type":"Column","width":"stretch","items":[]}
        if url:
            return {"type":"Column","width":"stretch","items":[{"type":"ActionSet","actions":[btn(title,None,style,url=url)]}]}
        if not action:
            return {"type":"Column","width":"stretch","items":[]}
        return {"type":"Column","width":"110px","items":[{"type":"ActionSet","actions":[btn(title,action,style)]}]}

    def row2(c1,c2)->dict: return {"type":"ColumnSet","columns":[c1,c2],"spacing":"Small"}
    def section(title: str)->dict: return {"type":"TextBlock","text":title,"weight":"Bolder","wrap":True,"spacing":"Medium"}
    def divider()->dict: return {"type":"TextBlock","text":"","separator":True,"spacing":"Medium"}

    def _rows2_from_buttons(btns):
        rows=[]
        for i in range(0,len(btns),2):
            chunk=btns[i:i+2]
            t,a,s,u = chunk[0] if len(chunk)>=1 else (None,None,None,None)
            c1=col_button(t,a,style=s,url=u)
            t,a,s,u = chunk[1] if len(chunk)>=2 else (None,None,None,None)
            c2=col_button(t,a,style=s,url=u)
            rows.append(row2(c1,c2))
        return rows

    ISSUE_BUTTONS = [
        ("📝 이슈발의","ISSUE_FORM","positive",None),
        ("📋 현재이슈","ISSUE_LIST",None,None),
        ("🕓 이슈이력","ISSUE_HISTORY",None,None),
        ("🔗 대시보드",None,None,dashboard_url),
    ]
    HOME_QUERY_BUTTONS = [
        ("🔎 코드조회","CODE_FINDER_FORM",None,None),
        ("🔎 PKG코드","PKGCODE_FORM",None,None), 
        ("🔎 담당조회","RIGHTPERSON_FORM",None,None),        
        ("🧩 PS  조회","PS_FORM",None,None),        
        ("📚 용어검색","TERM_FORM",None,None),
        ("📈 입고계획","IPGO_FORM",None,None),
        # ("🔗 정보센터",None,None,infocenter_url),
        ("🚚 출하조회","SHIP_FORM",None,None),  
        
    ]
    WATCHROOM_BUTTONS = [
        ("📣 이슈방+","WATCHROOM_FORM",None,None),
        ("🧭 바로가기","QUICK_LINKS",None,None),
    ]

    body = [
        {"type": "TextBlock","text": "🤖 공급망 챗봇 ","size": "Large","weight": "Bolder"},
        {"type": "TextBlock","text": "Built by 조성묵","size": "Small","isSubtle": True,"spacing": "None"},
        {"type":"TextBlock","text":"[@공급망 챗봇]으로 호출 하세요.","wrap":True,"spacing":"Small"},
        divider(), section("이슈"), *_rows2_from_buttons(ISSUE_BUTTONS),
        divider(), section("조회"), *_rows2_from_buttons(HOME_QUERY_BUTTONS),
        divider(), section("공지방"), *_rows2_from_buttons(WATCHROOM_BUTTONS),
    ]

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
    }

def build_quicklink_card(title: str, url: str) -> dict:
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body":[
            {"type":"TextBlock","text":title or "🔗 바로가기", "size":"Large", "weight":"Bolder"},
            {"type":"TextBlock","text":url, "wrap":True, "isSubtle":True, "spacing":"Small"},
        ],
        "actions":[
            {"type":"Action.OpenUrl","title":"🌐 열기", "url": url},
            {"type":"Action.Submit","title":"홈", "data":{"action":"HOME"}},
        ],
    }


def build_issue_form_card(sender_hint: str = "", room_id: str = ""):
    today = datetime.now().strftime("%Y-%m-%d")
    target_default = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body":[
            {"type":"TextBlock","text":"📝 이슈 발의","size":"Large","weight":"Bolder"},
            {"type":"TextBlock","text":"(이 화면은 개인메신저에서 작성되며, 등록은 원래 대화방에 반영됩니다.)", "wrap":True, "spacing":"Small", "isSubtle":True},
            {"type":"Input.Text","id":"title","placeholder":"제목", "maxLength":60},
            {"type":"Input.Text","id":"content","placeholder":"내용", "isMultiline":True, "maxLength":2000},
            {"type":"Input.Text","id":"url","placeholder":"LINK(선택) 예: https://... 또는 gpmtsp.samsungds.net", "maxLength":200},
            {"type":"Input.Date","id":"occur_date","title":"발생일", "value":today},
            {"type":"Input.Date","id":"target_date","title":"목표일", "value":target_default},
            {"type":"Input.Text","id":"owner","placeholder":"담당자(SSO or 이름)", "value":sender_hint},
        ],
        "actions":[
            {"type":"Action.Submit","title":"등록","style":"positive","data":{"action":"ISSUE_CREATE", "room_id": room_id}},
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }

def build_issue_list_card(issues: List[dict], room_id: str = ""):
    body = [{"type":"TextBlock","text":"📋 현재 OPEN 이슈","size":"Large","weight":"Bolder"}]
    if not issues:
        body.append({"type":"TextBlock","text":"현재 등록된 OPEN 이슈가 없습니다."})
    else:
        # ✅ UI에서도 한번 더 정렬(안전빵): d_day 오름차순(오버듀/임박 우선), None은 맨 뒤
        items = []
        for it in issues:
            dd = store._dday(it.get("target_date", ""))
            it["_dday"] = dd
            items.append(it)

        items.sort(key=lambda x: (999999 if x.get("_dday") is None else x.get("_dday"), int(x.get("issue_id", 0))))

        for it in issues[:30]:
            # ✅ store.py에서 계산된 d_day 우선 사용 (없으면 fallback)
            dd = it.get("d_day", None)
            if dd is None:
                dd = store._dday(it.get("target_date", ""))

            # ✅ [D-X] 태그 + 색(주황/빨강) + 지날수록 강도 이모지
            tag_text = ""
            tag_color = "Default"
            if dd is not None:
                if dd < 0:
                    mark = "🔥" if dd <= -3 else "🚨"
                    tag_text = f"{mark}[D+{abs(dd)}]"
                    tag_color = "Attention"
                elif dd == 0:
                    tag_text = "🚨[D-0]"
                    tag_color = "Attention"
                else:
                    tag_text = f"[D-{dd}]"
                    tag_color = "Warning" if dd <= 3 else "Default"

            url = (it.get("url") or "").strip()

            body.append({
                "type":"Container",
                "style":"emphasis",
                "items":[
                    # ✅ 태그(색) + 제목(고정 색) 분리
                    {"type":"ColumnSet","columns":[
                        {"type":"Column","width":"auto","items":[
                            {"type":"TextBlock","text":tag_text,"weight":"Bolder","wrap":True,
                             "color":tag_color,"isVisible": True if tag_text else False}
                        ]},
                        {"type":"Column","width":"stretch","items":[
                            {"type":"TextBlock","text":f"#{it['issue_id']} {it['title']}",
                             "weight":"Bolder","wrap":True,"color":"Accent"}
                        ]},
                    ]},
                    {"type":"TextBlock","text":f"- 내용: {it.get('content','')}", "wrap":True, "spacing":"Small"},                                        
                    {"type":"TextBlock","text":f"📅 발생일: {it.get('occur_date','-')} · 목표일: {it.get('target_date','-')}", "wrap":True, "spacing":"None", "isSubtle": True},
                    {"type":"TextBlock","text":f"👤 담당: {it.get('owner','-')}", "wrap":True, "spacing":"Small", "weight":"Bolder", "color":"Dark"},


                    *([
                        {"type":"TextBlock","text":f"- LINK: {url}","wrap":True,"spacing":"Small"},
                        {"type":"ActionSet","actions":[{"type":"Action.OpenUrl","title":"🔗 링크 열기","url": url}],"spacing":"Small"}
                    ] if url else []),
                    {"type":"ActionSet","actions":[
                        {"type":"Action.Submit","title":"✅ 완료","style":"positive","data":{"action":"ISSUE_CLEAR","issue_id":it["issue_id"], "room_id": room_id}},
                        {"type":"Action.Submit","title":"✏️ 수정","data":{"action":"ISSUE_EDIT_FORM","issue_id":it["issue_id"], "room_id": room_id}},
                    ]}
                ]
            })


    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"📝 이슈 발의","data":{"action":"ISSUE_FORM", "room_id": room_id}},
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }

def build_issue_edit_form_card(issue: dict, room_id: str = ""):
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body":[
            {"type":"TextBlock","text":f"✏️ 이슈 수정 #{issue.get('issue_id')}", "size":"Large", "weight":"Bolder"},
            {"type":"Input.Text","id":"title","placeholder":"제목(짧게)", "maxLength":60, "value":issue.get("title","")},
            {"type":"Input.Text","id":"content","placeholder":"내용(초간단 한줄)", "isMultiline":True, "maxLength":200, "value":issue.get("content","") or ""},
            {"type":"Input.Text","id":"url","placeholder":"LINK(선택)", "maxLength":200, "value":issue.get("url","") or ""},
            {"type":"Input.Date","id":"occur_date","title":"발생일", "value": (issue.get("occur_date") or "")},
            {"type":"Input.Date","id":"target_date","title":"목표일", "value": (issue.get("target_date") or "")},
            {"type":"Input.Text","id":"owner","placeholder":"담당자(SSO or 이름)", "value":issue.get("owner","") or ""},
        ],
        "actions":[
            {"type":"Action.Submit","title":"저장","style":"positive","data":{"action":"ISSUE_UPDATE","issue_id":issue.get("issue_id"), "room_id": room_id}},
            {"type":"Action.Submit","title":"현재 이슈","data":{"action":"ISSUE_LIST", "room_id": room_id}},
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }


def build_issue_summary_card(issues: List[dict], today_str: str, max_items: int = 15) -> dict:
    """
    매일 아침 OPEN 이슈 요약용 AdaptiveCard 1장
    issues: store.issue_list_open(...) 결과 + (선택) it["d_day"] 가 들어있으면 사용
    """

    def dlabel(d: Optional[int]) -> str:
        if d is None:
            return ""
        if d > 0:
            return f"D-{d}"
        if d == 0:
            return "🚨 D-DAY"
        return f"D+{abs(d)}"   # overdue

    body = [
        {"type":"TextBlock", "text":"📌 [OPEN 이슈 요약]", "size":"Medium", "weight":"Bolder"},
        {"type":"TextBlock", "text":f"기준: {today_str} · 총 {len(issues)}건", "wrap":True, "spacing":"Small", "isSubtle":True},
        {"type":"TextBlock", "text":"", "separator": True, "spacing":"Medium"},
    ]

    if not issues:
        body.append({"type":"TextBlock", "text":"OPEN 이슈가 없습니다 ✅", "wrap":True})
    else:
        for it in issues[:max_items]:
            _id = it.get("issue_id", "")
            title = (it.get("title") or "").strip()
            owner = (it.get("owner") or "").strip()
            target_date = (it.get("target_date") or "").strip()

            dd = it.get("d_day", None)
            if dd is None:
                dd = store._dday(it.get("target_date", ""))

            tag_text = ""
            tag_color = "Default"
            if dd is not None:
                if dd < 0:
                    mark = "🔥" if dd <= -3 else "🚨"
                    tag_text = f"{mark}[D+{abs(dd)}]"
                    tag_color = "Attention"
                elif dd == 0:
                    tag_text = "🚨[D-0]"
                    tag_color = "Attention"
                else:
                    tag_text = f"[D-{dd}]"
                    tag_color = "Warning" if dd <= 3 else "Default"

            body.append({
                "type":"Container",
                "spacing":"Small",
                "items":[
                    {"type":"ColumnSet","columns":[
                        {"type":"Column","width":"auto","items":[
                            {"type":"TextBlock","text":tag_text,"wrap":True,"weight":"Bolder",
                             "color":tag_color,"isVisible": True if tag_text else False}
                        ]},
                        {"type":"Column","width":"stretch","items":[
                            {"type":"TextBlock","text":f"#{_id} {title}",
                             "wrap":True, "color":"Accent", "weight":"Bolder"}
                        ]},
                    ]},
                    {"type":"TextBlock",
                     "text":f"담당: {owner or '-'} · 목표: {target_date or '-'}",
                     "wrap":True, "spacing":"None", "isSubtle":True}
                ]
            })


        if len(issues) > max_items:
            body.append({"type":"TextBlock", "text":f"…외 {len(issues)-max_items}건", "wrap":True, "spacing":"Small", "isSubtle":True})

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"📋 현재 이슈","data":{"action":"ISSUE_LIST"}},
            {"type":"Action.Submit","title":"🏠 홈","data":{"action":"HOME"}},
        ],
    }


def build_issue_history_card(issues: List[dict], page: int = 0, total: int = 0, page_size: int = store.HISTORY_PAGE_SIZE, room_id: str = ""):
    total_pages = max(1, (int(total) + int(page_size) - 1) // int(page_size))
    page = max(0, min(int(page), total_pages - 1))

    def nav_col(title: str, p: int, enabled: bool):
        if not enabled:
            return {"type":"Column","width":"stretch","items":[]}
        return {"type":"Column","width":"stretch","items":[{"type":"ActionSet","actions":[
            {"type":"Action.Submit","title":title,"data":{"action":"ISSUE_HISTORY","page":p, "room_id": room_id}}
        ]}]} 

    body = [
        {"type":"TextBlock","text":"🕓 이슈 이력(OPEN/CLOSED)","size":"Large","weight":"Bolder"},
        {"type":"TextBlock","text":f"페이지 {page+1}/{total_pages} · 총 {total}건 · {page_size}개씩", "wrap":True, "spacing":"Small"},
        {"type":"ColumnSet","spacing":"Medium","columns":[
            nav_col("⬅️ 이전", page-1, enabled=(page > 0)),
            nav_col("➡️ 다음", page+1, enabled=(page < total_pages-1)),
        ]},
        {"type":"TextBlock","text":"", "separator": True, "spacing":"Medium"},
    ]

    if not issues:
        body.append({"type":"TextBlock","text":"이슈 이력이 없습니다."})
    else:
        for it in issues[:page_size]:
            status = (it.get("status") or "").upper()
            is_closed = (status == "CLOSED")

            dlabel = ""
            td = store._parse_ymd(it.get("target_date", ""))
            if td:
                d = (td - datetime.now().date()).days
                if d > 0: dlabel = f"D-{d}"
                elif d == 0: dlabel = "🚨 D-DAY"
                else: dlabel = f"D+{abs(d)}"

            actions = []
            if is_closed:
                actions.append({"type":"Action.Submit","title":"🗑️ 삭제","style":"destructive",
                                "data":{"action":"ISSUE_DELETE","issue_id":it["issue_id"], "page": page, "room_id": room_id}})

            url = (it.get("url") or "").strip()

            body.append({
                "type":"Container","style":"emphasis",
                "items":[
                    {"type":"ColumnSet","columns":[
                        {"type":"Column","width":"stretch","items":[{"type":"TextBlock","text":f"[{status}] #{it['issue_id']} {it['title']}",
                            "weight":"Bolder","wrap":True,"color":("Accent" if not is_closed else "Default"),"isSubtle":is_closed}]},
                        {"type":"Column","width":"auto","items":[{"type":"TextBlock","text":dlabel,"weight":"Bolder","wrap":True,
                            "color":"Attention","horizontalAlignment":"Right","isVisible": True if dlabel else False,"isSubtle":is_closed}]},
                    ]},
                    {"type":"TextBlock","text":f"- 내용: {it.get('content','')}", "wrap":True, "spacing":"Small", "isSubtle": is_closed},
                    {"type":"TextBlock","text":f"- 발생일: {it.get('occur_date','')} / 목표일: {it.get('target_date','')} / 담당: {it.get('owner','')}",
                     "wrap":True, "spacing":"Small", "isSubtle": is_closed},
                    *([
                        {"type":"TextBlock","text":f"- LINK: {url}", "wrap":True, "spacing":"Small", "isSubtle": is_closed},
                        {"type":"ActionSet","actions":[{"type":"Action.OpenUrl","title":"🔗 링크 열기","url": url}],"spacing":"Small"}
                    ] if url else []),
                    *([{"type":"ActionSet","actions": actions, "spacing":"Small"}] if actions else [])
                ]
            })

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"📋 현재 이슈","data":{"action":"ISSUE_LIST", "room_id": room_id}},
            {"type":"Action.Submit","title":"📝 이슈 발의","data":{"action":"ISSUE_FORM", "room_id": room_id}},
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }

def build_watchroom_form_card():
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body":[
            {"type":"TextBlock","text":"📣 공지방 생성(푸시 대상)","size":"Large","weight":"Bolder"},
            {"type":"TextBlock","text":"봇이 직접 만든 방만 스케줄 워닝/이슈요약 푸시가 가능합니다.", "wrap":True},
            {"type":"Input.Text","id":"room_title","placeholder":"대화방 이름(선택) 예: 운영 워닝/이슈 공지방", "maxLength":128},
            {"type":"Input.Text","id":"members","placeholder":"참여자 SSO(콤마구분) 예: sungmook.cho,cc.choi", "isMultiline":True},
            {"type":"Input.Text","id":"note","placeholder":"방 설명(선택, DB에만 저장됨) 예: 운영 워닝/이슈방", "maxLength":80},
        ],
        "actions":[
            {"type":"Action.Submit","title":"공지방 생성 & 등록","style":"positive","data":{"action":"WATCHROOM_CREATE"}},
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }

def build_query_form_card(spec: Dict[str, Any]) -> dict:
    if spec.get("form_type") == "ONEVIEW_SHIP":
        return build_oneview_ship_form_card(spec)
    if spec.get("form_type") == "PKGCODE":
        return build_pkgcode_form_card(spec)    
    if spec.get("form_type") == "PS":
        return build_ps_form_card(spec)    
        
    body = [
        {"type":"TextBlock","text":f"🔎 {spec.get('title','조회')}", "size":"Large", "weight":"Bolder"},
        {"type":"TextBlock","text":"필수값을 입력 후 조회를 누르세요.", "wrap":True, "spacing":"Small"},
    ]

    for f in spec.get("fields", []):
        label = f.get("label", f.get("id"))
        ph = f.get("placeholder", "")
        required = f.get("required", False)
        body.append({"type":"TextBlock","text":f"{label}{' *' if required else ''}", "wrap":True, "spacing":"Small"})
        body.append({"type":"Input.Text", "id": f["id"], "placeholder": ph, "maxLength": 80})

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            *([{"type":"Action.OpenUrl","title":"🛠 수정","url": spec["edit_url"]}] if spec.get("edit_url") else []),
            {"type":"Action.Submit","title":"조회", "style":"positive", "data":{"action": spec["run_action"]}},
            {"type":"Action.Submit","title":"홈", "data":{"action":"HOME"}},
        ],
    }

# (추가 코드 - 추가용)  ※ build_query_form_card 아래에 붙이면 됨
def build_oneview_ship_form_card(spec: Dict[str, Any]) -> dict:
    ym = datetime.now().strftime("%Y%m")
    return {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.3",
        "body": [
            {"type":"Container","items":[
                {"type":"TextBlock","text":"🚚 출하이력 조회","size":"Large","weight":"Bolder"},
                {"type":"TextBlock","text":"NERP 원뷰 출하 조회 입니다.","spacing":"Small","weight":"Bolder","wrap":True},
            ]},
            {"type":"Container","spacing":"Padding","items":[
                {"type":"TextBlock","text":"※ 조회버튼은 한번만!","weight":"Bolder","wrap":True},
                {"type":"Container","style":"emphasis","spacing":"Padding","items":[
                    {"type":"ColumnSet","spacing":"Padding","columns":[
                        {"type":"Column","width":"72px","items":[{"type":"TextBlock","text":"설명","color":"Dark"}]},
                        {"type":"Column","width":"stretch","items":[
                            {"type":"TextBlock","text":"NERP 원뷰 테이블(출하) 기준 출하 조회 입니다.\n\nDO, ITEM, VERSION, 거래선으로 조회 가능합니다.","wrap":True}
                        ]},
                    ]}
                ]},

                {"type":"TextBlock","text":"조회월(YYYYMM)","weight":"Bolder","spacing":"Medium"},
                {"type":"ColumnSet","columns":[
                    {"type":"Column","width":"stretch","items":[{"type":"Input.Text","id":"hastartmon01","value": ym}]},
                    {"type":"Column","width":"stretch","items":[{"type":"Input.Text","id":"haendmon01","value": ym}]},
                ]},

                {"type":"TextBlock","text":"조회기준","weight":"Bolder","spacing":"Medium"},
                {"type":"Input.ChoiceSet",
                    "id":"hachoiceset01",
                    "style":"expanded",
                    "value":"deliverynum01",
                    "choices":[
                        {"title":"DO No.","value":"deliverynum01"},
                        {"title":"ITEM","value":"haitem01"},
                        {"title":"VERSION","value":"haversion01"},
                        {"title":"거래선","value":"hagc01"},
                    ]
                },

                {"type":"TextBlock","text":"검색값","weight":"Bolder","spacing":"Medium"},
                {"type":"Input.Text","id":"hainputtext01","placeholder":"ex) DO no. / ITEM / VERSION / 거래선", "maxLength": 80},
            ]},
        ],
        "actions":[
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
            {"type":"Action.Submit","title":"조회","style":"positive","data":{"action": spec["run_action"]}},
        ],
    }

# (추가 코드 - 추가용)  ※ build_query_form_card 아래쪽에 추가
def build_pkgcode_form_card(spec: Dict[str, Any]) -> dict:
    return {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.3",
        "body": [
            {"type":"Container","items":[
                {"type":"TextBlock","text":"📦 PKGCODE 조회","size":"Large","weight":"Bolder"},
                {"type":"TextBlock","text":"VER, PKG, PCBCODE를 입력해 주세요", "wrap":True, "spacing":"Small", "weight":"Bolder"},
                {"type":"TextBlock","text":"※ 조회버튼은 한번만!", "wrap":True, "spacing":"Small", "weight":"Bolder"},
                {"type":"Container","style":"emphasis","spacing":"Padding","items":[
                    {"type":"ColumnSet","spacing":"Padding","columns":[
                        {"type":"Column","width":"72px","items":[{"type":"TextBlock","text":"설명","color":"Dark"}]},
                        {"type":"Column","width":"stretch","items":[
                            {"type":"TextBlock","text":"PCB정보를 조회하는 용도이며,\n\nVER, PKG, PCBCODE로 조회 가능하고,\n\nPCB 수량 정보도 포함합니다.", "wrap":True}
                        ]},
                    ]}
                ]},
            ]},
            {"type":"Container","spacing":"Padding","items":[
                {"type":"Input.Text","id":"pkgcode3341","placeholder":"ex) WL / 9N7 / LA41-12223A", "maxLength": 80},
            ]},
        ],
        "actions":[
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
            {"type":"Action.Submit","title":"조회","style":"positive","data":{"action": spec["run_action"]}},
        ],
    }

def build_ps_form_card(spec: Dict[str, Any]) -> dict:
    return {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.3",
        "body": [
            {"type":"Container","items":[
                {"type":"TextBlock","text":"🧩 PS 조회","size":"Large","weight":"Bolder"},
                {"type":"TextBlock","text":"코드구분/조회기준 선택 후 코드 입력해서 조회하세요.", "wrap":True, "spacing":"Small", "weight":"Bolder"},
                {"type":"TextBlock","text":"※ 조회버튼은 한번만!", "wrap":True, "spacing":"Small", "weight":"Bolder"},
            ]},

            {"type":"Container","spacing":"Padding","items":[
                {"type":"TextBlock","text":"코드구분", "weight":"Bolder", "spacing":"Small"},
                {"type":"Input.ChoiceSet",
                    "id":"psgubun01",
                    "style":"expanded",
                    "value":"pscomp01",
                    "choices":[
                        {"title":"COMP",   "value":"pscomp01"},
                        {"title":"MODULE", "value":"psmodule01"},
                        {"title":"MCP",    "value":"psmultichip01"},
                    ]
                },

                {"type":"TextBlock","text":"조회기준", "weight":"Bolder", "spacing":"Medium"},
                {"type":"Input.ChoiceSet",
                    "id":"psconv01",
                    "value":"pseds03",
                    "choices":[
                        {"title":"FAB_CODE", "value":"psfab02"},
                        {"title":"EDS_CODE", "value":"pseds03"},
                        {"title":"ASY_CODE", "value":"psasy04"},
                        {"title":"TST_CODE", "value":"pstst05"},
                        {"title":"MOD_CODE", "value":"psmod06"},
                    ]
                },

                {"type":"TextBlock","text":"코드입력", "weight":"Bolder", "spacing":"Medium"},
                {"type":"Input.Text","id":"psver01","placeholder":"ex) K3KL4.. / WL / 9N7 ...", "maxLength": 80},
            ]},
        ],
        "actions":[
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
            {"type":"Action.Submit","title":"조회","style":"positive","data":{"action": spec["run_action"]}},
        ],
    }


def build_term_search_results_card(query: str, results: List[dict]) -> dict:
    body = [
        {"type":"TextBlock","text":"📚 용어검색 결과", "size":"Large", "weight":"Bolder"},
        {"type":"TextBlock","text":f"검색어: {query}", "wrap":True, "spacing":"Small", "isSubtle": True},
        {"type":"TextBlock","text":"", "separator": True, "spacing":"Medium"},
    ]

    for it in results[:10]:
        term = it.get("term","")
        subject = it.get("subject","")
        content = (it.get("content","") or "").strip()
        _raw_link = (it.get("link","") or "").strip()
        link = "" if _raw_link.lower() in ("none", "null", "nan", "-") else _raw_link

        # 너무 길면 살짝 자르기
        if len(content) > 280:
            content = content[:280] + "…"

        block = {
            "type":"Container",
            "style":"emphasis",
            "items":[
                {"type":"TextBlock","text":term, "weight":"Bolder", "wrap":True, "color":"Accent"},
                *([{"type":"TextBlock","text":f"분류: {subject}", "wrap":True, "spacing":"Small", "isSubtle": True}] if subject else []),
                *([{"type":"TextBlock","text":content, "wrap":True, "spacing":"Small"}] if content else []),
                *([{"type":"ActionSet","actions":[{"type":"Action.OpenUrl","title":"🔗 링크 열기","url": link}]}] if link else []),
            ]
        }
        body.append(block)

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }

def build_term_not_found_card(query: str) -> dict:
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body":[
            {"type":"TextBlock","text":"📚 용어검색", "size":"Large", "weight":"Bolder"},
            {"type":"TextBlock","text":f"[ {query} ]에 대한 검색결과가 없습니다.", "wrap":True, "spacing":"Small"},
            {"type":"TextBlock","text":"(필요하면 담당자에게 반영 요청할 수 있어요)", "wrap":True, "spacing":"Small", "isSubtle":True},
            {"type":"Input.Text","id":"memo","placeholder":"추가 설명(선택) 예: 어디서 봤는지/의미 추정 등", "isMultiline":True, "maxLength":400},
        ],
        "actions":[
            {"type":"Action.Submit","title":"📩 반영 요청", "style":"positive",
             "data":{"action":"TERM_UNKNOWN_SUBMIT", "findword": query}},
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }

def df_to_table_card(df: pd.DataFrame, title: str = "조회 결과", max_rows: int = 10, max_cols: int = 6) -> dict:
    if df is None or df.empty:
        return {
            "type":"AdaptiveCard",
            "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
            "version":"1.3",
            "body":[
                {"type":"TextBlock","text":f"🔎 {title}","size":"Large","weight":"Bolder"},
                {"type":"TextBlock","text":"조회 결과: 0건", "wrap":True}
            ],
            "actions":[{"type":"Action.Submit","title":"홈","data":{"action":"HOME"}}]
        }

    view = df.copy()
    if view.shape[1] > max_cols:
        view = view.iloc[:, :max_cols]
    truncated_rows = view.shape[0] > max_rows
    view = view.head(max_rows)

    def cell_str(x, limit=40):
        s = "" if x is None else str(x)
        s = s.replace("\n", " ")
        return s if len(s) <= limit else (s[:limit] + "…")

    columns = list(view.columns)

    body = [
        {"type":"TextBlock","text":f"🔎 {title}", "size":"Large", "weight":"Bolder"},
        {"type":"TextBlock","text":f"Rows: {df.shape[0]} / Cols: {df.shape[1]} (표시는 {min(df.shape[0],max_rows)}행, {min(df.shape[1],max_cols)}열)", "wrap":True, "spacing":"Small"},
        {"type":"TextBlock","text":"", "separator": True}
    ]

    body.append({
        "type":"ColumnSet",
        "columns":[{"type":"Column","width":"stretch","items":[{"type":"TextBlock","text":str(col), "weight":"Bolder","wrap":True}]} for col in columns],
        "spacing":"Small"
    })

    for _, row in view.iterrows():
        body.append({
            "type":"ColumnSet",
            "columns":[{"type":"Column","width":"stretch","items":[{"type":"TextBlock","text":cell_str(row[col]), "wrap":True,"spacing":"None"}]} for col in columns],
            "spacing":"Small"
        })

    if truncated_rows:
        body.append({"type":"TextBlock","text":"※ 결과가 많아 일부만 표시했습니다.", "wrap":True, "spacing":"Small"})

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
            {"type":"Action.Submit","title":"🔎 조회 1","data":{"action":"QUERY_1"}},
            {"type":"Action.Submit","title":"🔎 조회 2","data":{"action":"QUERY_2"}},
        ]
    }

def build_open_url_card(title: str, url: str) -> dict:
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body":[
            {"type":"TextBlock","text":title or "바로 열기", "size":"Large", "weight":"Bolder", "wrap":True},
            {"type":"TextBlock","text":url or "", "wrap":True, "isSubtle":True, "spacing":"Small"},
        ],
        "actions":[
            {"type":"Action.OpenUrl","title":"🚀 열기","url": url},
            {"type":"Action.Submit","title":"🏠 홈","data":{"action":"HOME"}},
        ],
    }


def build_quick_links_card(quick_links: list[tuple[list[str], str, str]]) -> dict:
    # 카테고리 분류(원하면 여기 룰만 바꾸면 됨)
    def norm(s: str) -> str:
        return (s or "").replace(" ", "").upper().strip()

    def cat(aliases: list[str], title: str, url: str) -> str:
        a = {norm(x) for x in (aliases or [])}
        t = norm(title)
        if a & {"GSCM","NSCM","O9"}: return "SCM"
        if a & {"SMDM","MDM","EDM","MSTR"}: return "마스터/시스템"
        if a & {"POWERBI","POWERBI","BI","PB","파워","파워BI"}: return "리포트/분석"
        if a & {"GITHUB","GIT","깃허브","깃헙"}: return "개발"
        if a & {"CONF","CONFLUENCE","컨플","컨플루언스","DSDN","DSASSISTANT","GPT","정보센터","정보"}: return "지식/협업"
        if "VOC" in t or any("VOC" in norm(x) for x in aliases): return "VOC/요청"
        if a & {"밥","식단","점심","아침","저녁","버스","출퇴근","통근","근태","근무시간","패밀리","패밀리넷","패넷","FAMILYNET","싱글","녹스","메일"}: return "생활"
        return "기타"

    def btn(title: str, url: str) -> dict:
        return {"type":"Action.OpenUrl","title":title,"url":url}

    def col_button(title: str | None, url: str | None) -> dict:
        if not title or not url:
            return {"type":"Column","width":"stretch","items":[]}

        # ✅ 균일 타일(박스) + 클릭(selectAction)
        return {
            "type":"Column",
            "width":"stretch",
            "items":[
                {
                    "type":"Container",
                    "style":"emphasis",
                    "minHeight":"44px",
                    "verticalContentAlignment":"Center",
                    "selectAction":{"type":"Action.OpenUrl","url":url},
                    "items":[
                        {
                            "type":"TextBlock",
                            "text": title,
                            "wrap": False,
                            "maxLines": 1,
                            "horizontalAlignment":"Center",
                            "weight":"Bolder",
                            "spacing":"None"
                        }
                    ]
                }
            ]
        }


    def row2(c1, c2) -> dict:
        return {"type":"ColumnSet","columns":[c1,c2],"spacing":"Small"}

    # 그룹핑
    buckets: dict[str, list[tuple[str,str]]] = {}
    for aliases, title, url in quick_links:
        buckets.setdefault(cat(aliases, title, url), []).append((title, url))

    order = ["SCM","마스터/시스템","리포트/분석","개발","지식/협업","VOC/요청","생활","기타"]
    body: list[dict] = [
        {"type":"TextBlock","text":"🧭 바로가기 모음", "size":"Large", "weight":"Bolder"},
        {"type":"TextBlock","text":"키워드만 쳐도 열리고, ‘바로가기’면 전체 목록이 떠요.", "wrap":True, "spacing":"Small", "isSubtle":True},
        {"type":"TextBlock","text":"", "separator": True, "spacing":"Medium"},
    ]

    for c in order:
        items = buckets.get(c) or []
        if not items:
            continue

        body.append({"type":"TextBlock","text":f"📌 {c}", "weight":"Bolder", "wrap":True, "spacing":"Medium"})

        # 2열 버튼 레이아웃
        for i in range(0, len(items), 2):
            t1,u1 = items[i]
            t2,u2 = items[i+1] if i+1 < len(items) else (None, None)
            body.append(row2(col_button(t1,u1), col_button(t2,u2)))

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"🏠 홈","data":{"action":"HOME"}},
        ],
    }          


def build_feedback_actions(request_id: str) -> List[dict]:
    return [
        {"type": "Action.Submit", "title": "👍 도움됨", "data": {"action": "FEEDBACK_LIKE", "request_id": request_id}},
        {"type": "Action.Submit", "title": "👎 아쉬움", "data": {"action": "FEEDBACK_DISLIKE", "request_id": request_id}},
    ]


def build_feedback_card(request_id: str) -> dict:
    return {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.3",
        "body": [
            {"type": "TextBlock", "text": "답변이 도움이 되었나요?", "weight": "Bolder", "wrap": True},
            {"type": "TextBlock", "text": f"요청 ID: {request_id}", "isSubtle": True, "spacing": "Small", "wrap": True},
        ],
        "actions": build_feedback_actions(request_id),
    }


def build_feedback_reason_card(request_id: str) -> dict:
    reasons = [
        ("최신 문서 아님", "stale_doc"),
        ("엉뚱한 문서 찾음", "wrong_doc"),
        ("SQL 조회 필요", "should_use_sql"),
        ("답이 모호함", "vague_answer"),
        ("기타", "other"),
    ]
    return {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.3",
        "body": [
            {"type": "TextBlock", "text": "아쉬웠던 이유를 선택해 주세요.", "weight": "Bolder", "wrap": True},
            {
                "type": "Input.Text",
                "id": "memo",
                "isMultiline": True,
                "placeholder": "추가 메모(선택)",
            },
        ],
        "actions": [
            {
                "type": "Action.Submit",
                "title": title,
                "data": {"action": "FEEDBACK_REASON_SUBMIT", "request_id": request_id, "reason_code": reason_code},
            }
            for title, reason_code in reasons
        ],
    }
