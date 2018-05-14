

--==========创建临时表01_START=================================================================--;
--==========获取用户安装基本信息=====================================================--;
-- SELECT COUNT(*) FROM TMP.LAB_EVT_FB_REPAIR_TMP A;
DROP TABLE IF EXISTS TMP.LAB_EVT_FB_REPAIR_TMP ;
CREATE TABLE TMP.LAB_EVT_FB_REPAIR_TMP AS
SELECT ACC_NBR,
       REPAIR_OPER,
       REPAIR_OPER_NAME,
       FIRST_RECEPT_TIME,
       ARCHIVE_TIME
FROM (SELECT A.CLOGCODE AS ACC_NBR,
               A.REPAIROPER AS REPAIR_OPER,
               A.REPAIROPERNAME AS REPAIR_OPER_NAME,
               A.FIRSTRECEPTTIME AS FIRST_RECEPT_TIME,
               A.ARCHIVETIME AS ARCHIVE_TIME,
               ROW_NUMBER() OVER(PARTITION BY A.CLOGCODE ORDER BY A.RECEPTTIME DESC) RM
          FROM INF.INF_FZ_RPT_SVR_OP_BASE_DAY A
          WHERE A.P_DAY_ID='20180422' ) X
WHERE RM = 1
;

--==========创建临时表1_END  =================================================================--;


--==========创建临时表02_START=================================================================--;
--==========创建用户资料临时表=====================================================--;
-- SELECT COUNT(*) FROM TMP.ASS_PRD_PROD_INST_TMP A;
DROP TABLE IF EXISTS TMP.ASS_PRD_PROD_INST_TMP ;
CREATE TABLE TMP.ASS_PRD_PROD_INST_TMP AS
SELECT A.PROD_INST_ID,
       A.ACC_NBR,
       A.LAN_ID
  FROM TST.ASS_PRD_PROD_INST_D A
  WHERE A.P_DAY_ID='20180422'
  -- AND A.END_MONTH >= '201804'
   -- AND SUBSTR(A.BEG_TIME,1,8)<= '20180422'
   -- AND SUBSTR(A.END_TIME,1,8) > '20180422'
;

--==========创建临时表2_END  =================================================================--;


--==========创建临时表03_START=================================================================--;
--==========创建汇总临时表=====================================================--;
-- SELECT COUNT(*) FROM TMP.LAB_EVT_FB_REPAIR_TMP2 A;
DROP TABLE IF EXISTS TMP.LAB_EVT_FB_REPAIR_TMP2 ;
CREATE TABLE TMP.LAB_EVT_FB_REPAIR_TMP2 AS
SELECT LAN_ID,
       PROD_INST_ID,
       ACC_NBR,
       REPAIR_OPER,
       REPAIR_OPER_NAME,
       FIRST_RECEPT_TIME,
       ARCHIVE_TIME
       FROM (
SELECT LAN_ID,
       PROD_INST_ID,
       A.ACC_NBR,
       REPAIR_OPER,
       REPAIR_OPER_NAME,
       FIRST_RECEPT_TIME,
       ARCHIVE_TIME,
       ROW_NUMBER() OVER(PARTITION BY B.PROD_INST_ID ORDER BY A.ARCHIVE_TIME DESC) RM
  FROM TMP.LAB_EVT_FB_REPAIR_TMP A
  LEFT JOIN TMP.ASS_PRD_PROD_INST_TMP B
    ON A.ACC_NBR = B.ACC_NBR
) X
WHERE PROD_INST_ID IS NOT NULL AND RM=1
;

--==========创建临时表3_END  =================================================================--;



--==========创建临时表02_START=================================================================--;
--==========取出目标拉链表已有，且需要更新的数据，修改END_TIME和END_MONTH=================================================================--;
-- SELECT COUNT(*) FROM TMP.LAB_EVT_FB_REPAIR_TMP3 A;
DROP TABLE IF EXISTS TMP.LAB_EVT_FB_REPAIR_TMP3 ;
CREATE TABLE TMP.LAB_EVT_FB_REPAIR_TMP3 AS
SELECT from_unixtime(unix_timestamp(), 'yyyyMMddHHmmss') AS ETL_TIME,
       BEG_TIME ,
       '20180422' AS END_TIME,
       '201804' AS END_MONTH,
       LAN_ID,
       PROD_INST_ID,
       ACC_NBR,
       REPAIR_OPER,
       REPAIR_OPER_NAME,
       FIRST_RECEPT_TIME,
       ARCHIVE_TIME,
       ROWID AS ROW_ID
  FROM TST.LAB_EVT_FB_REPAIR_D A
 WHERE A.P_DAY_ID='20180421'
 AND EXISTS (SELECT 1
          FROM LAB_EVT_FB_REPAIR_TMP2 B
         WHERE A.PROD_INST_ID = B.PROD_INST_ID)
;


--==========删除目标表中在新增数据中已有的数据_START=================================================================--;
-- DELETE FROM TST.LAB_EVT_FB_REPAIR_D A
 -- WHERE  END_MONTH = 400012
   -- AND EXISTS(SELECT 1 FROM TMP.LAB_EVT_FB_REPAIR_TMP3 B WHERE A.ROWID = B.ROW_ID)
-- ;

--==========删除目标表中在新增数据中已有的数据_END  =================================================================--;


--==========插入变更历史数据到目标表中_START=================================================================--;
ALTER TABLE TST.LAB_EVT_FB_REPAIR_D DROP IF EXISTS PARTITION(P_DAY_ID='20180422');
INSERT OVERWRITE TABLE TST.LAB_EVT_FB_REPAIR_D PARTITION(P_DAY_ID='20180422',P_LAN_ID)
SELECT ETL_TIME
      ,BEG_TIME
      ,END_TIME
      ,END_MONTH
      ,LAN_ID
      ,PROD_INST_ID
      ,ACC_NBR
      ,REPAIR_OPER
      ,REPAIR_OPER_NAME
      ,FIRST_RECEPT_TIME
      ,ARCHIVE_TIME
FROM TMP.LAB_EVT_FB_REPAIR_TMP3
;

--==========插入变更历史数据到目标表中_END  =================================================================--;


--==========插入新增数据到目标表中_START=================================================================--;
INSERT INTO TABLE TST.LAB_EVT_FB_REPAIR_D
SELECT from_unixtime(unix_timestamp(), 'yyyyMMddHHmmss') AS ETL_TIME
      ,'20180422' AS BEG_TIME
      ,'40001231' AS END_TIME
      ,'400012' AS END_MONTH
      ,LAN_ID
      ,PROD_INST_ID
      ,ACC_NBR
      ,REPAIR_OPER
      ,REPAIR_OPER_NAME
      ,FIRST_RECEPT_TIME
      ,ARCHIVE_TIME
FROM TMP.LAB_EVT_FB_REPAIR_TMP2
;





