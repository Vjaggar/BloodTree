
--1.取非熟卡非激活,新装已竣工用户
-- SELECT COUNT(*) FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D01 A; 4172
DROP TABLE IF EXISTS TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D01;
CREATE TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D01
    AS
SELECT
       DAY_ID
      ,MON_ID
      ,LAN_ID
      ,ETL_TIME
      ,ORDER_ITEM_ID
      ,PROD_INST_ID
      ,STD_PROD_ID
      ,STD_REGION_ID
      ,ACC_NBR
      ,PRD_COMPLETE_IN
      ,DEV_STAFF_ID
      ,CREATE_STAFF_ID
      ,CUST_ORDER_ID
      ,ADDRESS_ID
      ,CHANNEL_ID
      ,CREATE_POST
      ,SRC_PROD_ID
FROM(
SELECT
 '20180409'                       AS  DAY_ID                --日期
,'201804'                       AS  MON_ID                --月份
,LAN_ID                --本地网
,from_unixtime(unix_timestamp(), 'yyyyMMddHHmmss')  AS  ETL_TIME              --数据加载时间
,ORDER_ITEM_ID                   AS  ORDER_ITEM_ID         --定单编号
,PROD_INST_ID                    AS  PROD_INST_ID          --标准产品实例编号
,STD_PROD_ID                     AS  STD_PROD_ID           --标准产品编码
,STD_REGION_ID                   AS  STD_REGION_ID         --标准营业区编码
,ACC_NBR                         AS  ACC_NBR               --接入号码
,'1'                               AS  PRD_COMPLETE_IN       --新装竣工标识
,COALESCE(DEV_STAFF_ID,'NA')     AS  DEV_STAFF_ID          --揽机人
,COALESCE(CREATE_STAFF_ID,'NA')  AS  CREATE_STAFF_ID       --操作员编号
,CUST_ORDER_ID         --客户订单编码
,ADDRESS_ID            --标准地址编码
,CHANNEL_ID            --营业受理渠道编码
,CREATE_POST           --操作员岗位编码
,SRC_PROD_ID           --源始产品编码
FROM TST.ASS_EVT_PROD_INST_D A  -- EDC:ASS.ASS_EVT_PROD_INST_D
WHERE A.P_DAY_ID='20180409'
AND NOT ((SRC_ORDER_TYPE_CD IN ('CRM77','CRM78') OR SK_ACTIVE_FLAG ='3') AND STD_PROD_ID LIKE '2%' OR SRC_ORDER_TYPE_CD = 'CRM80')
AND STD_ORDER_STATUS_CD ='11'
-- AND LAN_ID='${LAN_ID}'
AND MON_ID='201804'
AND STD_SERVICE_OFFER_ID='10'
AND (XNK_FLAG NOT IN ('1') OR XNK_FLAG IS NULL OR XNK_FLAG='')
AND SUBSTR(FINISH_DATE,1,8) = '20180409'
AND NOT EXISTS(SELECT 1 FROM TST.ASS_EVT_PROD_INST_D B
                 WHERE B.P_DAY_ID='20180409'
                 AND A.PROD_INST_ID=B.PROD_INST_ID
                 AND A.LAN_ID=B.LAN_ID
                 AND B.STD_ORDER_STATUS_CD='11'  --竣工
                 AND B.STD_SERVICE_OFFER_ID='10'  --新装
                 -- AND B.LAN_ID='${LAN_ID}'
                 AND B.MON_ID='201804'
                 AND B.DAY_ID='20180409'
                 AND (TRIM(B.SRC_ORDER_TYPE_CD) IS NULL OR TRIM(B.SRC_ORDER_TYPE_CD)='')
                 AND (TRIM(B.SRC_SERVICE_OFFER_ID) IS NULL OR TRIM(B.SRC_SERVICE_OFFER_ID)='')
                 AND (TRIM(B.SRC_ORDER_SRC_CD) IS NULL OR TRIM(B.SRC_ORDER_SRC_CD)='')
                 AND (TRIM(B.SK_ACTIVE_FLAG) IS NULL OR TRIM(B.SK_ACTIVE_FLAG)=''))                 --排除熟卡激活那一部分数据
) X
GROUP BY DAY_ID
        ,MON_ID
        ,LAN_ID
        ,ETL_TIME
        ,ORDER_ITEM_ID
        ,PROD_INST_ID
        ,STD_PROD_ID
        ,STD_REGION_ID
        ,ACC_NBR
        ,PRD_COMPLETE_IN
        ,DEV_STAFF_ID
        ,CREATE_STAFF_ID
        ,CUST_ORDER_ID
        ,ADDRESS_ID
        ,CHANNEL_ID
        ,CREATE_POST
        ,SRC_PROD_ID
;



--2.取熟卡激活,新装已竣工用户
-- 916
INSERT INTO TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D01
    SELECT
         DAY_ID
        ,MON_ID
        ,LAN_ID
        ,ETL_TIME
        ,ORDER_ITEM_ID
        ,PROD_INST_ID
        ,STD_PROD_ID
        ,STD_REGION_ID
        ,ACC_NBR
        ,PRD_COMPLETE_IN
        ,DEV_STAFF_ID
        ,CREATE_STAFF_ID
        ,CUST_ORDER_ID
        ,ADDRESS_ID
        ,CHANNEL_ID
        ,CREATE_POST
        ,SRC_PROD_ID
    FROM(
    SELECT
     '20180409'                       AS  DAY_ID                --日期
    ,'201804'                       AS  MON_ID                --月份
    ,LAN_ID                --本地网
    ,from_unixtime(unix_timestamp(), 'yyyyMMddHHmmss')  AS  ETL_TIME              --数据加载时间
    ,ORDER_ITEM_ID                   AS  ORDER_ITEM_ID         --定单编号
    ,PROD_INST_ID                    AS  PROD_INST_ID          --标准产品实例编号
    ,STD_PROD_ID                     AS  STD_PROD_ID           --标准产品编码
    ,STD_REGION_ID                   AS  STD_REGION_ID         --标准营业区编码
    ,ACC_NBR                         AS  ACC_NBR               --接入号码
    ,'1'                               AS  PRD_COMPLETE_IN       --新装竣工标识
    ,COALESCE(DEV_STAFF_ID,'NA')     AS  DEV_STAFF_ID          --揽机人
    ,COALESCE(CREATE_STAFF_ID,'NA')  AS  CREATE_STAFF_ID       --操作员编号
    ,CUST_ORDER_ID         --客户订单编码
    ,ADDRESS_ID            --标准地址编码
    ,CHANNEL_ID            --营业受理渠道编码
    ,CREATE_POST           --操作员岗位编码
    ,SRC_PROD_ID           --源始产品编码
    FROM TST.ASS_EVT_PROD_INST_D
    WHERE P_DAY_ID='20180409'
    AND STD_ORDER_STATUS_CD='11'  --竣工
    AND   STD_SERVICE_OFFER_ID='10' --新装
    -- AND   LAN_ID='${LAN_ID}'
    AND MON_ID='201804'
    AND DAY_ID='20180409'
    AND (TRIM(SRC_ORDER_TYPE_CD) IS NULL OR TRIM(SRC_ORDER_TYPE_CD)='')
    AND (TRIM(SRC_SERVICE_OFFER_ID) IS NULL OR TRIM(SRC_SERVICE_OFFER_ID)='')
    AND (TRIM(SRC_ORDER_SRC_CD) IS NULL OR TRIM(SRC_ORDER_SRC_CD)='')
    AND (TRIM(SK_ACTIVE_FLAG)   IS NULL OR TRIM(SK_ACTIVE_FLAG)='')
    ) X
    GROUP BY DAY_ID
            ,MON_ID
            ,LAN_ID
            ,ETL_TIME
            ,ORDER_ITEM_ID
            ,PROD_INST_ID
            ,STD_PROD_ID
            ,STD_REGION_ID
            ,ACC_NBR
            ,PRD_COMPLETE_IN
            ,DEV_STAFF_ID
            ,CREATE_STAFF_ID
            ,CUST_ORDER_ID
            ,ADDRESS_ID
            ,CHANNEL_ID
            ,CREATE_POST
            ,SRC_PROD_ID
;


--局方提出修改需求：目前每日的宽带新发展数据里把ITV礼包每月送2小时上网时长的用户统计进来了,按照需求,需要将这部分用户剔除掉,不算新发展量。具体涉及三个销售品
--为Z111168511、Z111168513、Z111168510。修改日期：20170531,修改人：YS
  --START--
--3.取出当天发展用户与销售品表关联
-- SELECT COUNT(*) FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D02 A; 19825
DROP TABLE IF EXISTS TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D02;
CREATE TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D02
  AS
SELECT
     A1.STD_OFFER_ID   AS STD_OFFER_ID            --标准销售品标识
    ,A2.STD_PROD_ID    AS STD_PROD_ID             --标准产品ID
    ,A1.OBJ_ID         AS PROD_INST_ID            --标准产品实例ID
    ,A1.EFF_DATE       AS OFR_EFF_DATE            --销售品生效时间
    ,A1.EXP_DATE       AS OFR_EXP_DATE            --销售品失效时间
    ,A1.SRC_OFFER_ID   AS SRC_OFFER_ID            --源销售品ID
    ,A1.LAN_ID
FROM TST.ASS_OFR_OFFER_INST_D A1  -- EDC:ASS.ASS_OFR_OFFER_INST_L
  INNER JOIN TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D01 A2
  ON A1.OBJ_ID=A2.PROD_INST_ID
  AND A1.LAN_ID=A2.LAN_ID
  WHERE A1.P_DAY_ID='20180409'
  -- AND SUBSTR(A1.BEG_TIME,1,8)<'20180410'
  -- AND SUBSTR(A1.END_TIME,1,8)>='20180410'
  -- AND A1.END_MONTH>='201804'
  -- AND A1.LAN_ID='${LAN_ID}'
  AND A1.OBJ_TYPE='100000'
;

--4.取出用户的主销售品
-- SELECT COUNT(*) FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D03 A; 19695
DROP TABLE IF EXISTS TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D03;
CREATE TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D03
    AS
SELECT
      A1.PROD_INST_ID  AS PROD_INST_ID  --标准产品实例ID
     ,A1.STD_OFFER_ID  AS STD_OFFER_ID  --标准销售品ID
     ,A2.SRC_OFFER_ID  AS SRC_OFFER_ID  --源销售品ID
     ,A2.OFFER_GRADE   AS OFFER_GRADE   --套餐档次
     ,ROW_NUMBER() OVER (PARTITION BY A1.PROD_INST_ID
                         ORDER BY NVL(A2.MAIN_FLAG,'-1') DESC,
                         CAST(((CASE WHEN A1.STD_PROD_ID LIKE '1303%' AND A1.STD_OFFER_ID LIKE '1301%'
                            THEN '-1'
                            ELSE NVL(A2.OFFER_GRADE,'0') END)) AS BIGINT)DESC,       --宽带套餐为天翼宽带加装ITV 优先级降低
                          A1.OFR_EFF_DATE ASC,
                          A2.SRC_OFFER_ID ASC) RN
      ,A1.LAN_ID
FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D02  A1
 INNER JOIN ZNYX_APPED.BSS3_CFG_SRC_OFFER_L A2  -- EDC:CFG.CFG_SRC_OFFER_L
    ON  A1.SRC_OFFER_ID = A2.SRC_OFFER_ID
    AND A2.P_DAY_ID='20180409'
    -- AND SUBSTR(A2.BEG_TIME,1,8)<'20180410'
    -- AND SUBSTR(A2.END_TIME,1,8)>='20180410'
    -- AND A2.END_MONTH>='201804'
WHERE SUBSTR(A1.OFR_EXP_DATE,1,8) >= '20180409'
;


--5.剔除宽带用户办理了这三个套餐的当天发展用户

-- SELECT COUNT(*) FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D08 A; 5086
DROP TABLE IF EXISTS TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D08;
CREATE TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D08 AS
SELECT DAY_ID
      ,MON_ID
      ,LAN_ID
      ,ETL_TIME
      ,ORDER_ITEM_ID
      ,PROD_INST_ID
      ,STD_PROD_ID
      ,STD_REGION_ID
      ,ACC_NBR
      ,PRD_COMPLETE_IN
      ,DEV_STAFF_ID
      ,CREATE_STAFF_ID
      ,CUST_ORDER_ID
      ,ADDRESS_ID
      ,CHANNEL_ID
      ,CREATE_POST
      ,SRC_PROD_ID
FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D01 A1
   WHERE NOT EXISTS (SELECT 1 FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D03 A2
                 WHERE A1.PROD_INST_ID=A2.PROD_INST_ID
                 AND A1.LAN_ID=A2.LAN_ID
                 AND A2.SRC_OFFER_ID IN ('1168511', '1168513', '1168510')
                 AND A2.RN=1
                 AND A1.STD_PROD_ID LIKE '1303%'
                 AND A1.PRD_COMPLETE_IN=1)
;


--7.更改体验卡套餐未转正的当天发展用户的竣工标识为0
-- 34
INSERT INTO TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D08
SELECT
    DAY_ID                        --日期
    ,MON_ID                       --月份
    ,LAN_ID                       --本地网
    ,ETL_TIME                     --数据加载时间
    ,ORDER_ITEM_ID                --定单编号
    ,PROD_INST_ID                 --标准产品实例编号
    ,STD_PROD_ID                  --标准产品编码
    ,STD_REGION_ID                --标准营业区编码
    ,ACC_NBR                      --接入号码
    ,'0'   AS  PRD_COMPLETE_IN      --新装竣工标识
    ,DEV_STAFF_ID                 --揽机人
    ,CREATE_STAFF_ID              --操作员编号
    ,CUST_ORDER_ID         --客户订单编码
    ,ADDRESS_ID            --标准地址编码
    ,CHANNEL_ID            --营业受理渠道编码
    ,CREATE_POST           --操作员岗位编码
    ,SRC_PROD_ID           --源始产品编码
FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D08 A1
WHERE EXISTS(SELECT 1 FROM TST.LAB_EVT_PRD_TY_USER_SHARE_D A2  -- EDC:LAB.LAB_EVT_PRD_TY_USER_SHARE_D
               WHERE A2.P_DAY_ID='20180409'
               AND A2.IS_TRANS='0'
               AND A1.LAN_ID=A2.LAN_ID
               AND A1.PROD_INST_ID=A2.PROD_INST_ID
               -- AND A2.LAN_ID=${LAN_ID}
               )
AND  A1.PRD_COMPLETE_IN='1'
;


--8.删除发生变更的这一部分用户原来的标识
-- SELECT COUNT(*) FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D09 A; 5052
DROP TABLE IF EXISTS TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D09;
CREATE TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D09 AS
SELECT DAY_ID
      ,MON_ID
      ,LAN_ID
      ,ETL_TIME
      ,ORDER_ITEM_ID
      ,PROD_INST_ID
      ,STD_PROD_ID
      ,STD_REGION_ID
      ,ACC_NBR
      ,PRD_COMPLETE_IN
      ,DEV_STAFF_ID
      ,CREATE_STAFF_ID
      ,CUST_ORDER_ID
      ,ADDRESS_ID
      ,CHANNEL_ID
      ,CREATE_POST
      ,SRC_PROD_ID
FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D08 A1
WHERE NOT EXISTS(SELECT 1 FROM TST.LAB_EVT_PRD_TY_USER_SHARE_D A2
               WHERE A2.P_DAY_ID='20180409'
               AND A2.IS_TRANS=0
               AND A1.LAN_ID=A2.LAN_ID
               AND A1.PROD_INST_ID=A2.PROD_INST_ID
               -- AND   A2.LAN_ID=${LAN_ID}
               )
AND  A1.PRD_COMPLETE_IN=1
;


--8.取出已转正的体验卡用户
-- SELECT COUNT(*) FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D04 A; 26749
DROP TABLE IF EXISTS TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D04;
CREATE TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D04
    AS
    SELECT
          PROD_INST_ID
         ,TRNS_DATE     --转正日期
         ,LAN_ID
    FROM  TST.LAB_EVT_PRD_TY_USER_SHARE_D
    WHERE P_DAY_ID='20180409'
    AND IS_TRANS=1
    -- AND   LAN_ID='${LAN_ID}'
    GROUP BY PROD_INST_ID,TRNS_DATE,LAN_ID
;

--9.更新已经转正用户竣工标识
-- SELECT COUNT(*) FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D05 A; 3
DROP TABLE IF EXISTS TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D05;
CREATE TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D05
    AS
    SELECT
           B.TRNS_DATE              AS DAY_ID        --日期
          ,SUBSTR(B.TRNS_DATE,1,6)  AS MON_ID        --月份
          ,A.LAN_ID                                  --本地网
          ,A.ETL_TIME                                --数据加载时间
          ,A.ORDER_ITEM_ID                           --定单编号
          ,A.PROD_INST_ID                            --标准产品实例编号
          ,A.STD_PROD_ID                             --标准产品编码
          ,A.STD_REGION_ID                           --标准营业区编码
          ,A.ACC_NBR                                 --接入号码
          ,1                        AS     PRD_COMPLETE_IN         --新装竣工标识
          ,A.DEV_STAFF_ID                            --揽机人
          ,A.CREATE_STAFF_ID                         --操作员编号
          ,A.CUST_ORDER_ID         --客户订单编码
          ,A.ADDRESS_ID            --标准地址编码
          ,A.CHANNEL_ID            --营业受理渠道编码
          ,A.CREATE_POST           --操作员岗位编码
          ,A.SRC_PROD_ID           --源始产品编码
          FROM TST.LAB_EVT_PRD_COMPLETE_IN_D A
          INNER JOIN TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D04 B
          ON   A.PROD_INST_ID = B.PROD_INST_ID
          WHERE A.P_DAY_ID IN ('20180408')
          AND  A.PRD_COMPLETE_IN = 0
          -- AND  A.LAN_ID = '${LAN_ID}'
          -- AND A.MON_ID >= TO_NUMBER(TO_CHAR(ADD_MONTHS(TO_DATE(20180409, 'YYYYMMDD'),-6), 'YYYYMM'))
          AND A.MON_ID >= '201710'
          AND  NOT EXISTS(SELECT 1 FROM  TST.LAB_EVT_PRD_COMPLETE_IN_D C
                           WHERE C.P_DAY_ID IN ('20180408')
                           AND A.LAN_ID=C.LAN_ID
                           -- AND C.LAN_ID='${LAN_ID}'
                           -- AND C.MON_ID >= TO_NUMBER(TO_CHAR(ADD_MONTHS(TO_DATE(20180409, 'YYYYMMDD'),-6), 'YYYYMM'))
                           AND C.MON_ID >= '201710'
                           AND C.PRD_COMPLETE_IN=1
                           AND A.PROD_INST_ID=C.PROD_INST_ID)
;


-- DELETE FROM  TST.LAB_EVT_PRD_COMPLETE_IN_D
  -- WHERE  MON_ID=201804
  -- AND    DAY_ID=20180409
  -- AND    LAN_ID='${LAN_ID}'
  -- ;


--10.插入当天新增发展用户
ALTER TABLE TST.LAB_EVT_PRD_COMPLETE_IN_D DROP IF EXISTS PARTITION(P_DAY_ID='20180409');
INSERT OVERWRITE TABLE TST.LAB_EVT_PRD_COMPLETE_IN_D PARTITION(P_DAY_ID='20180409',P_LAN_ID)
SELECT day_id
      ,mon_id
      ,lan_id
      ,etl_time
      ,order_item_id
      ,cust_order_id
      ,prod_inst_id
      ,src_prod_id
      ,std_prod_id
      ,std_region_id
      ,address_id
      ,channel_id
      ,acc_nbr
      ,prd_complete_in
      ,dev_staff_id
      ,create_post
      ,create_staff_id
      ,LAN_ID
FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D09 A1
UNION ALL
SELECT day_id
      ,mon_id
      ,lan_id
      ,etl_time
      ,order_item_id
      ,cust_order_id
      ,prod_inst_id
      ,src_prod_id
      ,std_prod_id
      ,std_region_id
      ,address_id
      ,channel_id
      ,acc_nbr
      ,prd_complete_in
      ,dev_staff_id
      ,create_post
      ,create_staff_id
      ,LAN_ID
FROM TST.LAB_EVT_PRD_COMPLETE_IN_D A2
WHERE A2.P_DAY_ID='20180408'
;


--11.由于体验卡转正新增发展用户,转正时间为竣工时间
INSERT INTO TABLE TST.LAB_EVT_PRD_COMPLETE_IN_D PARTITION(P_DAY_ID='20180409',P_LAN_ID)
SELECT day_id
      ,mon_id
      ,lan_id
      ,etl_time
      ,TRIM(order_item_id)
      ,cust_order_id
      ,prod_inst_id
      ,src_prod_id
      ,std_prod_id
      ,std_region_id
      ,address_id
      ,channel_id
      ,acc_nbr
      ,prd_complete_in
      ,dev_staff_id
      ,create_post
      ,create_staff_id
      ,LAN_ID
FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D05 A1
;


--揽机人标识补丁程START
--获取最近5天内订单揽机人信息发生变化的用户
-- SELECT COUNT(*) FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D06 A; 1
DROP TABLE IF EXISTS TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D06;
CREATE TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D06
    AS
SELECT PROD_INST_ID,
           ORDER_ITEM_ID,
           DEV_STAFF_ID1,
           DEV_STAFF_ID2,
           A.LAN_ID
      FROM (SELECT A.PROD_INST_ID,
                   A.ORDER_ITEM_ID,
                   A.DEV_STAFF_ID  AS DEV_STAFF_ID1,
                   B.DEV_STAFF_ID  AS DEV_STAFF_ID2,
                   A.LAN_ID
            FROM TST.LAB_EVT_PRD_COMPLETE_IN_D A
            LEFT JOIN (SELECT B.ORDER_ITEM_ID,
                              B.DEV_STAFF_ID,
                              ROW_NUMBER() OVER(PARTITION BY ORDER_ITEM_ID ORDER BY CREATE_DATE DESC) SN,
                              CASE
                              WHEN LAN_ID = '731' THEN '11'
                              WHEN LAN_ID = '733' THEN '13'
                              WHEN LAN_ID = '732' THEN '12'
                              WHEN LAN_ID = '734' THEN '14'
                              WHEN LAN_ID = '739' THEN '19'
                              WHEN LAN_ID = '730' THEN '10'
                              WHEN LAN_ID = '736' THEN '16'
                              WHEN LAN_ID = '744' THEN '21'
                              WHEN LAN_ID = '737' THEN '17'
                              WHEN LAN_ID = '735' THEN '15'
                              WHEN LAN_ID = '746' THEN '23'
                              WHEN LAN_ID = '745' THEN '22'
                              WHEN LAN_ID = '738' THEN '18'
                              WHEN LAN_ID = '743' THEN '20'
                              ELSE  '24'
                              END AS LAN_ID
                        FROM  TST.MID_EVT_DEV_STAFF_INFO_HIS_D B  -- MID.MID_EVT_DEV_STAFF_INFO_HIS_D
                        WHERE B.P_DAY_ID='20180409'
                          AND B.DEV_STAFF_TYPE IN ('1000','002')
                          -- AND B.EDC_DAY_ID>='20180404'             -- MID表的增量怎么找
                          -- AND B.LAN_ID='${LAN_ID2}'
                          ) B
            ON A.ORDER_ITEM_ID = B.ORDER_ITEM_ID
            AND A.LAN_ID=B.LAN_ID
           AND B.SN = 1
         WHERE A.P_DAY_ID='20180409'
           AND A.MON_ID >= '201803'
           AND A.DAY_ID >= '20180404'
           -- AND A.LAN_ID='${LAN_ID}'
           AND A.PRD_COMPLETE_IN = 1) A
WHERE A.DEV_STAFF_ID1 <> A.DEV_STAFF_ID2
-- GROUP BY 1,2,3,4
GROUP BY PROD_INST_ID,
         ORDER_ITEM_ID,
         DEV_STAFF_ID1,
         DEV_STAFF_ID2,
         A.LAN_ID
;


--获取要发生变化的目标数据
-- SELECT COUNT(*) FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D07 A; 1
DROP TABLE IF EXISTS TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D07;
CREATE TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D07
    AS
      SELECT  A.DAY_ID                --日期
             ,A.MON_ID                --月份
             ,A.LAN_ID                --本地网
             ,A.ETL_TIME              --数据加载时间
             ,A.ORDER_ITEM_ID         --定单编号
             ,A.PROD_INST_ID          --标准产品实例编号
             ,A.STD_PROD_ID           --标准产品编码
             ,A.STD_REGION_ID         --标准营业区编码
             ,A.ACC_NBR               --接入号码
             ,A.PRD_COMPLETE_IN       --新装受理标识
             ,COALESCE(B.DEV_STAFF_ID2,A.DEV_STAFF_ID) AS DEV_STAFF_ID          --揽机人
             ,A.CREATE_STAFF_ID       --操作员编号
             ,A.CUST_ORDER_ID         --客户订单编码
             ,A.ADDRESS_ID            --标准地址编码
             ,A.CHANNEL_ID            --营业受理渠道编码
             ,A.CREATE_POST           --操作员岗位编码
             ,A.SRC_PROD_ID           --源始产品编码
        FROM TST.LAB_EVT_PRD_COMPLETE_IN_D  A
        LEFT JOIN TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D06 B
               ON A.PROD_INST_ID=B.PROD_INST_ID
              AND A.ORDER_ITEM_ID=B.ORDER_ITEM_ID
              AND A.LAN_ID=B.LAN_ID
        WHERE A.P_DAY_ID='20180409'
          AND A.MON_ID >= '201803'
          AND A.DAY_ID >= '20180404'
          -- AND  A.LAN_ID='${LAN_ID}'
          AND  A.PRD_COMPLETE_IN = 1
          AND  EXISTS(SELECT 1 FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D06 C
                               WHERE A.PROD_INST_ID=C.PROD_INST_ID
                                 AND A.ORDER_ITEM_ID=C.ORDER_ITEM_ID
                                 AND A.LAN_ID=C.LAN_ID)
;


--删除揽机人信息发生变更的这一部分用户
-- SELECT COUNT(*) FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D10 A; 24509
DROP TABLE IF EXISTS TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D10;
CREATE TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D10 AS
SELECT
DAY_ID
,MON_ID
,LAN_ID
,ETL_TIME
,TRIM(ORDER_ITEM_ID) AS ORDER_ITEM_ID
,PROD_INST_ID
,STD_PROD_ID
,STD_REGION_ID
,ACC_NBR
,PRD_COMPLETE_IN
,DEV_STAFF_ID
,CREATE_STAFF_ID
,CUST_ORDER_ID
,ADDRESS_ID
,CHANNEL_ID
,CREATE_POST
,SRC_PROD_ID
FROM TST.LAB_EVT_PRD_COMPLETE_IN_D A
WHERE A.P_DAY_ID='20180409'
AND NOT EXISTS(SELECT 1 FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D07 B
                      WHERE  A.PROD_INST_ID=B.PROD_INST_ID
                        AND  A.ORDER_ITEM_ID=B.ORDER_ITEM_ID
                        AND A.LAN_ID=B.LAN_ID)
                AND    A.MON_ID >= '201803'
                AND    A.DAY_ID >= '20180404'
                -- AND    A.LAN_ID='${LAN_ID}'
                AND    A.PRD_COMPLETE_IN = 1
;


--将变更后的揽机人信息插回目标表
-- SELECT COUNT(*) FROM TST.LAB_EVT_PRD_COMPLETE_IN_D A WHERE P_DAY_ID='20180409' AND P_LAN_ID='11' AND DAY_ID='20180409'; 5055
ALTER TABLE TST.LAB_EVT_PRD_COMPLETE_IN_D DROP IF EXISTS PARTITION(P_DAY_ID='20180409');
INSERT OVERWRITE TABLE TST.LAB_EVT_PRD_COMPLETE_IN_D PARTITION(P_DAY_ID='20180409',P_LAN_ID)
SELECT day_id
      ,mon_id
      ,lan_id
      ,etl_time
      ,TRIM(order_item_id)
      ,cust_order_id
      ,prod_inst_id
      ,src_prod_id
      ,std_prod_id
      ,std_region_id
      ,address_id
      ,channel_id
      ,acc_nbr
      ,prd_complete_in
      ,dev_staff_id
      ,create_post
      ,create_staff_id
      ,LAN_ID
FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D07 A1
UNION ALL
SELECT day_id
      ,mon_id
      ,lan_id
      ,etl_time
      ,TRIM(order_item_id)
      ,cust_order_id
      ,prod_inst_id
      ,src_prod_id
      ,std_prod_id
      ,std_region_id
      ,address_id
      ,channel_id
      ,acc_nbr
      ,prd_complete_in
      ,dev_staff_id
      ,create_post
      ,create_staff_id
      ,LAN_ID
FROM TMP.TMP_LAB_EVT_PRD_COMPLETE_IN_D10 A2
;





