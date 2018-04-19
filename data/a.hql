CREATE TABLE TMP.TMP_LAB_EVT_PRD_COMPLETE_RELY_IN_D01 AS SELECT * FROM TMP.HAHATMP_LAB_EVT_PRD_COMPLETE_RELY_IN_D01 UNION ALL
SELECT DAY_ID
      ,MON_ID
      ,LAN_ID
      ,ETL_TIME
      ,ORDER_ITEM_ID
      ,PROD_INST_ID
      ,STD_PROD_ID
      ,STD_REGION_ID
      ,ACC_NBR
      ,PRD_COMPLETE_RELLY_IN
FROM(
SELECT
     '20180409'                           AS  DAY_ID                --日期
    ,'201804'                           AS  MON_ID                --月份
    ,A.LAN_ID                --本地网
    ,from_unixtime(unix_timestamp(), 'yyyyMMddHHmmss') AS  ETL_TIME              --数据加载时间
    ,COALESCE(A.ORDER_ITEM_ID ,B.ORDER_ITEM_ID,' ')     AS  ORDER_ITEM_ID         --定单编号
    ,COALESCE(A.PROD_INST_ID  ,B.PROD_INST_ID ,' ')     AS  PROD_INST_ID          --标准产品实例编号
    ,COALESCE(A.STD_PROD_ID   ,B.STD_PROD_ID  ,' ')     AS  STD_PROD_ID           --标准产品编号
    ,COALESCE(A.STD_REGION_ID,B.STD_REGION_ID,' ')      AS  STD_REGION_ID         --标准营业区编码
    ,COALESCE(A.ACC_NBR       ,B.ACC_NBR      ,' ')     AS  ACC_NBR               --接入号码
    ,CAST(NVL(A.PRD_COMPLETE_IN,0) AS BIGINT) - CAST(NVL(B.PRD_COMPLETE_OUT,0) AS BIGINT)  AS  PRD_COMPLETE_RELLY_IN   --竣工净增用户标识
FROM TST.LAB_EVT_PRD_COMPLETE_IN_D A
LEFT JOIN TST.LAB_EVT_PRD_COMPLETE_OUT_D B
ON A.PROD_INST_ID=B.PROD_INST_ID
AND A.ORDER_ITEM_ID=B.ORDER_ITEM_ID
AND A.LAN_ID=B.LAN_ID
AND B.P_DAY_ID='20180409'
AND B.DAY_ID='20180409'
AND B.MON_ID='201804'
WHERE A.P_DAY_ID='20180409'
AND A.DAY_ID='20180409'
AND A.MON_ID='201804'
UNION ALL
SELECT
     '20180409'                           AS  DAY_ID                --日期
    ,'201804'                           AS  MON_ID                --月份
    ,A.LAN_ID                --本地网
    ,from_unixtime(unix_timestamp(), 'yyyyMMddHHmmss') AS  ETL_TIME              --数据加载时间
    ,COALESCE(A.ORDER_ITEM_ID ,B.ORDER_ITEM_ID,' ')     AS  ORDER_ITEM_ID         --定单编号
    ,COALESCE(A.PROD_INST_ID  ,B.PROD_INST_ID ,' ')     AS  PROD_INST_ID          --标准产品实例编号
    ,COALESCE(A.STD_PROD_ID   ,B.STD_PROD_ID  ,' ')     AS  STD_PROD_ID           --标准产品编号
    ,COALESCE(A.STD_REGION_ID,B.STD_REGION_ID,' ')      AS  STD_REGION_ID         --标准营业区编码
    ,COALESCE(A.ACC_NBR       ,B.ACC_NBR      ,' ')     AS  ACC_NBR               --接入号码
    ,CAST(NVL(B.PRD_COMPLETE_IN,0) AS BIGINT) - CAST(NVL(A.PRD_COMPLETE_OUT,0) AS BIGINT) AS  PRD_COMPLETE_RELLY_IN   --竣工净增用户标识
FROM   TST.LAB_EVT_PRD_COMPLETE_OUT_D A
LEFT JOIN TST.LAB_EVT_PRD_COMPLETE_IN_D B
ON A.PROD_INST_ID=B.PROD_INST_ID
AND A.ORDER_ITEM_ID=B.ORDER_ITEM_ID
AND A.LAN_ID=B.LAN_ID
AND B.P_DAY_ID='20180409'
AND B.MON_ID='201804'
AND B.DAY_ID='20180409'
WHERE A.P_DAY_ID='20180409'
AND A.DAY_ID='20180409'
AND A.MON_ID='201804'
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
      ,PRD_COMPLETE_RELLY_IN
;






