


DROP TABLE bloodtree_in;
create table bloodtree_in(tab VARCHAR(200),section int,father VARCHAR(200),son VARCHAR(200));


DROP TABLE bloodtree_out;
create table bloodtree_out(tab VARCHAR(200),father VARCHAR(200),son VARCHAR(200));


DELETE FROM TEST.bloodtree_out WHERE tab='9527';
INSERT INTO TEST.bloodtree_out(tab,father,son) SELECT tab,father,son FROM TEST.bloodtree_in WHERE father<>'' GROUP BY tab,father,son ORDER BY tab,son,father;



-- SELECT * from bloodtree_in;
-- SELECT * from bloodtree_out;

-- 接口表
SELECT a.father from
(SELECT father FROM TEST.bloodtree_out group by father) a
LEFT JOIN
(SELECT son FROM TEST.bloodtree_out group by son) b
on a.father=b.son
where b.son is null
order by a.father
;
-- 中间表
SELECT tmp FROM(
SELECT a.father as tmp from
(SELECT father FROM TEST.bloodtree_out group by father) a
LEFT JOIN
(SELECT son FROM TEST.bloodtree_out group by son) b
on a.father=b.son
where b.son is not null
UNION
SELECT a.son as tmp from
(SELECT son FROM TEST.bloodtree_out group by son) a
LEFT JOIN
(SELECT father FROM TEST.bloodtree_out group by father) b
on a.son=b.father
where b.father is not null
) x
order by tmp
;

-- 结果表
SELECT a.son from
(SELECT son FROM TEST.bloodtree_out group by son) a
LEFT JOIN
(SELECT father FROM TEST.bloodtree_out group by father) b
on a.son=b.father
where b.father is null
order by a.son
;


