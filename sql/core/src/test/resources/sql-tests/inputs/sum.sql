-- sum

-- null
select sum(cast(v as interval)) from VALUES ('1 seconds'), ('2 seconds'), (null) t(v) where v is null;

-- empty set
select sum(cast(v as interval)) from VALUES ('1 seconds'), ('2 seconds'), (null) t(v) where 1=0;

--
select sum(cast(v as interval)) from VALUES ('1 seconds'), ('2 seconds'), (null) t(v);
select sum(cast(v as interval)) from VALUES ('-1 seconds'), ('2 seconds'), (null) t(v);
select sum(cast(v as interval)) from VALUES ('-1 seconds'), ('-2 seconds'), (null) t(v);
select sum(cast(v as interval)) from VALUES ('-1 weeks'), ('2 seconds'), (null) t(v);

--group by
select
 i,
 sum(cast(v as interval))
from VALUES (1, '-1 weeks'), (2, '2 seconds'), (3, null), (1, '5 days') t(i, v)
group by i;

--having
select
 sum(cast(v as interval)) as sv
from VALUES (1, '-1 weeks'), (2, '2 seconds'), (3, null), (1, '5 days') t(i, v)
having sv is not null;

-- window
SELECT
 i,
 Sum(cast(v as interval)) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
FROM VALUES(1,'1 seconds'),(1,'2 seconds'),(2,NULL),(2,NULL) t(i,v);
