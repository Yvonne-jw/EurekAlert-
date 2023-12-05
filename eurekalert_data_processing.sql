/**--processing of eurekalert dataset
     table1: EurekAlert_keywords: 
	         euid|keyword_seq_1|keywords_seq_2|keyword_id
	 table2: EurekAlert_keyword_id:
	         keyword_id|keyword
	 table3: url_id:
	         url_id|url
	 table4: eurekalert_url
	         euid|url_seq|url_id
	 table4:fulltext_doi:   --92373
	        euid|doi
     table5: new_twid_euid  --466505
	        tweet_id|longurl|euid

**/
---keywords_split and make keywords table
drop table if exists #keywords
select euid,title,keywords 
into #keywords 
from EurekAlert_metadata_

--keywords first split/ seq
drop table if exists #keywords_firstsplit_seq
SELECT euid
       ,ROW_NUMBER() OVER(PARTITION BY euid ORDER BY Numbers.n) AS keywords_seq_1
	   ,v.value as keyword
	   ,keywords
into #keywords_firstsplit_seq
FROM #keywords
cross apply string_split(keywords,',') v
JOIN (SELECT 1 n  ) Numbers  --UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
ON LEN(keywords) - LEN(REPLACE(keywords, ',', '')) > Numbers.n - 1;

drop table if exists #losekeyword
select * 
into #losekeyword
from #keywords
where euid not in
(SELECT distinct euid
  FROM #keywords_firstsplit_seq)


insert into #keywords_firstsplit_seq
select euid,keywords_seq_1 = 1, keywords,keywords 
from #losekeyword
--9630;


--euid+keyseq
update #keywords_firstsplit_seq
set keyword = SUBSTRING(keyword,1,len(keyword)) 

ALTER TABLE #keywords_firstsplit_seq
add euid_keyseq nvarchar(50);

update #keywords_firstsplit_seq
set euid_keyseq = CAST(euid AS NVARCHAR(7))+'_'+CAST(keywords_seq_1 AS NVARCHAR(3))

--构建新关键词
ALTER TABLE #keywords_firstsplit_seq
add newkeywords nvarchar(1000);

update #keywords_firstsplit_seq
set newkeywords = concat(SUBSTRING(keyword,CHARINDEX('/',keyword)+1,len(keyword)),'',SUBSTRING(keyword,0,CHARINDEX('/',keyword)))


--second split
drop table if exists #keywords_secondsplit_seq
SELECT euid
       ,keywords_seq_1
       ,ROW_NUMBER() OVER(PARTITION BY euid_keyseq ORDER BY Numbers.n asc) AS keywords_seq_2
	   ,v.value as keyword
	   ,newkeywords
	   ,euid_keyseq
into #keywords_secondsplit_seq
FROM #keywords_firstsplit_seq
cross apply string_split(newkeywords,'/') v
JOIN (SELECT 1 n  ) Numbers  --UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
ON LEN(newkeywords) - LEN(REPLACE(newkeywords, '/', '')) > Numbers.n - 1;
--1 hour
--lost keywords
insert into #keywords_secondsplit_seq
select euid
       ,keywords_seq_1
	   ,keywords_seq_2 = 1
	   ,newkeywords as keyword
	   ,newkeywords
	   ,euid_keyseq
from #keywords_firstsplit_seq
where euid not in
(SELECT distinct [euid]
  FROM #keywords_secondsplit_seq)
--1152


drop table if exists #eurekalert_keywords
select euid
       ,keywords_seq_1
	   ,ROW_NUMBER() OVER(PARTITION BY euid_keyseq ORDER BY keywords_seq_2 desc) AS keywords_seq_2
	   ,keyword
	   ,newkeywords
into #eurekalert_keywords
from #keywords_secondsplit_seq
--7327256;00:27

--add keyword_id
drop table if exists #EurekAlert_keyword_id
select distinct keyword
into #EurekAlert_keyword_id
from #eurekalert_keywords
--6994;00:01

ALTER TABLE #EurekAlert_keyword_id
ADD keyword_id INT IDENTITY(1,1)

select * 
into [projectdb_eurekalert].[dbo].[EurekAlert_keyword_id]
from #EurekAlert_keyword_id

--构建关键词表
select a.euid
       ,a.keywords_seq_1
	   ,a.keywords_seq_2
	   ,b.keyword_id
into [projectdb_eurekalert].[dbo].[EurekAlert_keywords]
from #eurekalert_keywords a
inner join EurekAlert_keyword_id b
on a.keyword = b.keyword
--7327256;00:02




---link_split

drop table if exists #urls_firstsplit1
select t.euid,v.ResultsTable as fulltexturl --into #urls_firstsplit1
into #urls_firstsplit1
from [projectdb_eurekalert].[dbo].[EurekAlert_fulltext] t
cross apply SplitStringType(t.fulltext_urls,',http') v
where v.ResultsTable != ''
--clean data


update #urls_firstsplit1
set fulltexturl =SUBSTRING(fulltexturl,0,len(fulltexturl))--SUBSTRING(fulltexturl,19,len(fulltexturl)-19)--replace(fulltexturl,'"','')
where fulltexturl like '%,'


--distinct euid and url
drop table if exists #urls_firstsplit
select distinct * into  #urls_firstsplit from #urls_firstsplit1

--put url seq
drop table if exists #eurekalert_urls_seq
SELECT euid,
       ROW_NUMBER() OVER (PARTITION BY euid order by fulltexturl ) AS url_seq
	   ,fulltexturl
into #eurekalert_urls_seq
FROM #urls_firstsplit

--make url_id
select distinct fulltexturl into url_id from #urls_firstsplit1

ALTER TABLE url_id
ADD url_id INT IDENTITY(1,1) PRIMARY KEY

--table:eurekalert_url
select a.euid,url_seq ,b.url_id 
into eurekalert_urls
from #eurekalert_urls_seq a 
inner join  url_id b
on a.fulltexturl = b.fulltexturl
--880543  16sec

--** institution table
-- institution split
drop table if exists #institution_split
select t.euid,v.value as institution
into #institution_split
from EurekAlert_metadata t
cross apply string_split(t.institution,',') v
--where institution not like '%, and%' and institution not like '%, Japan%' and institution not like '%, Singapore%'
--and institution not like '%, Ltd%'and institution not like '%, Inc%'


--add institution_id
select distinct institution 
into EurekAlert_instituion_id 
from #institution_split
ALTER TABLE EurekAlert_instituion_id 
ADD institution_id INT IDENTITY(1,1) PRIMARY KEY

--creat table of eurekalert institution
select a.euid,a.institution_seq,b.institution_id 
into EurekAlert_institution
from (select euid
,ROW_NUMBER() over(partition by euid order by institution) institution_seq
,institution
from #institution_split) a
inner join EurekAlert_instituion_id  b
on a.institution  = b.institution 
