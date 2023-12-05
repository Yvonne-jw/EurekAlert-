/****** Script for SelectTopNRows command from SSMS  ******/
drop table if exists  [projectdb_eurekalert_2023mar].[dbo].[EurekAlert_metadata_clean]
select *
into [projectdb_eurekalert_2023mar].[dbo].[EurekAlert_metadata_clean]
from [projectdb_eurekalert_2023mar].[dbo].[EurekAlert_metadata]

--replace the '|' into '\'
update [projectdb_eurekalert_2023mar].[dbo].[EurekAlert_metadata_clean]
set title = REPLACE(title,'|','\')
    ,description = REPLACE(description,'|','\')
	,keywords = REPLACE(keywords,'|','\')
	,funder = REPLACE(funder,'|','\')
	,journal = REPLACE(journal,'|','\')
	,type = REPLACE(type,'|','\')
	,institution =  REPLACE(institution,'|','\')
	,meeting = REPLACE(meeting,'|','\')
	,original_source = REPLACE(original_source,'|','\')

drop table if exists  [projectdb_eurekalert_2023mar].[dbo].[EurekAlert_fulltext_clean]
select euid,REPLACE(content,'|','\') as content,image_id,REPLACE(fulltext_urls,'|','\') as fulltext_urls
into [projectdb_eurekalert_2023mar].[dbo].[EurekAlert_fulltext_clean]
from [projectdb_eurekalert_2023mar].[dbo].[EurekAlert_fulltext]
where content like '%|%' or fulltext_urls like '%|%'
--2045 number

update [projectdb_eurekalert_2023mar].[dbo].[EurekAlert_instituion_id]
set institution = REPLACE(institution,'|','\')

update [projectdb_eurekalert_2023mar].[dbo].[EurekAlert_keyword_id]
set keyword = REPLACE(keyword,'|','\')

update [projectdb_eurekalert_2023mar].[dbo].[EurekAlert_url_id]
set fulltexturl = REPLACE(fulltexturl,'|','\')


--replace the '\n' into '  '
update [projectdb_eurekalert_2023mar].[dbo].[EurekAlert_metadata_clean]
set description = REPLACE(description,'\n','  ')

update [projectdb_eurekalert_2023mar].[dbo].[EurekAlert_fulltext_clean]
set content = REPLACE(content,'\n','  ')

