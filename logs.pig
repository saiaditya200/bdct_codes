logs = LOAD '/user/cloudera/data/pig/simplelogs/logs.txt'
USING PigStorage('\t')
AS (site:chararray, hour_of_day:int, page_views:int, date:chararray);

by_day = GROUP logs BY date;
daily_views = FOREACH by_day GENERATE group AS date, SUM(logs.page_views) AS total_views;
DUMP daily_views;

by_hour = GROUP logs BY hour_of_day;
hourly_views = FOREACH by_hour GENERATE group AS hour, SUM(logs.page_views) AS total_views;
DUMP hourly_views;

by_site = GROUP logs BY site;
site_views = FOREACH by_site GENERATE group AS site, SUM(logs.page_views) AS total_views;
DUMP site_views;
