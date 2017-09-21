
# added to line 89 
# AND cast(joindate as timestamp) >= DATE_ADD(CURRENT_DATE(), -97, 'DAY')

query_body <- paste("FROM (
SELECT
base.date AS date,
user_profile.joindate AS joindate,
user_profile.gamerid AS gamerid,
user_profile.country AS country,
user_profile.platform AS platform,
user_profile.channel AS channel,
user_profile.campaign_name_id AS campaign_name_id,
user_profile.campaign_group_id AS campaign_group_id,
user_profile.campaign_name AS campaign_name,
user_profile.publisher AS publisher,
user_profile.creative AS creative,
DATEDIFF(base.date,user_profile.joindate) AS playerage,
DATEDIFF(first_purchase.first_purchase_ts,user_profile.joindate) AS purchase_age,
revenue.revenue AS revenue,
revenue.net_revenue AS net_revenue,
DATEDIFF(guild_joins.guild_join_date,user_profile.joindate) AS guild_age
FROM (
SELECT
joindate,
gamerid,
country,
platform,
CASE
WHEN LOWER(channel) CONTAINS 'vungle' THEN 'vungle'WHEN LOWER(channel) CONTAINS 'twitter' THEN 'twitter'WHEN LOWER(channel) CONTAINS 'tapjoy' THEN 'tapjoy'WHEN LOWER(channel) CONTAINS 'supersonic' THEN 'supersonic'WHEN LOWER(channel) CONTAINS 'motive' THEN 'motive'WHEN LOWER(channel) CONTAINS 'mdotm' THEN 'mdotm'WHEN LOWER(channel) CONTAINS 'google' THEN 'google'WHEN LOWER(channel) CONTAINS 'facebook' THEN 'facebook'WHEN LOWER(channel) CONTAINS 'crossinstall' THEN 'crossinstall'WHEN LOWER(channel) CONTAINS 'chartboost' THEN 'chartboost'WHEN LOWER(channel) CONTAINS 'applovin' THEN 'applovin'WHEN LOWER(channel) CONTAINS 'applift' THEN 'applift'WHEN LOWER(channel) CONTAINS 'applifier' THEN 'unity ads'WHEN LOWER(channel) CONTAINS 'appia' THEN 'appia'WHEN LOWER(channel) CONTAINS 'adcolony' THEN 'adcolony'ELSE LOWER(channel)
END AS channel,
campaign_name_id,
campaign_group_id,
campaign_name,
source_app AS publisher,
creative
FROM (
SELECT
joindate,
gamerid,
country,
IFNULL(platform,os) AS platform,
IFNULL(channel,'organic') AS channel,
campaign_name_id,
campaign_group_id,
campaign_name,
source_app,
creative
FROM (
SELECT
joindate,
gamerid,
country,
platform
FROM (
SELECT
ROW_NUMBER() OVER (PARTITION BY b.gamerid) AS row_number,
b.joindate AS joindate,
b.gamerid AS gamerid,
b.country AS country,
b.platform AS platform
FROM (
SELECT
gamerid,
MIN(joindate) AS min_joindate
FROM
[n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary]
GROUP BY
1) a
LEFT JOIN EACH (
SELECT
joindate,
gamerid,
country_code AS country,
os_system AS platform
FROM
[n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary]
WHERE
joindate = date
GROUP BY
1,
2,
3,
4) b
ON
a.gamerid = b.gamerid
AND a.min_joindate = b.joindate)
WHERE row_number = 1
AND cast(joindate as timestamp) >= DATE_ADD(CURRENT_DATE(), -97, 'DAY')
) a
LEFT JOIN (
SELECT
custom_user_id,
LOWER(campaign_source) AS channel,
campaign_name_id,
campaign_group_id,
campaign_name,
source_app,
creative,
os
FROM (
SELECT
custom_user_id,
campaign_source,
campaign_name_id,
campaign_group_id,
campaign_name,
source_app,
creative,
install_timestamp,
os,
MIN(install_timestamp) OVER (PARTITION BY custom_user_id) AS min_time
FROM (
SELECT
*
FROM
[n3twork-marketing-analytics:INSTALL_ATTRIBUTION.apsalar_installs]
WHERE
longname = 'com.n3twork.legendary'AND attribution = 'Install'AND LENGTH(custom_user_id) > 2)
GROUP BY
1,
2,
3,
4,
5,
6,
7,
8,
9)
WHERE install_timestamp = min_time
GROUP BY
1,
2,
3,
4,
5,
6,
7,
8) b
ON
a.gamerid = b.custom_user_id
ORDER BY
1 DESC)) user_profile
LEFT JOIN (
SELECT
date,
gamerid
FROM
[n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary]
WHERE
gamerid IN (
SELECT
gamerid
FROM
[n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary]
WHERE
joindate = date
GROUP BY
1)
GROUP BY
1,
2) base
ON
base.gamerid = user_profile.gamerid
LEFT JOIN (
SELECT
gamerid,
DATE(first_purchase_ts) AS first_purchase_ts
FROM (
SELECT
gamerid,
MIN(date) OVER (PARTITION BY gamerid) AS first_purchase_ts
FROM
[n3twork-legendary-analytics:DAILY_IAPS.iaps_summary_poole]
GROUP BY
gamerid,
date)
GROUP BY
1,
2) first_purchase
ON
user_profile.gamerid = first_purchase.gamerid
LEFT JOIN (
SELECT
DATE(date) AS date,
gamerid,
SUM(iap) AS revenue,
SUM(nrv) AS net_revenue
FROM
[n3twork-legendary-analytics:DAILY_IAPS.iaps_summary_poole]
GROUP BY
1,
2 ) revenue
ON
base.date = revenue.date
AND base.gamerid = revenue.gamerid
LEFT JOIN (
SELECT
gamerid,
guild_join_date
FROM
[n3twork-marketing-analytics:Metrics.first_guild_join_date_LEG] ) guild_joins
ON
user_profile.gamerid = guild_joins.gamerid
WHERE
user_profile.joindate >= '2016-10-01') a
CROSS JOIN (
SELECT
MAX(date) AS max_date
FROM
[n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary]) b
WHERE
a.joindate <= b.max_date
AND playerage <= 90
Group By 1        
Order by 1
",sep='') 

