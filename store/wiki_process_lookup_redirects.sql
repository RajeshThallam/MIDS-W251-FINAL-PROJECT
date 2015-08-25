-- + The SQL preprocesses the historical raw wikipedia traffic dumps 
-- + The data is used to create MySQL a redirect page_lookups table used by 
--   hive and spark.
-- + Download files from https://dumps.wikimedia.org/enwiki/latest/
--       https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-page.sql.gz
--       https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-redirect.sql.gz
-- + Filter namespace0 articles
--       awk -F',' '$2 == 0 { print $0 }' page.txt > page0.txt
-- + page.txt is export of page.sql. We used perl utility mysqldump_sql2csv.pl to 
--   convert sql file to txt file

DROP TABLE IF EXISTS 'page';
CREATE TABLE 'page' (
  'page_id' int(8) unsigned NOT NULL,
  'page_namespace' int(11) NOT NULL default '0',
  'page_title' varchar(255) binary NOT NULL default '',
  'page_restrictions' tinyblob NOT NULL,
  'page_counter' bigint(20) unsigned NOT NULL default '0',
  'page_is_redirect' tinyint(1) unsigned NOT NULL default '0',
  'page_is_new' tinyint(1) unsigned NOT NULL default '0',
  'page_random' double unsigned NOT NULL default '0',
  'page_touched' varchar(14) binary NOT NULL default '',
  'page_latest' int(8) unsigned NOT NULL default '0',
  'page_len' int(8) unsigned NOT NULL default '0',
PRIMARY KEY  ('page_id')
) TYPE=InnoDB;

LOAD DATA LOCAL INFILE '/wrk/wiki/data/preprocess/page0.txt'
INTO TABLE page
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
(page_id,
page_namespace, 
page_title, 
page_restrictions, 
page_counter, 
page_is_redirect, 
page_is_new, 
page_random, 
page_touched, 
page_latest, 
page_len);
-- Query OK, 11820620 rows affected, 1667 warnings (6 min 20 sec)

create index title_index on page (page_title(64));
-- Query OK, 11820620 rows affected (12 min 2 sec)
  
create index page_index on page (page_id);
-- Query OK, 11820620 rows affected (13 min 16 sec)

DROP TABLE IF EXISTS 'redirect';
CREATE TABLE 'redirect' (
  'id' int(8) unsigned NOT NULL auto_increment,
  'rd_from' int(8) unsigned NOT NULL default '0',
  'rd_namespace' int(11) NOT NULL default '0',
  'rd_title' varchar(255) binary NOT NULL default '',
PRIMARY KEY  ('id')
) TYPE=InnoDB;

-- awk -F',' '$2 == 0 { print $0 }' redirect.txt > redirect0.txt

LOAD DATA LOCAL INFILE '/wrk/wiki/data/preprocess/redirect0.txt'
INTO TABLE redirect
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
(rd_from, rd_namespace, rd_title);
-- Query OK, 3446196 rows affected, 1 warning (30.58 sec)

create index red_from_index on redirect (rd_from);
-- Query OK, 3446196 rows affected (56.78 sec)  

-- find the "true page" for all redirect article titles and 
-- direct to /wrk/wiki/data/preprocess/
select 
	original_page.page_title redirect_title, 
	REPLACE(final_page.page_title, '_', ' ') true_title, 
	final_page.page_id, 
	final_page.page_latest
from
	page original_page, 
	page final_page, 
	redirect
where
	rd_from = original_page.page_id
and rd_title = final_page.page_title
INTO OUTFILE 'page_lookup_redirects.txt';
-- Query OK, 3401301 rows affected (12 min 4.92 sec)

-- get non-redirects
SELECT page.page_title redirect_title, 
  REPLACE(page.page_title, '_', ' ') true_title,  
  page.page_id, 
  page.page_latest
  FROM page LEFT JOIN redirect ON page.page_id = redirect.rd_from
  WHERE redirect.rd_from IS NULL
  INTO OUTFILE 'page_lookup_nonredirects.txt';

-- build page loops
DROP TABLE IF EXISTS 'page_lookups';
CREATE TABLE 'page_lookups' (
  'id' int(8) unsigned NOT NULL auto_increment,  
  'redirect_title' varchar(255) binary NOT NULL default '',
  'true_title' varchar(255) binary NOT NULL default '',
  'page_id' int(8) unsigned NOT NULL,
  'page_latest' int(8) unsigned NOT NULL default '0',
PRIMARY KEY  ('id')  
) TYPE=InnoDB;

LOAD DATA LOCAL INFILE '/mnt/wikidata/wikidump/page_lookups.txt'
INTO TABLE page_lookups
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
(redirect_title, true_title, page_id, page_latest);