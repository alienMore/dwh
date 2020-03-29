SELECT 1;
select * from projections;

--drop table stream;
--drop table stream_metadata;
--drop table stream cascade;

--------------*DDL*--------------
-- 1.1. Create STAGE table
create table IF NOT EXISTS stream (
        id                      auto_increment(1,1,1) NOT NULL,
        timestamp               TIMESTAMP,
        remoteHost              varchar(255),
        sessionId               varchar(255),
        userAgentName           varchar(255),
        location                varchar(255),
        userAgentDeviceCategory varchar(255),
        userAgentOsFamily       varchar(255),
        source                  varchar(255),
        batchtime               TIMESTAMP,
        PRIMARY KEY (id) ENABLED)
order by timestamp;

-- 1.2. Create metadata table
create table IF NOT EXISTS stream_metadata (
        id                      auto_increment(1,1,1) NOT NULL,
        load_ts                 TIMESTAMP,
        batchtime               TIMESTAMP,
        source                  varchar(255),
        downloaded              varchar(25),
        PRIMARY KEY (id) ENABLED)
ORDER BY load_ts;

--------------*DML*--------------
-- Populate stream_metadata table
INSERT INTO stream_metadata (
        load_ts,
        batchtime,
        source,
        downloaded
        )
SELECT GETDATE() as load_ts,
       a.batchtime,
       a.source,
       'on' as downloaded
FROM stream a
      LEFT JOIN stream_metadata b
      ON b.downloaded = 'on'
      AND a.batchtime = b.batchtime
WHERE b.id IS NULL;

-- Get count string per batch
SELECT DATE_TRUNC('SECOND', batchtime) as when_received_batch,
       count(DATE_TRUNC('SECOND', batchtime)) as count_items_per_batch,
       REGEXP_SUBSTR(source,'\w+') as source,
       load_ts as insert_time_in_stream_metadata_table
FROM stream_metadata
GROUP BY DATE_TRUNC('SECOND', batchtime),REGEXP_SUBSTR(source,'\w+'),load_ts
ORDER BY when_received_batch;

--
SELECT * FROM stream ORDER BY batchtime;
SELECT * FROM stream_metadata;
SELECT count(*) FROM stream;
