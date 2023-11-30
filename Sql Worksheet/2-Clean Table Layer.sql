SELECT
  info:match_type_number::int AS match_type_number,
  info:match_type::text AS match_type,
  info:season::text AS season,
  info:team_type::text AS team_type,
  info:overs::text AS overs,
  info:city::text AS city,
  info:venue::text AS venue
FROM
  cricket.raw.match_raw_tbl;

// Create Table
create or replace transient table cricket.clean.match_detail_clean as
select
    info:match_type_number::int as match_type_number, 
    info:event.name::text as event_name,
    case
    when 
        info:event.match_number::text is not null then info:event.match_number::text
    when 
        info:event.stage::text is not null then info:event.stage::text
    else
        'NA'
    end as match_stage,   
    info:dates[0]::date as event_date,
    date_part('year',info:dates[0]::date) as event_year,
    date_part('month',info:dates[0]::date) as event_month,
    date_part('day',info:dates[0]::date) as event_day,
    info:match_type::text as match_type,
    info:season::text as season,
    info:team_type::text as team_type,
    info:overs::text as overs,
    info:city::text as city,
    info:venue::text as venue, 
    info:gender::text as gender,
    info:teams[0]::text as first_team,
    info:teams[1]::text as second_team,
    case 
        when info:outcome.winner is not null then 'Result Declared'
        when info:outcome.result = 'tie' then 'Tie'
        when info:outcome.result = 'no result' then 'No Result'
        else info:outcome.result
    end as matach_result,
    case 
        when info:outcome.winner is not null then info:outcome.winner
        else 'NA'
    end as winner,   

    info:toss.winner::text as toss_winner,
    initcap(info:toss.decision::text) as toss_decision,
    --
    stg_file_name ,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
    from 
    cricket.raw.match_raw_tbl;

    select * from MATCH_DETAIL_CLEAN;
--- Player Table
create or replace table player_clean_tbl as 
select 
    rcm.info:match_type_number::int as match_type_number, 
    p.path::text as country,
    team.value:: text as player_name,
    stg_file_name ,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
from cricket.raw.match_raw_tbl rcm,
lateral flatten (input => rcm.info:players) p,
lateral flatten (input => p.value) team;
-- Describe the table
DESC TABLE cricket.clean.player_clean_tbl;

-- Add NOT NULL constraint to match_type_number column
ALTER TABLE cricket.clean.player_clean_tbl ALTER  match_type_number  NOT NULL;


-- Add NOT NULL constraint to country column
ALTER TABLE cricket.clean.player_clean_tbl ALTER  country NOT NULL;


-- Add NOT NULL constraint to player_name column
ALTER TABLE cricket.clean.player_clean_tbl ALTER  player_name NOT NULL;


-- Add a primary key constraint to match_detail_clean table
ALTER TABLE cricket.clean.match_detail_clean
ADD CONSTRAINT pk_match_type_number PRIMARY KEY (match_type_number);

DESC TABLE cricket.clean.match_detail_clean;

-- Add a foreign key constraint to player_clean_tbl referencing match_detail_clean
ALTER TABLE cricket.clean.player_clean_tbl
ADD CONSTRAINT fk_match_id
FOREIGN KEY (match_type_number)
REFERENCES cricket.clean.match_detail_clean (match_type_number);

select get_ddl('table', 'cricket.clean.player_clean_tbl');

// Delivery Clean Table

select 
  m.info:match_type_number::int as match_type_number,
  m.innings
from cricket.raw.match_raw_tbl m
where match_type_number = 4667;

create or replace table delivery_clean_tbl as
select 
    m.info:match_type_number::int as match_type_number, 
    i.value:team::text as country,
    o.value:over::int+1 as over,
    d.value:bowler::text as bowler,
    d.value:batter::text as batter,
    d.value:non_striker::text as non_striker,
    d.value:runs.batter::text as runs,
    d.value:runs.extras::text as extras,
    d.value:runs.total::text as total,
    e.key::text as extra_type,
    e.value::number as extra_runs,
    w.value:player_out::text as player_out,
    w.value:kind::text as player_out_kind,
    w.value:fielders::variant as player_out_fielders,
    m.stg_file_name ,
    m.stg_file_row_number,
    m.stg_file_hashkey,
    m.stg_modified_ts
from cricket.raw.match_raw_tbl m,
lateral flatten (input => m.innings) i,
lateral flatten (input => i.value:overs) o,
lateral flatten (input => o.value:deliveries) d,
lateral flatten (input => d.value:extras, outer => True) e,
lateral flatten (input => d.value:wickets, outer => True) w;

// Foreign keys
-- Add NOT NULL constraints
ALTER TABLE cricket.clean.delivery_clean_tbl
MODIFY COLUMN match_type_number SET NOT NULL;

ALTER TABLE cricket.clean.delivery_clean_tbl
MODIFY COLUMN country SET NOT NULL;

ALTER TABLE cricket.clean.delivery_clean_tbl
MODIFY COLUMN over SET NOT NULL;

ALTER TABLE cricket.clean.delivery_clean_tbl
MODIFY COLUMN bowler SET NOT NULL;

ALTER TABLE cricket.clean.delivery_clean_tbl
MODIFY COLUMN batter SET NOT NULL;

ALTER TABLE cricket.clean.delivery_clean_tbl
MODIFY COLUMN non_striker SET NOT NULL;

DESC table cricket.clean.delivery_clean_tbl;

ALTER TABLE cricket.clean.delivery_clean_tbl
ADD CONSTRAINT fk_delivery_match_id
FOREIGN KEY (match_type_number)
REFERENCES cricket.clean.match_detail_clean(match_type_number);

select * from delivery_clean_tbl;