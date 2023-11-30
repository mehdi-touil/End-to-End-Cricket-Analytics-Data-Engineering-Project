-- Step 1: Creating streams on raw table for 3 different streams
-- Stream for match data
CREATE
OR REPLACE STREAM cricket.raw.for_match_stream ON TABLE cricket.raw.match_raw_tbl APPEND_ONLY = true;

-- Stream for player data
CREATE
OR REPLACE STREAM cricket.raw.for_player_stream ON TABLE cricket.raw.match_raw_tbl APPEND_ONLY = true;

-- Stream for delivery data
CREATE
OR REPLACE STREAM cricket.raw.for_delivery_stream ON TABLE cricket.raw.match_raw_tbl APPEND_ONLY = true;

-- Step 2: Creating a task that runs every 5 minutes to load JSON data into the raw layer
CREATE
OR REPLACE TASK cricket.raw.load_json_to_raw warehouse = 'COMPUTE_WH' SCHEDULE = '5 minute' AS copy into cricket.raw.match_raw_tbl
from
    (
        select
            t.$1 :meta :: object as meta,
            t.$1 :info :: variant as info,
            t.$1 :innings :: array as innings,
            --
            metadata$filename,
            metadata$file_row_number,
            metadata$file_content_key,
            metadata$file_last_modified
        from
            @cricket.land.my_stg/cricket/json(file_format=>'cricket.land.my_json_format') t
    ) on_error = continue;

-- Step 3: Creating a task that reads stream and load data into clean layer
CREATE
OR REPLACE TASK cricket.raw.load_to_clean_match warehouse = 'COMPUTE_WH'
after
    cricket.raw.load_json_to_raw when system$stream_has_data('cricket.raw.for_match_stream') as
insert into
    cricket.clean.match_detail_clean
select
    info:match_type_number :: int as match_type_number,
    info:event.name :: text as event_name,
    case
        when info:event.match_number :: text is not null then info :event.match_number :: text
        when info:event.stage :: text is not null then info :event.stage :: text
        else 'NA'
    end as match_stage,
    info :dates [0] :: date as event_date,
    date_part('year', info :dates [0] :: date) as event_year,
    date_part('month', info :dates [0] :: date) as event_month,
    date_part('day', info :dates [0] :: date) as event_day,
    info :match_type :: text as match_type,
    info :season :: text as season,
    info :team_type :: text as team_type,
    info :overs :: text as overs,
    info :city :: text as city,
    info :venue :: text as venue,
    info :gender :: text as gender,
    info :teams [0] :: text as first_team,
    info :teams [1] :: text as second_team,
    case
        when info :outcome.winner is not null then 'Result Declared'
        when info :outcome.result = 'tie' then 'Tie'
        when info :outcome.result = 'no result' then 'No Result'
        else info :outcome.result
    end as matach_result,
    case
        when info :outcome.winner is not null then info :outcome.winner
        else 'NA'
    end as winner,
    info :toss.winner :: text as toss_winner,
    initcap(info :toss.decision :: text) as toss_decision,
    stg_file_name,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
from
    cricket.raw.for_match_stream;

-- Step 4: Creating a child task after match data is populated
CREATE
OR REPLACE TASK cricket.raw.load_to_clean_player WAREHOUSE = 'COMPUTE_WH'
AFTER
    cricket.raw.load_clean_match
    WHEN SYSTEM$STREAM_HAS_DATA('cricket.raw.for_player_stream') AS
INSERT INTO
    cricket.clean.player_clean_tbl
SELECT
    rcm.info :match_type_number :: int as match_type_number,
    p.path :: text as country,
    team.value :: text as player_name,
    stg_file_name,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
FROM
    cricket.raw.for_player_stream rcm,
    lateral flatten (input=> rcm.info :players) p,
    lateral flatten (input=> p.value) team;

-- Step 5: Creating a task for loading data into the clean delivery table
CREATE
OR REPLACE TASK cricket.raw.load_to_clean_delivery WAREHOUSE = 'COMPUTE_WH'
AFTER
    cricket.raw.load_to_clean_player
    WHEN SYSTEM$STREAM_HAS_DATA('cricket.raw.for_delivery_stream') AS
INSERT INTO
    cricket.clean.delivery_clean_tbl
SELECT
    m.info :match_type_number :: int as match_type_number,
    i.value :team :: text as country,
    o.value :over :: int + 1 as over,
    d.value :bowler :: text as bowler,
    d.value :batter :: text as batter,
    d.value :non_striker :: text as non_striker,
    d.value :runs.batter :: text as runs,
    d.value :runs.extras :: text as extras,
    d.value :runs.total :: text as total,
    e.key :: text as extra_type,
    e.value :: number as extra_runs,
    w.value :player_out :: text as player_out,
    w.value :kind :: text as player_out_kind,
    w.value :fielders :: variant as player_out_fielders,
    m.stg_file_name,
    m.stg_file_row_number,
    m.stg_file_hashkey,
    m.stg_modified_ts
FROM
    cricket.raw.for_delivery_stream m,
    lateral flatten (input=> m.innings) i,
    lateral flatten (input=> i.value :overs) o,
    lateral flatten (input=> o.value :deliveries) d,
    lateral flatten (input=> d.value :extras, outer=> True) e,
    lateral flatten (input=> d.value :wickets, outer=> True) w;

-- Step 6: 
CREATE
OR REPLACE TASK cricket.raw.load_to_team_dim WAREHOUSE = 'COMPUTE_WH'
AFTER
    cricket.raw.load_to_clean_delivery
    WHEN SYSTEM$STREAM_HAS_DATA('cricket.raw.for_delivery_stream') AS
insert into
    cricket.consumption.team_dim (team_name)(
        select
            distinct team_name
        from
            (
                select
                    first_team as team_name
                from
                    cricket.clean.match_detail_clean
                union
                all
                select
                    second_team as team_name
                from
                    cricket.clean.match_detail_clean
            )
        minus
        select
            team_name
        from
            cricket.consumption.team_dim
    );

-- Step 7:
CREATE
OR REPLACE TASK cricket.raw.load_to_player_dim WAREHOUSE = 'COMPUTE_WH'
AFTER
    cricket.raw.load_to_clean_delivery
    WHEN SYSTEM$STREAM_HAS_DATA('cricket.raw.for_delivery_stream') AS
INSERT INTO
    cricket.consumption.player_dim (team_id, player_name)
SELECT
    b.team_id,
    a.player_name
FROM
    cricket.clean.player_clean_tbl a
    JOIN cricket.consumption.team_dim b ON a.country = b.team_name
GROUP BY
    b.team_id,
    a.player_name
minus
select
    team_id,
    player_name
from
    cricket.consumption.player_dim;

-- Step 8:
CREATE
OR REPLACE TASK cricket.raw.load_to_venue_dim WAREHOUSE = 'COMPUTE_WH'
AFTER
    cricket.raw.load_to_clean_delivery
    WHEN SYSTEM$STREAM_HAS_DATA('cricket.raw.for_delivery_stream') AS
INSERT INTO
    cricket.consumption.venue_dim (venue_name, city)
SELECT
    venue,
    CASE
        WHEN city IS NULL THEN 'NA'
        ELSE city
    END AS city
FROM
    (
        SELECT
            venue,
            CASE
                WHEN city IS NULL THEN 'NA'
                ELSE city
            END AS city
        FROM
            cricket.clean.match_detail_clean
        GROUP BY
            venue,
            city
        minus
        select
            venue_name,
            city
        from
            cricket.consumption.venue_dim
    );

--step-9:populate fact table
create
or replace task cricket.raw.load_match_fact WAREHOUSE = 'COMPUTE_WH'
after
    cricket.raw.load_to_team_dim,
    cricket.raw.load_to_player_dim,
    cricket.raw.load_to_venue_dim as
insert into
    cricket.consumption.match_fact
select
    a.*
from
    (
        select
            m.match_type_number as match_id,
            dd.date_id as date_id,
            0 as referee_id,
            ftd.team_id as first_team_id,
            std.team_id as second_team_id,
            mtd.match_type_id as match_type_id,
            vd.venue_id as venue_id,
            50 as total_overs,
            6 as balls_per_overs,
            max(
                case
                    when d.country = m.first_team then d.over
                    else 0
                end
            ) as OVERS_PLAYED_BY_TEAM_A,
            sum(
                case
                    when d.country = m.first_team then 1
                    else 0
                end
            ) as balls_PLAYED_BY_TEAM_A,
            sum(
                case
                    when d.country = m.first_team then d.extras
                    else 0
                end
            ) as extra_balls_PLAYED_BY_TEAM_A,
            sum(
                case
                    when d.country = m.first_team then d.extra_runs
                    else 0
                end
            ) as extra_runs_scored_BY_TEAM_A,
            0 fours_by_team_a,
            0 sixes_by_team_a,
            (
                sum(
                    case
                        when d.country = m.first_team then d.runs
                        else 0
                    end
                ) + sum(
                    case
                        when d.country = m.first_team then d.extra_runs
                        else 0
                    end
                )
            ) as total_runs_scored_BY_TEAM_A,
            sum(
                case
                    when d.country = m.first_team
                    and player_out is not null then 1
                    else 0
                end
            ) as wicket_lost_by_team_a,
            max(
                case
                    when d.country = m.second_team then d.over
                    else 0
                end
            ) as OVERS_PLAYED_BY_TEAM_B,
            sum(
                case
                    when d.country = m.second_team then 1
                    else 0
                end
            ) as balls_PLAYED_BY_TEAM_B,
            sum(
                case
                    when d.country = m.second_team then d.extras
                    else 0
                end
            ) as extra_balls_PLAYED_BY_TEAM_B,
            sum(
                case
                    when d.country = m.second_team then d.extra_runs
                    else 0
                end
            ) as extra_runs_scored_BY_TEAM_B,
            0 fours_by_team_b,
            0 sixes_by_team_b,
            (
                sum(
                    case
                        when d.country = m.second_team then d.runs
                        else 0
                    end
                ) + sum(
                    case
                        when d.country = m.second_team then d.extra_runs
                        else 0
                    end
                )
            ) as total_runs_scored_BY_TEAM_B,
            sum(
                case
                    when d.country = m.second_team
                    and player_out is not null then 1
                    else 0
                end
            ) as wicket_lost_by_team_b,
            tw.team_id as toss_winner_team_id,
            m.toss_decision as toss_decision,
            m.matach_result as matach_result,
            mw.team_id as winner_team_id
        from
            cricket.clean.match_detail_clean m
            join date_dim dd on m.event_date = dd.full_dt
            join team_dim ftd on m.first_team = ftd.team_name
            join team_dim std on m.second_team = std.team_name
            join match_type_dim mtd on m.match_type = mtd.match_type
            join venue_dim vd on m.venue = vd.venue_name
            and m.city = vd.city
            join cricket.clean.delivery_clean_tbl d on d.match_type_number = m.match_type_number
            join team_dim tw on m.toss_winner = tw.team_name
            join team_dim mw on m.winner = mw.team_name --where m.match_type_number = 4686
        group by
            m.match_type_number,
            date_id,
            referee_id,
            first_team_id,
            second_team_id,
            match_type_id,
            venue_id,
            total_overs,
            toss_winner_team_id,
            toss_decision,
            matach_result,
            winner_team_id

) a
left join cricket.consumption.match_fact b on a.match_id = b.match_id
where
    b.match_id is null;
-- Delivery Fact Task
create
or replace task cricket.raw.load_delivery_fact WAREHOUSE = 'COMPUTE_WH'
after
    cricket.raw.load_match_fact as
insert into cricket.consumption.delivery_fact
select a.* from (
select 
    d.match_type_number as match_id,
    td.team_id,
    bpd.player_id as bower_id, 
    spd.player_id batter_id, 
    nspd.player_id as non_stricker_id,
    d.over,
    d.runs,
    case when d.extra_runs is null then 0 else d.extra_runs end as extra_runs,
    case when d.extra_type is null then 'None' else d.extra_type end as extra_type,
    case when d.player_out is null then 'None' else d.player_out end as player_out,
    case when d.player_out_kind is null then 'None' else d.player_out_kind end as player_out_kind
from 
    cricket.clean.delivery_clean_tbl d
    join team_dim td on d.country = td.team_name
    join player_dim bpd on d.bowler = bpd.player_name
    join player_dim spd on d.batter = spd.player_name
    join player_dim nspd on d.non_striker = nspd.player_name
) a
left join cricket.consumption.match_fact b on a.match_id = b.match_id
where
    b.match_id is null;

USE ROLE accountadmin;

-- Switching to the accountadmin role
GRANT EXECUTE TASK,
EXECUTE MANAGED TASK ON ACCOUNT TO ROLE ACCOUNTADMIN;

-- Granting the necessary privileges to the sysadmin role
USE ROLE ACCOUNTADMIN;

-- Switching to the sysadmin role
ALTER TASK cricket.raw.load_delivery_fact RESUME;

ALTER TASK cricket.raw.load_match_fact RESUME;

ALTER TASK cricket.raw.load_to_venue_dim RESUME;

ALTER TASK cricket.raw.load_to_player_dim RESUME;

ALTER TASK cricket.raw.load_to_team_dim RESUME;

ALTER TASK cricket.raw.load_to_clean_delivery RESUME;

ALTER TASK cricket.raw.load_to_clean_player RESUME;

ALTER TASK cricket.raw.load_to_clean_match RESUME;

ALTER TASK cricket.raw.load_json_to_raw RESUME;

-- Resuming the specified tasks