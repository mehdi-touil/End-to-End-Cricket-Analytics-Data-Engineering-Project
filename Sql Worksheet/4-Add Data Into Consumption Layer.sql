----- lets start with team dim, and for simplicity, it is just team name

select distinct team_name from (
select first_team as team_name from cricket.clean.match_detail_clean union all
select second_team as team_name from cricket.clean.match_detail_clean );
-- v2
insert into cricket.consumption.team_dim (team_name)
select distinct team_name from (
select first_team as team_name from cricket.clean.match_detail_clean union all
select second_team as team_name from cricket.clean.match_detail_clean ) order by team_name;
-- v3
select * from cricket.consumption.team_dim order by team_name;

---------------------------------------------------------------------------
-- Select players with team information

SELECT a.country, b.team_id, a.player_name
FROM cricket.clean.player_clean_tbl a
JOIN cricket.consumption.team_dim b ON a.country = b.team_name
GROUP BY a.country, b.team_id, a.player_name;

-- Insert player data into player_dim
INSERT INTO cricket.consumption.player_dim (team_id, player_name)
SELECT b.team_id, a.player_name
FROM cricket.clean.player_clean_tbl a
JOIN cricket.consumption.team_dim b ON a.country = b.team_name
GROUP BY b.team_id, a.player_name;

-- Select data from player_dim
SELECT * FROM cricket.consumption.player_dim;
---------------------------------------------------------------------------
-- referees dimension
SELECT
  info:officials:match_referees[0]::TEXT AS match_referee,
  info:officials:reserve_umpires[0]::TEXT AS reserve_umpire,
  info:officials:tv_umpires[0]::TEXT AS tv_umpire,
  info:officials:umpires[0]::TEXT AS first_umpire,
  info:officials:umpires[1]::TEXT AS second_umpire
FROM cricket.raw.match_raw_tbl;
---------------------------------------------------------------------------
--Venue Dimension
INSERT INTO cricket.consumption.venue_dim (venue_name, city)
SELECT venue, CASE WHEN city IS NULL THEN 'NA' ELSE city END AS city
FROM (
  SELECT venue, CASE WHEN city IS NULL THEN 'NA' ELSE city END AS city
  FROM cricket.clean.match_detail_clean
  GROUP BY venue, city
);
----------------------------------------------------------------
select min(event_date),max(event_date) from cricket.clean.match_detail_clean;

-- Create or replace the transient table
CREATE OR REPLACE TRANSIENT TABLE cricket.consumption.date_range01 (Date DATE);

-- Insert hardcoded date values
INSERT INTO cricket.consumption.date_range01 (Date)
VALUES
  ('2023-10-12'),
  ('2023-10-13'),
  ('2023-10-14'),
  ('2023-10-15'),
  ('2023-10-16'),
  ('2023-10-17'),
  ('2023-10-18'),
  ('2023-10-19'),
  ('2023-10-20'),
  ('2023-10-21'),
  ('2023-10-22'),
  ('2023-10-23'),
  ('2023-10-24'),
  ('2023-10-25'),
  ('2023-10-26'),
  ('2023-10-27'),
  ('2023-10-28'),
  ('2023-10-29'),
  ('2023-10-30'),
  ('2023-10-31'),
  ('2023-11-01'),
  ('2023-11-02'),
  ('2023-11-03'),
  ('2023-11-04'),
  ('2023-11-05'),
  ('2023-11-06'),
  ('2023-11-07'),
  ('2023-11-08'),
  ('2023-11-09'),
  ('2023-11-10');

-- Optional: If you want to view the inserted data
SELECT * FROM cricket.consumption.date_range01;

INSERT INTO cricket.consumption.date_dim (Date_ID, Full_Dt, Day, Month, Year, Quarter, DayOfWeek, DayOfMonth, DayOfYear, DayOfWeekName, IsWeekend)
SELECT
    ROW_NUMBER() OVER (ORDER BY Date) AS DateID,
    Date AS FullDate,
    EXTRACT(DAY FROM Date) AS Day,
    EXTRACT(MONTH FROM Date) AS Month,
    EXTRACT(YEAR FROM Date) AS Year,
    CASE WHEN EXTRACT(QUARTER FROM Date) IN (1, 2, 3, 4) THEN EXTRACT(QUARTER FROM Date) END AS Quarter,
    DAYOFWEEKISO(Date) AS DayOfWeek,
    EXTRACT(DAY FROM Date) AS DayOfMonth,
    DAYOFYEAR(Date) AS DayOfYear,
    DAYNAME(Date) AS DayOfWeekName,
    CASE When DAYNAME(Date) IN ('Sat', 'Sun') THEN 1 ELSE 0 END AS IsWeekend
FROM cricket.consumption.date_range01;


select * from cricket.consumption.date_dim;
------------------------------------------------
--matche type dimension
SELECT *
FROM cricket.clean.match_detail_clean
LIMIT 10;

SELECT match_type
FROM cricket.clean.match_detail_clean
GROUP BY match_type;

INSERT INTO cricket.consumption.match_type_dim (match_type)
SELECT match_type
FROM cricket.clean.match_detail_clean
GROUP BY match_type;
------------------------------------------------

SELECT
  m.match_type_number AS match_id,
  dd.date_id,
  0 AS referee_id,
  ftd.team_id AS team_a_id,
  std.team_id AS team_b_id,
  mtd.match_type_id,
  vd.venue_id
FROM
  cricket.clean.match_detail_clean m
JOIN date_dim dd ON m.event_date = dd.full_dt
JOIN team_dim ftd ON m.first_team = ftd.team_name
JOIN team_dim std ON m.second_team = std.team_name
JOIN match_type_dim mtd ON m.match_type = mtd.match_type
JOIN venue_dim vd ON m.venue = vd.venue_name;

-- Create Match Fact Table ----
insert into cricket.consumption.match_fact
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
    max(case when d.country = m.first_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_A,
    sum(case when d.country = m.first_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_A,
    sum(case when d.country = m.first_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_A,
    sum(case when d.country = m.first_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_A,
    0 fours_by_team_a,
    0 sixes_by_team_a,
    (sum(case when d.country = m.first_team then  d.runs else 0 end ) + sum(case when d.country = m.first_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_A,
    sum(case when d.country = m.first_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_a,    
    
    max(case when d.country = m.second_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_B,
    sum(case when d.country = m.second_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_B,
    sum(case when d.country = m.second_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_B,
    sum(case when d.country = m.second_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_B,
    0 fours_by_team_b,
    0 sixes_by_team_b,
    (sum(case when d.country = m.second_team then  d.runs else 0 end ) + sum(case when d.country = m.second_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_B,
    sum(case when d.country = m.second_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_b,
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
    join venue_dim vd on m.venue = vd.venue_name and m.city = vd.city
    join cricket.clean.delivery_clean_tbl d  on d.match_type_number = m.match_type_number 
    join team_dim tw on m.toss_winner = tw.team_name 
    join team_dim mw on m.winner= mw.team_name 
    --where m.match_type_number = 4686
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
        winner_team_id;

select * FROM  cricket.consumption.match_fact;

-- Create Delivery Fact Table ----


CREATE or replace TABLE delivery_fact (
    match_id INT ,
    team_id INT,
    bowler_id INT,
    batter_id INT,
    non_striker_id INT,
    over INT,
    runs INT,
    extra_runs INT,
    extra_type VARCHAR(255),
    player_out VARCHAR(255),
    player_out_kind VARCHAR(255),

    CONSTRAINT fk_del_match_id FOREIGN KEY (match_id) REFERENCES match_fact (match_id),
    CONSTRAINT fk_del_team FOREIGN KEY (team_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_bowler FOREIGN KEY (bowler_id) REFERENCES player_dim (player_id),
    CONSTRAINT fk_batter FOREIGN KEY (batter_id) REFERENCES player_dim (player_id),
    CONSTRAINT fk_stricker FOREIGN KEY (non_striker_id) REFERENCES player_dim (player_id)
);

-- insert record
insert into delivery_fact
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
    join player_dim nspd on d.non_striker = nspd.player_name;

-- Show delivery Table
select * from delivery_fact;

