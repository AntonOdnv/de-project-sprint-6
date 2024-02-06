DROP TABLE IF EXISTS STV2024012236__DWH.l_user_group_activity CASCADE;

CREATE TABLE STV2024012236__DWH.l_user_group_activity (
  hk_l_user_group_activity integer PRIMARY KEY, 
  hk_user_id int NOT NULL REFERENCES STV2024012236__DWH.h_users(hk_user_id), 
  hk_group_id int NOT NULL REFERENCES STV2024012236__DWH.h_groups(hk_group_id), 
  load_dt timestamp NOT NULL, 
  load_src VARCHAR(20)
)
ORDER BY hk_l_user_group_activity
segmented BY hash(hk_l_user_group_activity) ALL nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

---------------------------------------------------------------------

DROP TABLE IF EXISTS STV2024012236__DWH.s_auth_history CASCADE;

CREATE TABLE STV2024012236__DWH.s_auth_history (
  hk_l_user_group_activity int REFERENCES STV2024012236__DWH.l_user_group_activity(hk_l_user_group_activity), 
  user_id_from int, 
  event varchar(20) not null check (event in ('create', 'add', 'leave')), 
  event_dt timestamp, 
  load_dt timestamp NOT NULL, 
  load_src VARCHAR(20)
)
ORDER BY hk_l_user_group_activity
segmented BY hash(hk_l_user_group_activity) ALL nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);