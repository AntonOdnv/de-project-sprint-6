WITH user_group_messages  AS (
SELECT gd.hk_group_id, 
       count(lu.hk_user_id) AS cnt_users_in_group_with_messages 
FROM STV2024012236__DWH.l_user_message AS lu
LEFT JOIN STV2024012236__DWH.l_groups_dialogs AS gd ON lu.hk_message_id = gd.hk_message_id
GROUP BY gd.hk_group_id
),
	 user_group_log  AS (
SELECT hg.hk_group_id, 
       hg.registration_dt, 
       count(DISTINCT ug.hk_user_id) AS cnt_added_users 
FROM STV2024012236__DWH.h_groups AS hg 
LEFT JOIN STV2024012236__DWH.l_user_group_activity AS ug ON ug.hk_group_id = hg.hk_group_id 
LEFT JOIN STV2024012236__DWH.s_auth_history AS sa ON sa.hk_l_user_group_activity = ug.hk_l_user_group_activity 
WHERE sa.event = 'add' 
GROUP BY hg.hk_group_id, hg.registration_dt 
ORDER BY hg.registration_dt
LIMIT 10
) 
SELECT ug.hk_group_id, 
       cnt_added_users, 
  	   cnt_users_in_group_with_messages, 
  	   cnt_users_in_group_with_messages / cnt_added_users AS group_conversion 
FROM user_group_log AS ul 
LEFT JOIN user_group_messages AS ug ON ul.hk_group_id = ug.hk_group_id 
ORDER BY cnt_users_in_group_with_messages / cnt_added_users DESC;