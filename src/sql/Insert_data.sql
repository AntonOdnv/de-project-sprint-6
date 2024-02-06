INSERT INTO STV2024012236__DWH.l_user_group_activity(
  hk_l_user_group_activity, hk_user_id, 
  hk_group_id, load_dt, load_src
) 
SELECT DISTINCT hash(hk_user_id, hk_group_id) as hk_l_user_group_activity, 
	   hk_user_id, 
	   hk_group_id, 
	   now() AS load_dt, 
	   's3' AS load_src 
FROM 
STV2024012236__STAGING.group_log AS sgl 
LEFT JOIN STV2024012236__DWH.h_users hu ON hu.user_id = sgl.user_id 
LEFT JOIN STV2024012236__DWH.h_groups hg ON hg.group_id = sgl.group_id 
WHERE hash(hk_user_id, hk_group_id) NOT IN (
    SELECT hk_l_user_group_activity 
    FROM STV2024012236__DWH.l_user_group_activity
);

--------------------------------------------------------------

INSERT INTO STV2024012236__DWH.s_auth_history(
  hk_l_user_group_activity, user_id_from, 
  event, event_dt, load_dt, load_src
) 
SELECT ug.hk_l_user_group_activity, 
	   gl.user_id_from, 
	   gl.event, 
	   gl.datetime, 
	   now() AS load_dt, 
	   's3' AS load_src 
FROM STV2024012236__STAGING.group_log AS gl 
LEFT JOIN STV2024012236__DWH.h_groups AS hg ON gl.group_id = hg.group_id 
LEFT JOIN STV2024012236__DWH.h_users AS hu ON gl.user_id = hu.user_id 
LEFT JOIN STV2024012236__DWH.l_user_group_activity AS ug 
		  ON hg.hk_group_id = ug.hk_group_id AND hu.hk_user_id = ug.hk_user_id;
