WITH patient_data
AS
(
	SELECT distinct p.patient_id 
		, pn.given_name 
		, pn.family_name 
		, floor(datediff(CAST(o.obs_datetime  AS DATE), birthdate)/365) AS Age
		, case  p2.gender when 'F' then 'Females' when 'M' then 'Males' END as gender
		, parent.name  as facility
		, DATE_FORMAT(obs_datetime, '%Y%m') as period
		, MAX(case when cn.name = 'Coded Diagnosis' then val.name END) as diagnosis
		, MAX(case when cn.name = 'Diagnosis order' then val.name END) as orders
		, MAX(case when cn.name = 'Diagnosis Certainty' then val.name END) as certainty
		
	FROM patient p 
		JOIN person_name pn ON p.patient_id = pn.person_id 
		JOIN person p2 on p2.person_id  = p.patient_id 
		JOIN obs o on o.person_id = p.patient_id 
		JOIN location loc on loc.location_id = o.location_id 
		JOIN location parent on loc.parent_location = parent.location_id
		join concept_name cn on cn.concept_id  = o.concept_id 
		join concept_name val on val.concept_id = o.value_coded 
	WHERE
	cn.concept_id in (15, 16, 19) # 15-> Coded Diagnosis, 16->Diagnosis Certainty, 19-> Diagnosis order
		#and val.name like '%hyper%'
		group by 1,2,3,4,5,6,7
), 
patent_data_formated
AS
(
	SELECT patient_id
		, given_name
		, family_name
		, CONCAT(CONCAT(FLOOR(Age/ 5) * 5, '-', (FLOOR(Age / 5) * 5) + 4, 'yrs'), ', ', gender) AS age_gender
		, facility
		, period
		, diagnosis
		, orders
		, certainty
	FROM patient_data
), 
disease
AS(
	SELECT 'Hypertension' as conditions, 'MYYZJbjEU9q' as uid
		UNION 
	SELECT 'diabetes' as conditions, 'GOfTMCBneLa' as uid
		UNION 
	SELECT 'Asthma' as conditions, 'Zm7Ns3J5pBK' as uid
		UNION
	SELECT 'Cholera' as conditions, 'MYYZJbjEU9q' as uid
		UNION 
	SELECT 'Plague' as conditions, 'GOfTMCBneLa' as uid
		UNION 
	SELECT 'Yellow' as conditions, 'Zm7Ns3J5pBK' as uid
			UNION 
	SELECT 'Measles' as conditions, 'Zm7Ns3J5pBK' as uid
	
),
disaggregation
AS
(
	SELECT '35-39yrs, Females' AS name, 'yuVFlYtBaUK' AS uid
	 UNION
	SELECT '55-59yrs, Males' AS name, 'QO5uvaiR1e1' AS uid
	 UNION
	SELECT '60-64yrs, Males' AS name, 'Vsrzs1qtw8u' AS uid
	 UNION
	SELECT '20-24yrs, Females' AS name, 'YiRdMXgCzzl' AS uid
	 UNION
	SELECT '5-9yrs, Males' AS name, 'LjVyylTgU0j' AS uid
	 UNION
	SELECT '65+yrs, Males' AS name, 'joeXHmL9Pjq' AS uid
	 UNION
	SELECT '40-44yrs, Females' AS name, 'jAtRomrx07a' AS uid
	 UNION
	SELECT '0-4yrs, Females' AS name, 'vbmPkpZrITh' AS uid
	 UNION
	SELECT '55-59yrs, Females' AS name, 'HWh8nkCBq0A' AS uid
	 UNION
	SELECT '0-4yrs, Males' AS name, 'BDgFW0V94Cb' AS uid
	 UNION
	SELECT '45-49yrs, Males' AS name, 'qbjgfrNCmkZ' AS uid
	 UNION
	SELECT '20-24yrs, Males' AS name, 'nzyjfylUvac' AS uid
	 UNION
	SELECT '65+yrs, Females' AS name, 'QUwj8Bu3Z8q' AS uid
	 UNION
	SELECT '10-14yrs, Females' AS name, 'NgM71ztODYH' AS uid
	 UNION
	SELECT '25-29yrs, Females' AS name, 'AWSV5LJuGCS' AS uid
	 UNION
	SELECT '50-54yrs, Males' AS name, 'PxI4PkhhNYI' AS uid
	 UNION
	SELECT '45-49yrs, Females' AS name, 'Z5UD7pwvgcj' AS uid
	 UNION
	SELECT '60-64yrs, Females' AS name, 'ooI1KRN7HOb' AS uid
	 UNION
	SELECT '35-39yrs, Males' AS name, 'doESbQGwuZ4' AS uid
	 UNION
	SELECT '25-29yrs, Males' AS name, 'k2rxooqL7LT' AS uid
	 UNION
	SELECT '15-19yrs, Females' AS name, 'JcIbK1Sw4RY' AS uid
	 UNION
	SELECT '30-34yrs, Males' AS name, 'QKCeoT02J2Z' AS uid
	 UNION
	SELECT '5-9yrs, Females' AS name, 'agw9714Knyu' AS uid
	 UNION
	SELECT '10-14yrs, Males' AS name, 'zPaXh0BSIol' AS uid
	 UNION
	SELECT '30-34yrs, Females' AS name, 'C1bFnUxVOUH' AS uid
	 UNION
	SELECT '15-19yrs, Males' AS name, 'AQtvO9iPPTr' AS uid
	 UNION
	SELECT '40-44yrs, Males' AS name, 'PIaGd3R2am5' AS uid
	 UNION
	SELECT '50-54yrs, Females' AS name, 'AWwYoSuQ1Gr' AS uid
)
SELECT
	disease.uid as dataelement
	, period
	, facility as orgunit
	, disag.uid as categoryoptioncombo
	, 'tUiK0vIsDaX' as attributeoptioncombo
	, count(*) as value
	
FROM patent_data_formated
JOIN disaggregation disag on age_gender = disag.name
cross join disease
where orders = 'primary' 
	and certainty = 'Confirmed'
	and diagnosis like CONCAT('%',conditions,'%')
GROUP BY 1,2,3,4
		