create table  dim_source(
	id		SERIAL primary key,
	source	VARCHAR(50)
);


insert into dim_source
values 
(nextval('dim_source_id_seq'), 'PERSAL'),
(nextval('dim_source_id_seq'), 'SALGA'),
(nextval('dim_source_id_seq'), 'PEPFAR')
;

select *
from dim_source

alter table dim_practitioner 
	add column source_id integer references dim_source(id)
;
	
alter table fact_practitioners_employment  
	add column source_id integer references dim_source(id)
;

update dim_practitioner dp 
set source_id = 1
;


update fact_practitioners_employment 
set source_id = 1
;

	
