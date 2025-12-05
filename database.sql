
-- Create department/unit 
create table dim_department(
	dept_id		integer primary Key,
	dept_code		varchar(10),
	dept_name	varchar(50),
	created_at			DATE,
	lastupdated_at		TIMESTAMP
);

-- Create Employee Dimension
create table dim_employee(
	employee_id 		Integer primary key,
	employee_code 		VARCHAR(10),
	firstname			VARCHAR(50),
	surname				VARCHAR(50),
	dept_id				integer references dim_department(dept_id),
	created_at			DATE,
	lastupdated_at		TIMESTAMP
);

-- Create dim Project
create table dim_project(
	project_id			integer primary Key,
	project_code		varchar(10),
	project_name		Varchar(100),
	project_shortname	varchar(50),
	project_startdate	date,
	project_enddate		date,
	created_at			DATE,
	lastupdated_at		TIMESTAMP
);

-- Create dim Tasks
create table dim_task(
	task_id				integer primary Key,
	task_code			varchar(10),
	task_name			varchar(100),
	task_shortname		varchar(50),
	project_id			integer references dim_project(project_id),
	created_at			DATE,
	lastupdated_at		TIMESTAMP
)
;

-- Crete fact table LOE
create table fact_loe(
	employee_id			integer references dim_employee(employee_id),
	task_id			integer references dim_task(task_id),
	period			date,
	planned_loe		integer,
	estimated_loe	integer,
	used_loe		integer
)
