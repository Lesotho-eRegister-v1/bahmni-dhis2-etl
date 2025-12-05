# Airflow connections configutation

Create a the following connection with through the Airflow frontend:

## Database connection

**Connection ID:** bahmni_db
**Connection Type:** mysql
**HOST:** IP address of the openmrsdb container
**port:** 3306

Add the username and password of the openmrs database 

## Connection to DHIS2


Connection ID: ls_conn
Connection Type: http
HOST : https://ls-dhis.hisp.org

Use your DHIS2 logging details for **logging** and **password**