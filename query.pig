-- Definir parámetro de entrada del título del trabajo a buscar
%default job_title_query 'account executive'

ordered_percentage_skills= LOAD 'hdfs://cm:9000/uhadoop2024/projects/skills/sorted_skills' USING PigStorage(',') AS (job_title, skill, skill_percentage);

-- Filtrar por el trabajo que se desea buscar usando el parámetro
filtered_skills = FILTER ordered_percentage_skills BY job_title == '$job_title';

-- Hacer un head de las 10 primeras habilidades más frecuentes
head_filtered_skills = LIMIT filtered_skills 10;

-- Mostrar las 10 habilidades más frecuentes con su porcentaje
DUMP head_filtered_skills;