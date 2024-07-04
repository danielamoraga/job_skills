-- Definir parámetro de entrada del título del trabajo a buscar
%default job_title_query 'account executive'

-- Cargar el data set de skills relacionadas al url del trabajo
raw_skills = LOAD 'hdfs://cm:9000/uhadoop2024/projects/skills/job_skills.csv' USING PigStorage(';') AS (url:chararray, skills:chararray);
-- head_skills= LIMIT raw_skills 10;
-- DUMP head_skills;

-- Separar las skills
skills_listed = FOREACH raw_skills GENERATE url, FLATTEN(TOKENIZE(skills, ',')) AS skill;
-- head_skills_listed = LIMIT skills_listed 10;
-- DUMP head_skills_listed;

-- Eliminar espacios en blanco alrededor de las habilidades y convertirlas a minúsculas
skills_trimmed = FOREACH skills_listed GENERATE url, LOWER(TRIM(skill)) AS skill_trimmed;
-- head_trimmed = LIMIT skills_trimmed 10;
-- DUMP head_trimmed;

-- Filtrar habilidades vacías o nulas
skills_filtered = FILTER skills_trimmed BY skill_trimmed IS NOT NULL AND skill_trimmed != '';

-- Cargar el data set de trabajos asociados a su url
raw_postings = LOAD 'hdfs://cm:9000/uhadoop2024/projects/skills/linkedin_job_postings.csv' USING PigStorage(';') AS (url:chararray, last_processed_time:datetime, got_summary:boolean, got_ner:boolean, is_being_worked:boolean, job_title:chararray, company:chararray, job_location:chararray, first_seen:datetime, search_city:chararray, search_country:chararray, search_position:chararray, job_level:chararray, job_type:chararray);

-- Seleccionar las columnas de interés
selected = FOREACH raw_postings GENERATE url, LOWER(job_title) AS job_title_lower,company,job_location,search_position,search_country;
-- head_postings = LIMIT selected 10;
-- DUMP head_postings;

-- Juntar ambas tablas usando el url como columna común
joined_data = JOIN skills_filtered BY url, selected BY url;
-- head_join = LIMIT joined_data 10;
-- DUMP head_join;

-- Generar una relación que contenga job_title y skills
job_title_skills = FOREACH joined_data GENERATE selected::job_title_lower, skills_filtered::skill_trimmed;

-- Agrupar por job_title y skill para contar la frecuencia de cada habilidad
grouped_by_title_skill = GROUP job_title_skills BY job_title_lower;

-- Aplanar habilidades por título ded trabajo
flattened_skills = FOREACH grouped_by_title_skill  GENERATE group, FLATTEN(job_title_skills);

-- Agrupar por título de trabajo y habilidad específica
grouped_skills = GROUP flattened_skills BY (group, job_title_skills::skills_filtered::skill_trimmed);

-- Contar las habilidades
skill_counts = FOREACH grouped_skills GENERATE FLATTEN(group) AS (job_title_lower, skill_trimmed), COUNT(flattened_skills) AS skill_count;

-- Ordenar habilidades por frecuencia en orden descenciente
order_skill_counts = ORDER skill_counts by skill_count DESC;

-- Juntar el número total de trabajos con el número de trabajos que requieren cada habilidad
joined_totals = JOIN skill_counts BY job_title, total_jobs_by_title BY job_title;

-- Calcular el porcentaje de trabajos que requieren cada habilidad
percentage_skills = FOREACH joined_totals GENERATE skill_counts::job_title AS job_title, skill_counts::skill AS skill, (skill_counts::skill_count * 100.0 / total_jobs_by_title::total_jobs) AS skill_percentage;

-- Ordenar habilidades por porcentaje en orden descendente
ordered_percentage_skills = ORDER percentage_skills BY skill_percentage DESC;

STORE ordered_percentage_skills INTO '/uhadoop2024/projects/skills/sorted_skills';