raw_skills = LOAD 'hdfs://cm:9000/uhadoop2024/projects/skills/job_skills.csv' USING PigStorage(';') AS (url:chararray, skills:chararray);
head_skills= LIMIT raw_skills 10;
DUMP head_skills;

raw_postings = LOAD 'hdfs://cm:9000/uhadoop2024/projects/skills/linkedin_job_postings.csv' USING PigStorage(',') AS (url:chararray, last_processed_time:datetime, got_summary:boolean, got_ner:boolean, is_being_worked:boolean, job_title:chararray, company:chararray, job_location:chararray, first_seen:datetime, search_city:chararray, search_country:chararray, search_position:chararray, job_level:chararray, job_type:chararray);
head_postings= LIMIT raw_postings 10;
DUMP head_postings;

-- Separar las habilidades en una lista
skills_listed = FOREACH raw_skills GENERATE FLATTEN(STRSPLIT(skills, ',')) AS skill;

-- Eliminar espacios en blanco alrededor de las habilidades
skills_trimmed = FOREACH skills_listed GENERATE TRIM(skill) AS skill_trimmed;

-- Contar la frecuencia de cada habilidad
skills_grouped = GROUP skills_trimmed BY skill_trimmed;

skills_count = FOREACH skills_grouped GENERATE group AS skill, COUNT(skills_trimmed) AS count;

-- Ordenar las habilidades por frecuencia
skills_sorted = ORDER skills_count BY count DESC;

-- Guardar los resultados en un archivo
STORE skills_sorted INTO '/uhadoop2024/projects/skills/sorted_skills/' USING PigStorage(',');

-- Mostrar los resultados
head_sorted= LIMIT skills_sorted 10;
DUMP head_sorted;
