raw_skills = LOAD 'hdfs://cm:9000/uhadoop2024/projects/skills/job_sample.csv' USING PigStorage(';') AS (url:chararray, skills:chararray);
head_skills= LIMIT raw_skills 10;
DUMP head_skills;

skills_listed = FOREACH raw_skills GENERATE url, FLATTEN(TOKENIZE(skills, ',')) AS skill;
head_skills_listed = LIMIT skills_listed 10;
DUMP head_skills_listed;
-- Eliminar espacios en blanco alrededor de las habilidades
skills_trimmed = FOREACH skills_listed GENERATE url, TRIM(skill) AS skill_trimmed;
head_trimmed = LIMIT skills_trimmed 10;
DUMP head_trimmed;

-- Filtrar habilidades vacías o nulas
skills_filtered = FILTER skills_trimmed BY skill_trimmed IS NOT NULL AND skill_trimmed != '';

raw_postings = LOAD 'hdfs://cm:9000/uhadoop2024/projects/skills/posting_sample.csv' USING PigStorage(';') AS (url:chararray, last_processed_time:datetime, got_summary:boolean, got_ner:boolean, is_being_worked:boolean, job_title:chararray, company:chararray, job_location:chararray, first_seen:datetime, search_city:chararray, search_country:chararray, search_position:chararray, job_level:chararray, job_type:chararray);

selected= FOREACH raw_postings GENERATE url,job_title,company,job_location,search_position,search_country;
head_postings = LIMIT selected 10;
dump head_postings;

joined_data = JOIN skills_filtered BY url, selected BY url;
head_join=  limit joined_data 10;
dump head_join;

-- Generar una relación que contenga job_title y skills
job_title_skills = FOREACH joined_data GENERATE selected::job_title, skills_filtered::skill_trimmed;

-- Agrupar por job_title y skill para contar la frecuencia de cada habilidad
grouped_by_title_skill = GROUP job_title_skills BY job_title;

flattened_skills = FOREACH grouped_by_title_skill  GENERATE group, FLATTEN(job_title_skills);

grouped_skills = GROUP flattened_skills BY (group, job_title_skills::skills_filtered::skill_trimmed);

skill_counts = FOREACH grouped_skills GENERATE FLATTEN(group) AS (job_title, skill_trimmed), COUNT(flattened_skills) AS skill_count;

order_skill_counts= Order skill_counts by skill_count DESC;

----
--grouped_by_title = GROUP skill_counts BY job_title;

--ordered_skills= FOREACH grouped_by_title {
--    sorted_skills = ORDER skill_counts BY skill_count DESC;
--    GENERATE group AS job_title, sorted_skills;