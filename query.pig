-- Definir parámetro de entrada del título del trabajo a buscar
%default job 'account executive'

table = LOAD 'hdfs://cm:9000/uhadoop2024/projects/skills/sorted_skills' AS (job_title:chararray, skill:chararray, skill_percentage:double);

-- Filtrar por el trabajo que se desea buscar usando el parámetro
filtered_skills = FILTER table BY job_title == '$job';

-- Hacer un head de las 10 primeras habilidades más frecuentes
head_filtered_skills = LIMIT filtered_skills 10;

-- Mostrar las 10 habilidades más frecuentes con su porcentaje
DUMP head_filtered_skills;