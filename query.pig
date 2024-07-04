-- Definir parámetro de entrada del título del trabajo a buscar
%default job 'account executive'

table = LOAD 'hdfs://cm:9000/uhadoop2024/projects/skills/sorted_skills' AS (job_title:chararray, skill:chararray, skill_percentage:double);

-- Ordenar la columna de porcentajes
ordered_table = ORDER table BY skill_percentage DESC;

-- Filtrar por el trabajo que se desea buscar usando el parámetro
filtered_skills = FILTER ordered_table BY job_title == '$job';

-- Hacer un head de las 10 primeras habilidades más frecuentes
head_filtered_skills = LIMIT filtered_skills 10;

-- Mostrar las 10 habilidades más frecuentes con su porcentaje
DUMP head_filtered_skills;