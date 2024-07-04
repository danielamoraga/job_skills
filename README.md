# Pig Script Execution Guide

This guide explains how to run two Pig scripts sequentially. The first script processes input data and stores the result, and the second script loads the stored result for further processing.

## Prerequisites

- Apache Pig installed and configured
- Input data file located at `hdfs://cm:9000/uhadoop2024/projects/skills/`

## Scripts & Execution Steps

### `table_creation.pig`

This script loads the input data, filters it, groups it, and stores the result.

To excecute this script and store the processed data:
`pig table_creation.pig`

### `query.pig`

This script orders the data by descending percentage and filters by a job returning the 10 skills most solicitated in that job and their percentages.

To search a job, it has to be written in lowercase. For example, 'chef':
`pig -param job='chef' query.pig`


