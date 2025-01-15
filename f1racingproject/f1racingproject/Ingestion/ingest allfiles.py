# Databricks notebook source
v_result = dbutils.notebook.run("ingest_circuitsfile", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_racefile", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_constructorsfile", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})


# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_driversfile", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_resultsfile", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_pitstopfile", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_laptimesfile", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_qualifyingfile", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result