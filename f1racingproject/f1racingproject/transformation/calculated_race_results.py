# Databricks notebook source
# MAGIC %sql
# MAGIC --- USE f1_processed;
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/pathfile"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------


spark.sql(f"""
              CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
              (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
              )
              USING DELTA
""")

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TEMP VIEW race_result_updated AS
    SELECT races.race_year,
           constructors.name AS team_name,
           drivers.driver_id,
           drivers.name AS driver_name,
           races.race_id,
           results.position,
           results.points,
           11 - results.position AS calculated_points
      FROM f1_processed.results
      JOIN f1_processed.drivers ON results.driver_id = drivers.driver_id
      JOIN f1_processed.constructors ON results.constructor_id = constructors.constructor_id
      JOIN f1_processed.races ON results.race_id = races.race_id
     WHERE results.position <= 10
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from race_result_updated;

# COMMAND ----------

spark.sql(f"""
              MERGE INTO f1_presentation.calculated_race_results tgt
              USING race_result_updated upd
              ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
              WHEN MATCHED THEN
                UPDATE SET tgt.position = upd.position,
                           tgt.points = upd.points,
                           tgt.calculated_points = upd.calculated_points,
                           tgt.updated_date = current_timestamp
              WHEN NOT MATCHED
                THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
                     VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)
       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from race_result_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.calculated_race_results
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT driver_id FROM f1_processed.drivers LIMIT 10;
# MAGIC SELECT constructor_id FROM f1_processed.constructors LIMIT 10;
# MAGIC SELECT race_id FROM f1_processed.races LIMIT 10;
# MAGIC SELECT race_id FROM f1_processed.results LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM f1_processed.results r
# MAGIC JOIN f1_processed.drivers d ON r.driver_id = d.driver_id;
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM f1_processed.results r
# MAGIC JOIN f1_processed.constructors c ON r.constructor_id = c.constructor_id;
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM f1_processed.results r
# MAGIC JOIN f1_processed.races ra ON r.race_id = ra.race_id;
# MAGIC