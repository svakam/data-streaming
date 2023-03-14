# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch data from source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load libraries

# COMMAND ----------

# install.packages("RSocrata")
library(httr)
# library("RSocrata")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Test GET call, offsetting by 1500

# COMMAND ----------

PMP_endpoint <- "https://data.wa.gov/resource/8y5c-ekcc.json?"
limit <- 1500
offset <- 0
param_limit <- paste("$limit=",limit, sep = "", collapse = "")
param_offset <- paste("&$offset=",offset, sep = "", collapse = "")
param_order <- "&$order=yearqtr"

v <- 1:5
for (n in v) {
  PMP_endpoint_params <- paste(PMP_endpoint,param_limit,param_offset,param_order, sep = "", collapse = "")
  offset <- offset + limit
  param_offset <- paste("&$offset=",offset, sep = "", collapse = "")
  print(PMP_endpoint_params)

  PMP_data <- GET(PMP_endpoint_params, 
               "app_token" = "AdlTgBAS42ql0KcjOvL7zww2V"
               )
  print(status_code(PMP_data))
  print(warn_for_status(PMP_data))
}
print("done")
