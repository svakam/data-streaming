# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch data from source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load libraries

# COMMAND ----------

# install.packages("RSocrata")
# library("RSocrata")
install.packages("rjson")
library(httr)
library("rjson")

# start spark session on the cluster
library(SparkR)
sparkR.session()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Test GET call

# COMMAND ----------


PMP_endpoint <- "https://data.wa.gov/resource/8y5c-ekcc.json?"
limit <- 500
offset <- 0
param_limit <- paste("$limit=",limit, sep = "", collapse = "")
param_offset <- paste("&$offset=",offset, sep = "", collapse = "")
param_order <- "&$order=yearqtr"
PMP_df <- NULL

v <- 1:2
for (n in v) {
  PMP_endpoint_params <- paste(PMP_endpoint,param_limit,param_order,param_offset, sep = "", collapse = "")
  offset <- offset + limit
  param_offset <- paste("&$offset=",offset, sep = "", collapse = "")

  PMP_data <- GET(PMP_endpoint_params, 
               "app_token" = "AdlTgBAS42ql0KcjOvL7zww2V", 
               accept_json()
               )
  PMP_text_data <- content(PMP_data, as="text", type="application/json")
  PMP_r <- fromJSON(PMP_text_data)
  # print(head(PMP_r))
  # print(str(PMP_r))
  # str(PMP_r)
  PMP_df <- createDataFrame(PMP_r)
  print(PMP_df)
  PMP_df
  # head(PMP_df)
  # print(PMP_df)
  # head(PMP_df)
  # print(PMP_endpoint_params)
  # print(class(PMP_data)) -> "response"
  # print(content(PMP_data, type="application/json"))
  # print(status_code(PMP_data))
  # print(http_error(PMP_data)) -> "false" if success
  # print(headers(PMP_data))
}
print("done")
# print(PMP_data)
