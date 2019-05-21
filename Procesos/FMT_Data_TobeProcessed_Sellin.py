# Databricks notebook source
# MAGIC  %run "../Libraries/ADP_Farmatic_Process"

# COMMAND ----------

#################################################################################
""" Main Process for Farmatic Canonize Files

"""
 #Who                 When           What
 #Victor Salesa       05/11/2018     Initial Version
 #Victor Salesa       07/11/2018     Added Window function to get the latest timestamp and filter file list by it
################################################################################


# Create File list from Farmatic TobeProcessed Folder
filelist = dbutils.fs.ls(__PHARMATIC_TOBEPROCESSED_BASE_PATH__)

# Process Landing Files
try: 
  
  # Create Dataframe with the fields and create original_name and timestamp fields for later using
  file_df = (
    createDFFromList(filelist)
      .filter(col("name").substr(-20,2)=='LD')
      .withColumn("timestamp",col("name").substr(-18,14))
      .withColumn("original_name",col("name").substr(lit(0),(length(col("name"))-21)))
  )
  
  #Create a window function to get the latest timestamp of the file
  windowSpec = Window.partitionBy(file_df['original_name']).orderBy(file_df['timestamp'].desc())
  
  #Filter files to have just the latest timestamp version
  file_df = (file_df.withColumn("latest_timestamp",first("timestamp").over(windowSpec))
         .filter(col("timestamp")==col("latest_timestamp"))
  )
  
  #Process all tobeprocessed files and generate the canonical
  #Get the column of files for the SL filetype
  name_column_PR = file_df.rdd.filter(lambda f: 'GPR' in f.name).take(20)


  #Convert the column to an iterator of parameters to feed the ThreadPool
  parameter_list_PR = ((__PHARMATIC_TOBEPROCESSED_BASE_PATH__,
                   str(row.name),
                   __PHARMATIC_ERRORPROCESS_BASE_PATH__,
                   __PHARMATIC_CANONICAL_BASE_PATH__,
                   True
                  ) 
                  for row in name_column_PR)

  
  
  #Create thread pool to paralelize canonical generation
  threadPool_PR = ThreadPool(32)
  
  #Configure spark to be optimized to paralilize
  spark.conf.set("spark.scheduler.mode",'FAIR')
  
  print("Start Canonizing Files")
  
#Run thread pool with the list of files to be 
  results_PR = (
    threadPool_PR.starmap(
      ProcessPharmaticFilesDataSellin,
      parameter_list_PR,
      5
    )
  )
  
  threadPool_PR.close()
  threadPool_PR.join()
  
  print("End Canonizing Files")
  
  #Full Sellin processed files read
  df_full_Sellin = (spark.read.format('csv')
                  .options(header='true',charset='UTF-8')
                  .option("sep","|")
                  .option("inferSchema",True)
                  .load(__PHARMATIC_CANONICAL_BASE_PATH__+'/PR/6*_CMD.csv')
                 )

  #Full Sellout processed files write to Hive Metastore in order to be able to fectch data from Tableau / other BI
  df_full_Sellin.write.mode('overwrite').saveAsTable("SELLIN_SPAIN")
  sqlContext.refreshTable("SELLIN_SPAIN")
  
  
except Exception as e:
  print(e)