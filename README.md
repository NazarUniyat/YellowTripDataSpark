# YellowTripDataSpark


In order to run this application on the EMR cluster, you have to:
1. Upload necessary data (https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) to Amazon S3 (If you want to process last 5 years you need to remember that schema was a bit changed in the middle of the 2016 year. In this case, you can just put files with old schema in one folder and with new in another and pass two paths in parameters before start app) 
2. Choose "Go to advanced options" when creating EMR cluster
3. In the root directory, you can find `sparkConfig.json`. This config file is hardcoded for nodes with 32RAM. You can change this configuration according to the parameters of your node.
4. On the first step of the configuration put `sparkConfig.json` under "Edit software settings" options. (Also you can upload this config to S3 and then just choose path).
5. Upload build of this project to S3 (JAR).
6. Once cluster is ready to use you can copy JAR using AWS CLI command `aws s3 cp paths_to_your_jar_on_s3 .`  
7. Use the following command in order to run spark job `spark-submit ./path_to_your_jar 'old=path_to_s3_folder_with_data_generated_with_old_schema' 'new=path_to_s3_folder_with_data_generated_with_new_schema' 'spark.executor.memory=memory_per_one_executor' 'spark.executor.cores=cores_per_one_executor' 'spark.executor.instances=count_of_executors'`