# S3_ETL
ETL pipeline which works with S3 bucket

# Installation and configurtion
  1. Install docker-compose ([doc](https://docs.docker.com/compose/install/))
  2. Clone repo
  3. Enter created directory
  4. Update variables.env with yours AWS credentials
  5. `docker-compose build`
  6. `docker-compose up`
  
# Notes
  1. Script automatically starts when `docker-compose up` called
  2. Script (docker-compose volumes, to be precise) creates database and logs directories (you can use database to explore loaded data)
  3. In logs folder there are 2 files script_result.log contains list of file processed and gives script summary(with loaded data examples), error.log contains errors and log any exceptions thrown in main func. 

**Fun fact**: There are quite a lot apps where size_bytes is divisible by 256
  
