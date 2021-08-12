# S3 Data Lake Pipeline Using Spark

- The aim of this project is building an ETL pipeline for an S3 data lake. JSON logs will be extracted from an S3 data warehouse with Spark and transformed using a star schema to an S3 data lake. The S3 data warehouse contains over 14,000 JSON logs of user activity and metadata for a fictitious startup called Sparkify. The transformed data could be queried to gain insight on user activity. 

## Project Files

### dl.cfg
    - Provides AWS credentials in order to run a Spark session

### etl.py
    - Creates a Spark session, extracts songs data and log data, transforms the data into songs, artists, users, and time dimension tables and a songplays fact table,
    then loads the transformed data into a S3 data lake for use by the Sparkify analytics team
    
### etl_test.ipynb
    - Jupyter notebook used to quickly test code as a substitute for running etl.py 
    
## How to Run Project
1. Enter AWS credentials into dl.cfg
2. Run etl.py as ```python3 etl.py``` in terminal
5. Run sample query to test data


## Sample Query
**These queries are not optimized and will take a while to run**
These two simple queries demonstrate that the ETL pipeline is successful and available for the analytics team to run queries.

![Artists with most minutes of music](artists_songs_sum.png)

![Songs by artist](songs_by_artist_query_image.png)
