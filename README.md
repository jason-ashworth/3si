# 3si

## Question 1
- https://github.com/jason-ashworth/3si/blob/main/Question1/question1.sql
- I loaded the flat file text document in to a local Microsoft SQL Server on my laptop into a table called "ProblemOne"
- The question1.sql file represents the investigative queries I employed
- I did not come to one final answer, other than perhaps the point of this question is to get a glimpse into thought processes
- Some queries not included were brief, hard-coded explorations into if column1 would need to be a prime, or follow the Fibonnaci sequence
    - I did not have any luck with those approaches

## Question 2
- https://github.com/jason-ashworth/3si/blob/main/Question2/question2.sql
- The question2.sql file contains a series of queries that provide the answers asked for this question
- Like Question 1, I loaded the sample data for this question into my local MSSQL instance as two separate tables (seen in the queries)
- I used common table expresions (CTEs) to round up the initial data that would be used in queries to solve the problems
- I did include comments on within the file as to why I made some of the decisions I did
- https://github.com/jason-ashworth/3si/blob/main/Question2/distinct_groupby_analysis.sql
    - This is what I created to show the performance difference between DISTINCT and GROUP BY when querying for unique rows of data
    - If you run this, or similar logic, in SQL Server Management Studio (SSMS) - check the "Messages" tab for the time output

## Question 3
- https://github.com/jason-ashworth/3si/blob/main/Question3/python_version_of_question2.ipynb
- This is a Jupyter notebook that solves Question 2 using Python, working off of the same local MSSQL tables I created for Question 2
- I did notice an issue with obtaining distinct rows using Pandas and Python versus SQL
    - Pandas dataframes treat NULL rows as unique, even if every field is NULL
    - I believe this has something to do with the discrepancy I noticed in that the Python version had 2 more rows than the SQL version for the NCES dataset
    - This is denoted as comments in the Jupyter notebook and I would pursue relentlessly to resolve if not adhering to the suggested time

## Question 4
- https://github.com/jason-ashworth/3si/tree/main/Question4
- This folder contains three files:
    - Dockerfile
    - question4.py
    - requirements.txt
- This solutions answers Question 4 by using an Ubuntu base image and spark in the requirements.txt
- The question4.py contains a mixture of pure Python and PySpark to accomplish the task
- Ultimately, this container would run in a cloud service like AWS ECS and ECR, which can automatically scale up the number of instances to accommodate higher workloads
- To run the container locally, navigate to the directory with the three files and run:
    - ``docker build -t question-4-app .``
    - ``docker run question-4-app``
- I did attempt building this in the Databricks Free Version first and made some great progress; however, the serverless compute and runtimes between Databricks, Spark, and Python became out of sync during multiple executions
    - Apparently this is somewhat common based on my troubleshooting research, so decided to replicate the logic using Docker, Ubuntu, and Spark