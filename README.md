# Git-Recruiter
Helping recruiters to find develpors faster.
## Business Use Case
Finding the right coder is time consuming becuase recruiters have to search a very large group of apllicants to find the right person. 

## Solution
The solution to the bussiness can be solved by giving recruiters a targeted list of candidates. GitRecruier uses two resources to narrow down the serach. 
* Developers whom recruiters already know
* Github archive data 

GitRecuter can find other developers who have similar coding skills to the one that recruiter has already known by using GitHub data. GitHub data includes all details about its users action. GitRecruiter would extract the useful and related data which includes 3 actions that user can do on GiHub:
* Create
* Fork
* Push

Based on these type of actions that users has with eachother, the GitRecruier would estimate their connection and return top 10 coders that are related to that specific user the recruiter knows.

## ETL Pipeline

GitHub historic data is saved in an Amazon S3 bucket. Then an offline batch processing Apache Spark job reads and processes the data and save it in a PostgreSQL database. The user facing component of this pipeline is the Dahs application. User can enter the github username of a developer and the application will make query to the database . The results willbe shown in the format of table by using Dash.


## User Interface
Link to [GitRecruier application](https://gitrecruiter.herokuapp.com/).
## Installation
Things are need to be installed
* Apache Spark
* PostgreSQL Database
* Python
* Dash
* Psycopg2

## Presentation Link
Link to [GitRecruiter presentation](https://docs.google.com/presentation/d/1kqAAoDpIRNIdIX9tg9GXVVmA0WcIYgtkIOV1AuagW10/edit?usp=sharing).
