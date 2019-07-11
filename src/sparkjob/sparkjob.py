#-------------------------------------------------------------
# sparkjob.py
# 
# Description: Finds the connections of a GitHub user and
#              calculates the score of the connections.
#
# author: Mahsa Hayeri
# June 2019 
#-------------------------------------------------------------

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrame
from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.5 pyspark-shell'

import sys
try:
    spark = SparkSession.builder.master("spark://ec2-3-215-129-181.compute-1.amazonaws.com:7077").appName("My Spark Application").config("spark.submit.deployMode", "client").getOrCreate()
except:
    print("Make sure the spark server is running!")
    sys.exit(1)
    
try:
    json_path = sys.argv[1] # the path to the input json files
except:
    print("Pass the input json file as an argument!")
    sys.exit(1)

# @desc     selects the required fields from the input json file
# @param    input json file
# @return   dataframe - action information, dataframe - actor information     
def read_jason(path):

    try:
        df = spark.read.json(path)
    except:
        print("Can't find the input file")
        sys.exit(1)

    # actor information: id, username, url
    try:
        actor_df = df.select(df['actor.id'].alias('actor_id'), df['actor.login'].alias('login_username'), df['actor.url'].alias('url')).groupBy('actor_id', 'login_username', 'url').count()
    except:
        print("Wrong file format!")
        sys.exit(1)

    # action information: type (Create, Push, Fork), actor_id, repo_id
    try:
        action_df = df.select(df['type'], df['actor.id'].alias('actor_id'), df['repo.id'].alias('repo_id')).filter((df['type']=="CreateEvent") | (df['type']=="PushEvent") | (df['type']=="ForkEvent") )
    except:
        print("Wrong file format!")
        sys.exit(1)

    return [action_df, actor_df]


from pyspark.sql.functions import lit

# @desc     finds pairs of actions on the same repo
# @param    dataframe - action information
# @return   dataframe - joint dataframe  
def join_by_repo_id(df):

    df1 = df.alias('df1')
    # rename the columns for the second actor
    df2 = df.alias('df2').withColumnRenamed("actor_id", "actor2_id").withColumnRenamed("type", "type2")
    # join the dataframes where the rows have the same repo_id
    # and first actor_id is less than second actor_id to prevent the duplicates
    joint_df = df1.join(df2, (df1.repo_id == df2.repo_id)&(df1.actor_id < df2.actor2_id), 'inner')
    # add the score column and initialize to 0
    joint_df = joint_df.withColumn("score", lit(0))
    return joint_df    

# score of each type of relationship
CREATE_PUSH = 100
PUSH_PUSH   = 50
CREATE_FORK = 20
PUSH_FORK   = 10
FORK_FORK   = 1

from pyspark.sql import Row
from pyspark.sql import functions as f  
# @desc     calculates the score of each relationship
# @param    row of dataframe - relationship
# @return   row of dataframe - with updated score
def calculate_score_tuple(df_row):
    type1 = str(df_row['type'])
    type2 = str(df_row['type2'])

    # Create-Push relationship
    if (((type1 == "CreateEvent") & (type2 == "PushEvent")) | ((type2 == "CreateEvent") & (type1 == "PushEvent"))):
        return Row(actor_id=(int(df_row['actor_id']), int(df_row['actor2_id'])), score=int(df_row['score'])+CREATE_PUSH)
    # Create-Fork relationship
    elif (((type1 == "CreateEvent") & (type2 == "ForkEvent")) | ((type2 == "CreateEvent") & (type1 == "ForkEvent"))):
        return Row(actor_id=(int(df_row['actor_id']), int(df_row['actor2_id'])), score=int(df_row['score'])+CREATE_FORK)
    # Push-Push relationship
    elif (((type1 == "PushEvent") & (type2 == "PushEvent"))):
        return Row(actor_id=(int(df_row['actor_id']), int(df_row['actor2_id'])), score=int(df_row['score'])+PUSH_PUSH)
    # Push-Fork relationship
    elif (((type1 == "PushEvent") & (type2 == "ForkEvent")) | ((type2 == "PushEvent") & (type1 == "ForkEvent"))):
        return Row(actor_id=(int(df_row['actor_id']), int(df_row['actor2_id'])), score=int(df_row['score'])+PUSH_FORK)
    # Fork-Fork relationship
    elif (((type1 == "ForkEvent") & (type2 == "ForkEvent"))):
        return Row(actor_id=(int(df_row['actor_id']), int(df_row['actor2_id'])), score=int(df_row['score'])+FORK_FORK)

    else:
        return Row(actor_id=(int(df_row['actor_id']), int(df_row['actor2_id'])), score=int(df_row['score']))    

# @desc     write the dataframe into the database
# @param    dataframe, database table name, mode
def write_to_DB(df, table, mode):
    url = "jdbc:postgresql://database-git.cq4qxi57fodj.us-east-1.rds.amazonaws.com/postgres"
    properties = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

    try:
        df.write.jdbc(url=url, table=table, mode=mode, properties=properties)
    except:
        print("Make sure the database server is running!")
        sys.exit(1)


import time
import datetime
start_ts = time.time() # start timestamp

[action_df, actor_df] = read_jason(json_path) # read the input json file
joint_df = join_by_repo_id(action_df) # find relationships
score_df = joint_df.rdd.map(calculate_score_tuple).toDF().groupBy("actor_id").sum("score") # calculate the score of each row(relationship)
final_df = score_df.select(score_df["actor_id._1"].alias("actor1_id"), score_df["actor_id._2"].alias("actor2_id"), score_df["sum(score)"]) # select the required columns

# writing to the database
write_to_DB(actor_df, 'user_info', 'overwrite')
write_to_DB(final_df, 'user_score', 'overwrite')

finish_ts = time.time() # finish timestamp
print(finish_ts-start_ts) # print the execution time
