# ProjectTweets
A time series forecast of the sentiment of the entire dataset at 1 week, 1 month and 3 months going forward with Dashboard

ProjectTweets.csv

Context

This dataset contains 1,600,000 tweets extracted using the twitter api .

Content

It contains the following 5 fields:

ids: The id of the tweet (eg. 4587)

date: the date of the tweet (eg. Sat May 16 23:58:44 UTC 2009)

flag: The query (eg. lyx). If there is no query, then this value is NO_QUERY.

user: the user that tweeted (eg. bobthebuilder).

text: the text of the tweet (eg. Lyx is cool).

Following your analysis, you are then required to make a time series forecast of the sentiment of the entire dataset at
1 week, 1 month and 3 months going forward. 

This forecast must be displayed as a dynamic dashboard.    

Your project must incorporate the following elements:

●           Utilisation of a distributed data processing environment (e.g., Hadoop Map-reduce or Spark), for some part of the analysis.

●           Source dataset(s) can be stored into an appropriate SQL/ NoSQL database(s) prior to processing by MapReduce / Spark (HBase / HIVE / Spark SQL /Cassandra / MongoDB / etc.) The data can be populated into the NoSQL database using an appropriate tool (Hadoop/ Spark etc.)

●           Post Map-reduce processing dataset(s) can be stored into an appropriate NoSQL database(s) (Follow a similar choice as in the previous step)

●           Store the data and then follow-up analysis on the output data. It can be extracted from the NoSQL database into another format, using an appropriate tool, if necessary (e.g. extract to CSV to import into R/ Python etc.).

●           Devise and implement a test strategy in order to perform a comparative analysis of the capabilities of any two databases (MySQL, MongoDB, Cassandra, HBase and CouchDB) in terms of the performance. You should record a set of appropriate metrics and perform a quantitative analysis for             comparison purposes between the two chosen database systems.

●           Provide evidence and justification of your choice of sentiment extraction.

●           Explore at least 2 methods of time-series forecasting.

●           Evidence and justify your choices for your final analysis and include your forecasts at  1 week, 1 month and 3 months.

●           Your dashboard must be dynamic and interactive. Include your design rationale expressing Tufts principles.
