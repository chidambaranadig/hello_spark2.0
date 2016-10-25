#Spark 2.0 Introduction

This is a simple Spark Application that I implemented to play with Spark 2.0

The idea for this spark application is inspired from an _Introduction to Spark_ Jupyter Notebook found on the [Databricks Website.](www.databricks.com/try)

This application was developed and run on my laptop with Ubuntu 16.04LTS, Intel Core-i7 Processor and 8GB RAM.

###Input Dataset
The Input Dataset for my Spark Application is taken from the [**SF Open Data**](https://data.sfgov.org/) website. 

I chose to work on data collected by the San Francisco Fire Department.

The first dataset is a compilation of all the calls made to the San Francisco Fire Department. This is a CSV File of 1.6GB with 4.1Million Rows.

The second dataset is a compilation of all the Fire Incidents that took place in San Francisco. This is a CSV File of 141MB with around 412K rows.

I worked on these datasets when they were both last updated on October 22nd 2016.

###Performance Tuning
Since all my testing was run on my local machine, there was little leeway for playing around with Spark Configurations. However, here are some settings I used to improve performance.

####1. Driver and Executor Memory
I set the `spark.executor.memory=4g` and `spark.driver.memory=4g`.

####2. Driver and Executor Core
The machine that I am using has 8 CPUs, hence I set `spark.driver.cores=1` and `spark.executor.cores=4`

_You'd probably not want to allocate all 8 CPUs and all the RAM to your Spark application...unless you want to majorly slow down your computer to the extent that you can't really do any work on it. ;-)_

####3. Re-Partitioning RDDs
In this setup, the Spark master is going to spin up just 1 executor across the entire Spark cluster (of 1 Node).
Therefore, repartitioning my RDDs to a multiple of 4 (because I've set 4 cores for my executor) will achieve better performance than using the spark's default RDD partitions of 13.
This can be accomplished by running:
```
val fireIncidents = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(fireIncidentsSchema).load("/home/cnadig/Developer/Fire_Incidents.csv")
val fireIncidents2 = fireIncidents.repartition(16)

val fireCalls = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(fireCallsSchema).load("/home/cnadig/Developer/Fire_Department_Calls_for_Service.csv")
val fireCalls2 = fireCalls.repartition(16)

```
####4. Caching
Most of the actions I executed on the DataFrame Transformations would finish within a minute without any caching.

However, when you cache your Data Frames, you'll achieve better performance. Your DataFrame actions will finish within 1 second or at most 2 seconds.

DataFrames can be cached this way,

```
fireCallsDF.cache
fireIncidendsDF.cache
```

Once I cached my DataFrames, I was able to run several Join and Aggregate operations within a second.
_Although my Ubuntu OS started getting a little slow at this point_

Also note that my input Datasets was 1592MB + 141MB (= 1733MB).
With driver memory set to 4GB and Executor Memory set to 4GB, there was enough space for me to cache both my DataFrames.

The spark application might run into JAVA Heap Space errors if you try to cache more memory than what your spark application is allocated.

###Analysis
The analysis on these datasets was done using Spark DataFrames and its API.

Some of the DataFrame transformations I have used are `select()`, `groupBy()`, `orderBy()`, `count()` and `join()`

Here are some Questions that can be answered by analyzing this dataset:

####1. Number of Fire Incidents and Calls to the Fire Department By Year
|Year|FireIncidents|FireCalls|
|---|---|---|
|2000|         null|   166273|
|2001|         null|   221699|
|2002|         null|   227120|
|2003|        32819|   240527|
|2004|        29013|   235507|
|2005|        27224|   233051|
|2006|        27212|   235660|
|2007|        30342|   236293|
|2008|        29811|   250886|
|2009|        30180|   245678|
|2010|        31217|   256899|
|2011|        31303|   269487|
|2012|        31831|   266209|
|2013|        30555|   274062|
|2014|        26225|   281681|
|2015|        31418|   297724|
|2016|        23663|   241652|

####2. Number of Fire Incidents By Neighborhood

|NeighborhoodDistrict          |count|
|---|---|
|                              |72564|
|Tenderloin                    |35156|
|Financial District/South Beach|32224|
|Mission                       |32018|
|South of Market               |29398|
|Western Addition              |15520|
|Bayview Hunters Point         |14291|
|Nob Hill                      |13390|
|Pacific Heights               |10198|
|Chinatown                     |9776 |
|Hayes Valley                  |9502 |
|Castro/Upper Market           |9383 |
|Marina                        |8947 |
|Sunset/Parkside               |8917 |
|North Beach                   |7977 |
|Potrero Hill                  |7456 |
|Russian Hill                  |6801 |
|Bernal Heights                |6388 |
|Outer Richmond                |6066 |
| Lakeshore                     |5989 |
| West of Twin Peaks|5867|
| Haight Ashbury|5716|
| Excelsior|5208|
| Inner Sunset|5082|
| Outer Mission|4985|
| Mission Bay|4943|
| Noe Valley|4488|
| Oceanview/Merced/Ingleside|4389|
| Lone Mountain/USF|4382|
| Inner Richmond|3677|
| Presidio Heights|3339|
| Golden Gate Park|3314|
| Portola|2936|
| Visitacion Valley|2704|
| Japantown|2413|
| Glen Park|1831|
| Twin Peaks|1778|
| Presidio|1427|
| Treasure Island|774|
| McLaren Park|672|
| Lincoln Park|494|
| Seacliff|433|



####3. Number of Calls made to the San Francisco Fire Department By Neighborhood
                                         
| NeighborhoodDistrict          | count |
|---|---|
| Tenderloin                    | 544524 |
| South of Market               | 393574|
| Mission                       | 381476|
| Financial District/South Beach| 278196|
| Bayview Hunters Point         | 227920|
| Sunset/Parkside               | 166657|
| Western Addition              | 155949|
| Nob Hill                      | 138360|
| Outer Richmond                | 114548|
| Hayes Valley                  | 103241|
| Castro/Upper Market           | 98517 |
| West of Twin Peaks            | 95663 |
| Chinatown                     | 91164 |
| North Beach                   | 89667 |
| Pacific Heights               | 87060 |
| Excelsior                     | 84716 |
| Bernal Heights                | 80241 |
| Marina                        | 78989 |
| Potrero Hill                  | 76811 |
| Inner Sunset                  | 68867 |
|Russian Hill|66675|
|Haight Ashbury|63115|
|Outer Mission|61515|
|Inner Richmond|61320|
|Oceanview/Merced/Ingleside|61215|
|Lakeshore|60428|
|Visitacion Valley|56638|
|Lone Mountain/USF|50192|
|Noe Valley|47729|
|Mission Bay|42640|
|Portola|39614|
|Presidio Heights|38214|
|Japantown|37094|
|Treasure Island|25966|
|Twin Peaks|24510|
|Golden Gate Park|23946|
|Glen Park|19986|
|None|14794|
|Presidio|13314|
|Seacliff|6734|
|McLaren Park|4655|
|Lincoln Park|3974|


####4. Types of Calls made to the SF Fire Department

| CallType          | count |
|---|---|
|Medical Incident|2651155|                                                      
|Structure Fire|569258|
|Alarms|441974|
|Traffic Collision|162408|
|Other|65597|
|Citizen Assist / Service Call|63238|
|Outside Fire|47430|
|Administrative|30129|
||26961|
|Vehicle Fire|20251|
|Water Rescue|18681|
|Gas Leak (Natural and LP Gases)|14666|
|Odor (Strange / Unknown)|11934|
|Electrical Hazard|11262|
|Elevator / Escalator Rescue|10608|
|Smoke Investigation (Outside)|8932|
|Fuel Spill|4931|
|Transfer|4200|
|HazMat|3555|
|Industrial Accidents|2739|
|Explosion|2418|
|Aircraft Emergency|1502|
|Assist Police|1168|
|Train / Rail Incident|1056|
|High Angle Rescue|1030|
|Watercraft in Distress|864|
|Extrication / Entrapped (Machinery, Vehicle)|581|
|Oil Spill|507|
|Confined Space / Structure Collapse|366|
|Mutual Aid / Assist Outside Agency|358|
|Marine Fire|356|
|Suspicious Package|277|
|Train / Rail Fire|10|
|Lightning Strike (Investigation)|6|
