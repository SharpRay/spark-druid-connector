# spark-druid-connector

A library for querying Druid data sources with Apache Spark.
# Compatability

This libaray is compatable with Spark-2.x and Druid-0.9.0+

# Usage

## Compile

```
sbt clean assembly
```

## Using with spark-shell

```
bin/spark-shell --jars spark-druid-connector-assembly-0.1.0-SNAPSHOT.jar
```

In spark-shell, a temp table could be created like this:

```
val df = spark.read.format("org.rzlabs.druid").
  option("druidDatasource", "ds1").
  option("zkHost", "localhost:2181").
  option("hyperUniqueColumnInfo", """[{"column":"city", "hllMetric": "unique_city"}]""").load
df.createOrReplaceTempView("ds")
spark.sql("select time, sum(event) from ds group by time").show
```

or you can create a hive table:

```
spark.sql("""
  create table ds1 using org.rzlabs.druid options (
    druidDatasource "ds1",
    zkHost "localhost:2181",
    hyperUniqueColumnInfo, "[{\"column\": \"city\", \"hllMetric\": \"unique_city\"}]"
  )
""")
```

# Options

|option|required|default value|descrption|
|-|-|-|-|
|druidDatasource|yes|none|data source name in Druid|
|zkHost|no|localhost|zookeeper server Druid use, e.g., localhost:2181|
|zkSessionTimeout|no|30000|zk server connection timeout|
|zkEnableCompression|no|true|zk enbale compression or not|
|zkDruidPath|no|/druid|The druid metadata root path in zk|
|zkQualifyDiscoveryNames|no|true||
|queryGranularity|no|all|The query granularity of the Druid datasource|
|maxConnectionsPerRoute|no|20|The max simultaneous live connections per Druid server|
|maxConnections|no|100|The max simultaneous live connnections of the Druid cluster|
|loadMetadataFromAllSegments|no|true|Fetch metadata from all available segments or not|
|debugTransformations|no|false|Log debug informations about the transformations or not|
|timeZoneId|no|UTC||
|useV2GroupByEngine|no|false|Use V2 groupby engine or not|
|useSmile|no|true|Use smile binary format as the data format exchanged between client and Druid servers|

# Major features

## Currently

* Direct table creating in Spark without requiring of base table.
* Support Aggregate and Project & Filter operators pushing down and transform to GROUPBY and SCAN query against Druid accordingly.
* Support majority of primitive filter specs, aggregation specs and extraction functions.
* Lightweight datasource metadata updating.

## In the future

* Support Join operator.
* Support Limit and Having operators pushing down.
* Suport more primitive specs and extraction functions.
* Support more Druid query specs according to query details.
* Suport datasource creating and metadata lookup.
* ...
