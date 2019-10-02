# CSV to Parquet MapReduce Job

## Implementation Details

The entry point to the application is [Csv2Parquet](./src/main/java/com/epam/bigdata/training/Csv2Parquet.java) class
which has a main method, starting a ToolRunner. In addition to that, it defines the MapReduce job.
The idea is to provide the hdfs path to csv file as an input and the hdfs path to output. 
`org.apache.parquet.hadoop.ParquetOutputFormat` is used as the job output format. It requires the implementation of
`org.apache.parquet.hadoop.api.WriteSupport` which specifies the way the input record gets converted to parquet format.
The [CsvWriteSupport](./src/main/java/com/epam/bigdata/training/CsvWriteSupport.java) is the implementation of 
`org.apache.parquet.hadoop.api.WriteSupport` which tells parquet to start a new message and then to add row values as 
binary data. 

The MapReduce Job consists of only the map stage. 
The `com.epam.bigdata.training.CsvRow2ParquetMapper` is the mapper implementation which tokenizes the input lines by comma separator
and converts the strings array into the `org.apache.hadoop.io.ArrayWritable` which is later consumed by `CsvWriteSupport`.

In order to be able to run the jar file by hadoop, the fat jar has been created
via `maven-assembly-plugin`. The output jar is suffixed with `-jar-with-dependencies` 
and contains all mandatory dependencies.

## How to run

The MapReduce Job has been run in pseudo-distributed mode against the 
hdfs cluster running in Docker environment. YARN is used in standalone mode.

To execute the job locally, the following two steps required:

1. First, the hadoop user with the proper rights has to be specified in order
to be able to write to hdfs: 
```
export HADOOP_USER_NAME=admin
```

2. Secondly, to activate the Presudo-Distributed mode, fs.defaultFS setting has to be set to 
point at hdfs NameNode in `etc/hadoop/core-site.xml` inside the `<configuration></configuration>` tag:
```xml
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:8020</value>
</property>
```

3. Finally, the job can be started from inside the hadoop folder via the following command
```
bin/hadoop jar /mnt/disk-d/IdeaWorkspace/csv-to-parquet-conversion/target/csv-to-parquet-conversion-1.0-SNAPSHOT-jar-with-dependencies.jar \
    com.epam.bigdata.training.Csv2Parquet \
    /user/admin/expedia_recommendations/test.csv \
    /user/admin/expedia_recommendations/test-updated.parquet
```
where the latter two parameters point at the input csv file location and the desired output file location.
