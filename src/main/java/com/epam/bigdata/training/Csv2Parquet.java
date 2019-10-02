package com.epam.bigdata.training;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Entry point to the application. Defines Job details and runs MapReduce task.
 */
public class Csv2Parquet extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(Csv2Parquet.class);

    public int run(String[] args) throws Exception {
        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);

        final Job job = new Job(getConf());
        job.setJarByClass(getClass());
        Configuration conf = job.getConfiguration();

        final String columnsRow = readCsvColumnsRow(inputPath, conf);
        log.info("CSV first row : {}", columnsRow);

        // point to input data
        FileInputFormat.addInputPath(job, inputPath);

        // set the output format
        job.setOutputFormatClass(ParquetOutputFormat.class);
        ParquetOutputFormat.setWriteSupportClass(job, CsvWriteSupport.class);
        ParquetOutputFormat.setOutputPath(job, outputPath);
        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setCompressOutput(job, true);
        CsvWriteSupport.setSchema(conf, columnsRow);

        job.setMapperClass(CsvRow2ParquetMapper.class);
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private String readCsvColumnsRow(Path source, Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(configuration);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(source)))) {
            return br.readLine();
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Csv2Parquet(), args);
        System.exit(exitCode);
    }
}
