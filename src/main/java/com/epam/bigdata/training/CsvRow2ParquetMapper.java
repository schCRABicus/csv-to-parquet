package com.epam.bigdata.training;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * {@link Mapper} implementation to map the CSV row into the parquet format.
 *
 * Since the {@link org.apache.parquet.hadoop.ParquetOutputFormat} is used with the
 * {@link CsvWriteSupport} injected, the text input string is expected to be tokenized
 * and the resulted array to be written into the context. Later on the {@link ArrayWritable}
 * will be consumed by {@link CsvWriteSupport} and the array elements will be written into the parquet one by one.
 */
public class CsvRow2ParquetMapper extends Mapper<Object, Text, NullWritable, StringArrayWritable> {

    /**
     * CSV row elements delimeter.
     */
    public static final String DELIMITER = ",";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        final String[] values = StringUtils.splitPreserveAllTokens(value.toString(), DELIMITER);

        final StringArrayWritable writable = new StringArrayWritable(values);
        context.write(null, writable);
    }
}
