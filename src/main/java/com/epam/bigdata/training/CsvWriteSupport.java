package com.epam.bigdata.training;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Csv records to parquet format write support.
 * Simply consumes records and writes them as strings, no value type is resolved.
 * Expects incoming record as a list of strings (tokenized csv row).
 */
public class CsvWriteSupport<T> extends WriteSupport<T> {

    private static final Logger log = LoggerFactory.getLogger(CsvWriteSupport.class);

    /**
     * Key to persist schema details in hadoop job configuration,
     */
    public static final String CSV_SCHEMA = "csv.parquet.schema";

    /**
     * Parquet abstraction for writing records.
     */
    private RecordConsumer recordConsumer;

    /**
     * Records schema.
     */
    private MessageType schema;

    @Override
    public WriteContext init(Configuration configuration) {
        MessageType schema = parseSchema(configuration);
        Preconditions.checkNotNull(schema, "Schema has not been specified");

        this.schema = schema;
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(T record) {
        recordConsumer.startMessage();
        Preconditions.checkState(record instanceof ArrayWritable, "Csv write supports only ArrayWritable");

        String[] values = ((ArrayWritable) record).toStrings();

        List<ColumnDescriptor> columns = schema.getColumns();
//        for (int i = 0; i < Math.min(columns.size(), values.length); i++) {
        for (int i = 0; i < values.length; i++) {
            String value = values[i];

//            if (value.length() == 0) {
//                System.out.println("Empty value ! Replacing with '-'");
//                value = "-";
//            }
//            if (value.length() > 0) {
                recordConsumer.startField(columns.get(i).getPath()[0], i);
                recordConsumer.addBinary(Binary.fromCharSequence(value));
                recordConsumer.endField(columns.get(i).getPath()[0], i);
//            }
        }

        recordConsumer.endMessage();
    }

    /**
     * Sets unparsed schema (just a first row of csv file).
     * @param configuration Configuration.
     * @param schema    Raw first csv line (column names).
     */
    public static void setSchema(Configuration configuration, String schema) {
        configuration.set(CsvWriteSupport.CSV_SCHEMA, schema);
    }

    @VisibleForTesting
    static MessageType parseSchema(Configuration configuration) {
        String raw = configuration.get(CSV_SCHEMA);
        StringTokenizer tokenizer = new StringTokenizer(raw, ",");

        Types.MessageTypeBuilder builder = Types.buildMessage();

        while (tokenizer.hasMoreTokens()) {
            builder
                    .primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                    .named(tokenizer.nextToken().trim());
        }

        return builder.named("root");
    }
}
