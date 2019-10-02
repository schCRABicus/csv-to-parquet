package com.epam.bigdata.training;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

@Ignore("Fails due to the null pointer issue - MRUnit does not support null keys")
public class CsvRow2ParquetMapperTest {

    private MapDriver<Object, Text, NullWritable, StringArrayWritable> driver;

    @Before
    public void setUp() {
        driver = new NullSafeMapDriver<Object, Text, NullWritable, StringArrayWritable>().withMapper(new CsvRow2ParquetMapper());
    }

    @Test
    public void mapperSplitsTheCsvRowIntoTheElementsArray() throws IOException {
        // when & then
        driver
                .withInput(new Text(""), new Text("v-1, v-2, v-3"))
                .withOutput(NullWritable.get(), new StringArrayWritable(new String[] { "v-1", "v-2", "v-3" }))
                .runTest();
    }

    @Test
    public void mapperSplitsTheCsvRowIntoTheElementsArrayWithEmptyValues() throws IOException {
        // when & then
        driver
                .withInput(new Text(""), new Text("v-1,, v-2,, v-3"))
                .withOutput(NullWritable.get(), new StringArrayWritable(new String[] { "v-1", "", "v-2", "", "v-3" }))
                .runTest();
    }

    /**
     * There is an issue in {@link org.apache.hadoop.mrunit.TestDriver} with copy method
     * which does not perform nullness check. For that reason the null key or value is not copied and
     * {@link NullPointerException} gets thrown.
     * @param <K1>
     * @param <V1>
     * @param <K2>
     * @param <V2>
     */
    private static class NullSafeMapDriver<K1, V1, K2, V2> extends MapDriver<K1, V1, K2, V2> {
        @Override
        protected <E> E copy(E object) {
            return object != null ? super.copy(object) : null;
        }
    }
}