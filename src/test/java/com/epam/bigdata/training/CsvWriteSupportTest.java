package com.epam.bigdata.training;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CsvWriteSupportTest {

    @Mock
    Configuration configurationMock;

    @Test
    public void parseSchema() {
        // given
        Mockito.when(configurationMock.get(CsvWriteSupport.CSV_SCHEMA)).thenReturn("col-1, col-2, col-3");

        // when
        MessageType schema = CsvWriteSupport.parseSchema(configurationMock);

        // then
        Assert.assertNotNull(schema);
        Assert.assertThat(schema.getColumns().size(), Is.is(3));
    }
}