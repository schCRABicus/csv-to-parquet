package com.epam.bigdata.training;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Arrays;

/**
 * Accroding to {@link ArrayWritable} javadocs, it must be subclassed with a proper type specified
 * in order to be deserializable as there is no default constructor.
 *
 * So, this class has been introduced just for the sake of deserialization.
 */
public class StringArrayWritable extends ArrayWritable {

    public StringArrayWritable() {
        this(Text.class);
    }

    public StringArrayWritable(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    public StringArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
        super(valueClass, values);
    }

    public StringArrayWritable(String[] strings) {
        super(strings);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.get());
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof StringArrayWritable
                && Arrays.equals(this.get(), ((StringArrayWritable) obj).get());
    }
}
