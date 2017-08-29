package org.datavec.api.writable;

import org.datavec.api.writable.comparator.TextWritableComparator;
import org.junit.Test;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import static org.junit.Assert.assertEquals;

public class WritableTest {

    @Test
    public void testWritableEqualityReflexive() {
        assertEquals(new IntWritable(1), new IntWritable(1));
        assertEquals(new LongWritable(1), new LongWritable(1));
        assertEquals(new DoubleWritable(1), new DoubleWritable(1));
        assertEquals(new FloatWritable(1), new FloatWritable(1));
        assertEquals(new Text("Hello"), new Text("Hello"));

        INDArray ndArray = Nd4j.rand(new int[]{1, 100});

        assertEquals(new NDArrayWritable(ndArray), new NDArrayWritable(ndArray));
        assertEquals(new NullWritable(), new NullWritable());
        assertEquals(new BooleanWritable(true), new BooleanWritable(true));
        byte b = 0;
        assertEquals(new ByteWritable(b), new ByteWritable(b));
    }

    @Test
    public void testIntLongWritable() {
        assertEquals(new IntWritable(1), new LongWritable(1));
        assertEquals(new LongWritable(1), new IntWritable(1));
    }
}
