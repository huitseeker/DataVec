package org.datavec.api.writable;
import org.nd4j.linalg.api.ndarray.INDArray;

/**
 * Created by huitseeker on 5/13/17.
 */
public class UnsafeWritableInjector {
    public static <T> Writable inject (T x){
        if (x instanceof INDArray) {
            throw new IllegalArgumentException("Wrong argument of type INDArray (" +
                    x.getClass().getName() + ") please use org.datavec.common.data.NDArrayWritable manually to convert.");
        }
        else if (x instanceof Integer) {
            return new IntWritable((Integer) x);
        } else if (x instanceof Long) {
            return new LongWritable((Long) x);
        } else if (x instanceof Float) {
            return new FloatWritable((Float) x);
        } else if (x instanceof Double) {
            return new DoubleWritable((Double) x);
        } else if (x instanceof String) {
            return new Text((String) x);
        } else if (x instanceof Text) {
            return (Text)x;
        } else if (x instanceof Byte) {
            return  new ByteWritable((Byte) x);
        } else if (x == null){
            return new NullWritable();
        } else
            throw new IllegalArgumentException("Wrong argument type for writable conversion " + x.getClass().getName());
    }
}
