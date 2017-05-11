package org.datavec.api.transform.ops;

/**
 * Created by huitseeker on 4/28/17.
 */
public interface AggregableReduceOp<T, U, V> {

    public U tally(U accumulator, T element);

    public U combine(U accu1, U accu2);

    public U neutral();

    public V summarize(U acc);

}
