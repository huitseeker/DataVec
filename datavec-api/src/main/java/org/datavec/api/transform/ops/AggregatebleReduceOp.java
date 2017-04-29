package org.datavec.api.transform.ops;

/**
 * Created by huitseeker on 4/28/17.
 */
public interface AggregatebleReduceOp<T, U, V> {

    public U add(U accumulator, T element);

    public U combine(U accu1, U accu2);

    public U neutral();

    public V finalAggregate(U acc);

}
