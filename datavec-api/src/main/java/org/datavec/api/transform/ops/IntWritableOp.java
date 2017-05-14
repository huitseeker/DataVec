package org.datavec.api.transform.ops;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.datavec.api.writable.Writable;

import java.util.List;

/**
 * Created by huitseeker on 5/14/17.
 */
@AllArgsConstructor
public class IntWritableOp<T> implements IAggregableReduceOp<Writable, T> {

    @Getter
    private IAggregableReduceOp<Integer, T> operation;

    @Override
    public <W extends IAggregableReduceOp<Writable, T>> void combine(W accu) {
        if (accu instanceof IntWritableOp)
            operation.combine(((IntWritableOp) accu).getOperation());
    }

    @Override
    public void accept(Writable writable) {
        operation.accept(writable.toInt());
    }

    @Override
    public T get() {
        return operation.get();
    }
}
