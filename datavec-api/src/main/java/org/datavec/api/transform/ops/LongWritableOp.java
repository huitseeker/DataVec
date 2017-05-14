package org.datavec.api.transform.ops;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.datavec.api.writable.Writable;

import java.util.List;

/**
 * Created by huitseeker on 5/14/17.
 */
@AllArgsConstructor
public class LongWritableOp<T> implements IAggregableReduceOp<Writable, T> {

    @Getter
    private IAggregableReduceOp<Long, T> operation;

    @Override
    public <W extends IAggregableReduceOp<Writable, T>> void combine(W accu) {
        if (accu instanceof LongWritableOp)
            operation.combine(((LongWritableOp) accu).getOperation());
    }

    @Override
    public void accept(Writable writable) {
        operation.accept(writable.toLong());
    }

    @Override
    public T get() {
        return operation.get();
    }
}