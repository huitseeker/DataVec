package org.datavec.api.transform.ops;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.datavec.api.writable.Writable;

import java.util.List;

/**
 * Created by huitseeker on 5/14/17.
 */
@AllArgsConstructor
public class ByteWritableOp<T> implements IAggregableReduceOp<Writable, T> {

    @Getter
    private IAggregableReduceOp<Byte, T> operation;

    @Override
    public <W extends IAggregableReduceOp<Writable, T>> void combine(W accu) {
        if (accu instanceof ByteWritableOp)
            operation.combine(((ByteWritableOp) accu).getOperation());
    }

    @Override
    public void accept(Writable writable) {
        int val = writable.toInt();
        operation.accept((byte)val);
    }

    @Override
    public T get() {
        return operation.get();
    }
}
