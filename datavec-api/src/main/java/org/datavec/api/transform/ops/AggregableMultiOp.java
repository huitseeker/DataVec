package org.datavec.api.transform.ops;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.datavec.api.writable.ByteWritable;
import org.datavec.api.writable.Writable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by huitseeker on 5/8/17.
 */
@AllArgsConstructor
public class AggregableMultiOp<T> implements IAggregableReduceOp<T, List<Writable>> {

    @Getter
    @NonNull
    private List<IAggregableReduceOp<T, Writable>> operations;

    public void accept(T t){
        for (int i = 0; i < operations.size(); i++){
            operations.get(i).accept(t);
        }
    }

    public <U extends IAggregableReduceOp<T, List<Writable>>> void combine(U accu) {
        if (accu instanceof AggregableMultiOp){
            AggregableMultiOp<T> accumulator = (AggregableMultiOp<T>) accu;
            List<IAggregableReduceOp<T, Writable>> otherAccumulators = accumulator.getOperations();
            for (int i = 0; i < Math.min(operations.size(), otherAccumulators.size()); i++){
                operations.get(i).combine(otherAccumulators.get(i));
            }
        }
    }

    public List<Writable> get(){
        List<Writable> res = new ArrayList<>(operations.size());
        for (int i = 0; i < operations.size(); i++){
            res.add(operations.get(i).get());
        }
        return res;
    }

}
