package org.datavec.api.transform.ops;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.datavec.api.transform.condition.Condition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by huitseeker on 5/14/17.
 */
public class DispatchWithConditionOp<Writable, U> implements IAggregableReduceOp<List<Writable>, List<U>> {


    @Getter
    @NonNull
    private List<IAggregableReduceOp<Writable, List<U>>> operations;
    @Getter
    @NonNull
    private List<Condition> conditions;


    public DispatchWithConditionOp(List<IAggregableReduceOp<Writable, List<U>>> ops, List<Condition> conds){
        checkArgument(ops.size() == conditions.size());
        operations = ops;
        conditions = conds;
    }

    @Override
    public <W extends IAggregableReduceOp<List<Writable>, List<U>>> void combine(W accu) {
        if (accu instanceof DispatchWithConditionOp){
            List<IAggregableReduceOp<Writable, List<U>>> otherOps = ((DispatchWithConditionOp<Writable, U>) accu).getOperations();
            for (int i = 0; i < Math.min(operations.size(), otherOps.size()); i++){
                operations.get(i).combine(otherOps.get(i));
            }
            // conditions should be the same
        }
    }

    @Override
    public void accept(List<Writable> ts) {
        for (int i = 0; i < Math.min(operations.size(), ts.size()); i++){
            if (!conditions.isEmpty() && conditions.get(i).condition(ts))
                operations.get(i).accept(ts.get(i));
        }
    }

    @Override
    public List<U> get() {
        List<U> res = new ArrayList<>();
        for (int i = 0; i < operations.size(); i++){
            res.addAll(operations.get(i).get());
        }
        return res;
    }
}
