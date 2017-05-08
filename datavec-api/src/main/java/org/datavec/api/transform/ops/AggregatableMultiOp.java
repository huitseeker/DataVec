package org.datavec.api.transform.ops;

import org.apache.commons.lang3.tuple.Pair;
import org.datavec.api.writable.Writable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by huitseeker on 5/8/17.
 */
public abstract class AggregatableMultiOp<T, U> implements AggregatableReduceOp<T,U,List<Writable>> {

    <V> AggregatableReduceOp<T, Pair<U, V>, List<Writable>> andThen(final AggregatableReduceOp<T, V, List<Writable>> otherOp){
        return new AggregatableMultiOp<T, Pair<U, V>>() {
            @Override
            public Pair<U, V> tally(Pair<U, V> accumulator, T element) {
                return Pair.of(AggregatableMultiOp.this.tally(accumulator.getLeft(), element), otherOp.tally(accumulator.getRight(), element));
            }

            @Override
            public Pair<U, V> combine(Pair<U, V> accu1, Pair<U, V> accu2) {
                return Pair.of(AggregatableMultiOp.this.combine(accu1.getLeft(), accu2.getLeft()), otherOp.combine(accu1.getRight(), accu2.getRight()));
            }

            @Override
            public Pair<U, V> neutral() {
                return Pair.of(AggregatableMultiOp.this.neutral(), otherOp.neutral());
            }

            @Override
            public List<Writable> summarize(Pair<U, V> acc) {
                List<Writable> leftList = AggregatableMultiOp.this.summarize(acc.getLeft());
                List<Writable> rightList = otherOp.summarize(acc.getRight());
                List<Writable> res = new ArrayList<>(leftList);
                res.addAll(rightList);
                return res;
            }
        };
    }

}
