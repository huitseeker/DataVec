package org.datavec.api.transform.ops;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.datavec.api.writable.Writable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by huitseeker on 5/8/17.
 */
public abstract class AggregatableMultiOp<T, U> implements AggregatableReduceOp<T,U,List<Writable>> {

    <W extends Writable> AggregatableMultiOp<T, U> fromOp(final AggregatableReduceOp<T, U, W> op){
        return new AggregatableMultiOp<T, U>() {

            @Override
            public U tally(U accumulator, T element) {
                return op.tally(accumulator, element);
            }

            @Override
            public U combine(U accu1, U accu2) {
                return op.combine(accu1, accu2);
            }

            @Override
            public U neutral() {
                return op.neutral();
            }

            @Override
            public List<Writable> summarize(U acc) {
                return Collections.singletonList((Writable) op.summarize(acc));
            }
        };
    }

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
