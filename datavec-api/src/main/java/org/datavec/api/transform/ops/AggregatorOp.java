package org.datavec.api.transform.ops;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Created by huitseeker on 5/1/17.
 */
public abstract class AggregatorOp<T, U, V> implements AggregatableReduceOp<T, U, V> {

    public abstract U tally(U accumulator, T element);

    public abstract U combine(U accu1, U accu2);

    public abstract U neutral();

    public abstract V summarize(U acc);

    public <W, X> AggregatorOp<T, Pair<U, W>, Pair<V, X>> compose(final AggregatorOp<T, W, X> otherAggregator) {
        return new AggregatorOp<T, Pair<U, W>, Pair<V, X>>() {
            @Override
            public Pair<U, W> tally(Pair<U, W> accumulator, T element) {
                return Pair.of(AggregatorOp.this.tally(accumulator.getLeft(), element), otherAggregator.tally(accumulator.getRight(), element));
            }

            @Override
            public Pair<U, W> combine(Pair<U, W> accu1, Pair<U, W> accu2) {
                return Pair.of(AggregatorOp.this.combine(accu1.getLeft(), accu2.getLeft()), otherAggregator.combine(accu1.getRight(), accu2.getRight()));
            }

            @Override
            public Pair<U, W> neutral() {
                return Pair.of(AggregatorOp.this.neutral(), otherAggregator.neutral());
            }

            @Override
            public Pair<V, X> summarize(Pair<U, W> acc) {
                return Pair.of(AggregatorOp.this.summarize(acc.getLeft()), otherAggregator.summarize(acc.getRight()));
            }
        };
    }

}
