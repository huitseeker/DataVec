package org.datavec.api.transform.ops;

import org.apache.commons.lang3.tuple.Pair;

import java.util.function.Function;

/**
 * Created by huitseeker on 5/1/17.
 */
public abstract class AggregableReduceOp<T, U, V> implements IAggregableReduceOp<T, U, V> {

    public abstract U tally(U accumulator, T element);

    public abstract U combine(U accu1, U accu2);

    public abstract U neutral();

    public abstract V summarize(U acc);

    public static <T, U, V, W, X> AggregableReduceOp<T, Pair<U, W>, Pair<V, X>> compose(final AggregableReduceOp<T, U, V> thisAggregator, final AggregableReduceOp<T, W, X> otherAggregator) {
        return new AggregableReduceOp<T, Pair<U, W>, Pair<V, X>>() {
            @Override
            public Pair<U, W> tally(Pair<U, W> accumulator, T element) {
                return Pair.of(thisAggregator.tally(accumulator.getLeft(), element), otherAggregator.tally(accumulator.getRight(), element));
            }

            @Override
            public Pair<U, W> combine(Pair<U, W> accu1, Pair<U, W> accu2) {
                return Pair.of(thisAggregator.combine(accu1.getLeft(), accu2.getLeft()), otherAggregator.combine(accu1.getRight(), accu2.getRight()));
            }

            @Override
            public Pair<U, W> neutral() {
                return Pair.of(thisAggregator.neutral(), otherAggregator.neutral());
            }

            @Override
            public Pair<V, X> summarize(Pair<U, W> acc) {
                return Pair.of(thisAggregator.summarize(acc.getLeft()), otherAggregator.summarize(acc.getRight()));
            }
        };
    }


    public <W> AggregableReduceOp<T, U, W> andFinally(final Function<V, W> finalize) {
        return new AggregableReduceOp<T, U, W>(){

            @Override
            public U tally(U accumulator, T element) {
                return AggregableReduceOp.this.tally(accumulator, element);
            }

            @Override
            public U combine(U accu1, U accu2) {
                return AggregableReduceOp.this.combine(accu1, accu2);
            }

            @Override
            public U neutral() {
                return AggregableReduceOp.this.neutral();
            }

            @Override
            public W summarize(U acc) {
                return finalize.apply(AggregableReduceOp.this.summarize(acc));
            }
        };
    }

}
