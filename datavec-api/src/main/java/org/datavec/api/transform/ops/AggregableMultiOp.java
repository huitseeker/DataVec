package org.datavec.api.transform.ops;

import org.apache.commons.lang3.tuple.Pair;
import org.datavec.api.writable.ByteWritable;
import org.datavec.api.writable.Writable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by huitseeker on 5/8/17.
 */
public abstract class AggregableMultiOp<T, U> implements AggregableReduceOp<T, U, List<Writable>> {

        public static <X, Y, W extends Writable> AggregableMultiOp<X, Y> fromOp(final AggregableReduceOp<X, Y, W> op){
        return new AggregableMultiOp<X, Y>() {

            @Override
            public Y tally(Y accumulator, X element) {
                return op.tally(accumulator, element);
            }

            @Override
            public Y combine(Y accu1, Y accu2) {
                return op.combine(accu1, accu2);
            }

            @Override
            public Y neutral() {
                return op.neutral();
            }

            @Override
            public List<Writable> summarize(Y acc) {
                return Collections.singletonList((Writable) op.summarize(acc));
            }
        };
    }

    public <V, W extends AggregableReduceOp<T, V, List<Writable>>> AggregableMultiOp<T, Pair<U, V>> andThen(final W otherOp){
        return new AggregableMultiOp<T, Pair<U, V>>() {
            @Override
            public Pair<U, V> tally(Pair<U, V> accumulator, T element) {
                return Pair.of(AggregableMultiOp.this.tally(accumulator.getLeft(), element), otherOp.tally(accumulator.getRight(), element));
            }

            @Override
            public Pair<U, V> combine(Pair<U, V> accu1, Pair<U, V> accu2) {
                return Pair.of(AggregableMultiOp.this.combine(accu1.getLeft(), accu2.getLeft()), otherOp.combine(accu1.getRight(), accu2.getRight()));
            }

            @Override
            public Pair<U, V> neutral() {
                return Pair.of(AggregableMultiOp.this.neutral(), otherOp.neutral());
            }

            @Override
            public List<Writable> summarize(Pair<U, V> acc) {
                List<Writable> leftList = AggregableMultiOp.this.summarize(acc.getLeft());
                List<Writable> rightList = otherOp.summarize(acc.getRight());
                List<Writable> res = new ArrayList<>(leftList);
                res.addAll(rightList);
                return res;
            }
        };
    }

    public static <W, X, Y extends AggregableMultiOp<W, X>>  AggregableMultiOp<List<W>, List<X>> parallel(final List<Y> lOps){
        return new AggregableMultiOp<List<W>, List<X>>() {

            @Override
            public List<X> tally(List<X> accumulator, List<W> element) {
                List<X> res = new ArrayList<>(lOps.size());
                for(int i = 0; i < lOps.size(); i++){
                    X thisAcc = accumulator.get(i);
                    W thiselem = element.get(i);
                    res.add(lOps.get(i).tally(thisAcc, thiselem));
                }
                return res;
            }

            @Override
            public List<X> combine(List<X> accu1, List<X> accu2) {
                List <X> res = new ArrayList<>(lOps.size());
                for (int i = 0; i < lOps.size(); i++){
                    X leftAcc = accu1.get(i);
                    X rightAcc = accu2.get(i);
                    res.add(lOps.get(i).combine(leftAcc, rightAcc));
                }
                return res;
            }

            @Override
            public List<X> neutral() {
                List<X> res = new ArrayList<>(lOps.size());
                for (int i = 0; i < lOps.size(); i++){
                    res.add(lOps.get(i).neutral());
                }
                return res;
            }

            @Override
            public List<Writable> summarize(List<X> acc) {
                List<Writable> res = new ArrayList<>();
                for (int i = 0; i < lOps.size(); i++){
                    res.addAll(lOps.get(i).summarize(acc.get(i)));
                }
                return res;
            }
        };
    }


    public static <U> AggregableMultiOp<Writable, U> toLongWritable(final AggregableMultiOp<Long, U> aggregableMultiOp){
        return new AggregableMultiOp<Writable, U>() {
            @Override
            public U tally(U accumulator, Writable element) {
                return aggregableMultiOp.tally(accumulator, element.toLong());
            }

            @Override
            public U combine(U accu1, U accu2) {
                return aggregableMultiOp.combine(accu1, accu2);
            }

            @Override
            public U neutral() {
                return aggregableMultiOp.neutral();
            }

            @Override
            public List<Writable> summarize(U acc) {
                return aggregableMultiOp.summarize(acc);
            }
        };
    }

    public static <U> AggregableMultiOp<Writable, U> toIntWritable(final AggregableMultiOp<Integer, U> aggregableMultiOp){
        return new AggregableMultiOp<Writable, U>() {
            @Override
            public U tally(U accumulator, Writable element) {
                return aggregableMultiOp.tally(accumulator, element.toInt());
            }

            @Override
            public U combine(U accu1, U accu2) {
                return aggregableMultiOp.combine(accu1, accu2);
            }

            @Override
            public U neutral() {
                return aggregableMultiOp.neutral();
            }

            @Override
            public List<Writable> summarize(U acc) {
                return aggregableMultiOp.summarize(acc);
            }
        };
    }

    public static <U> AggregableMultiOp<Writable, U> toDoubleWritable(final AggregableMultiOp<Double, U> aggregableMultiOp){
        return new AggregableMultiOp<Writable, U>() {
            @Override
            public U tally(U accumulator, Writable element) {
                return aggregableMultiOp.tally(accumulator, element.toDouble());
            }

            @Override
            public U combine(U accu1, U accu2) {
                return aggregableMultiOp.combine(accu1, accu2);
            }

            @Override
            public U neutral() {
                return aggregableMultiOp.neutral();
            }

            @Override
            public List<Writable> summarize(U acc) {
                return aggregableMultiOp.summarize(acc);
            }
        };
    }

    public static <U> AggregableMultiOp<Writable, U> toFloatWritable(final AggregableMultiOp<Float, U> aggregableMultiOp){
        return new AggregableMultiOp<Writable, U>() {
            @Override
            public U tally(U accumulator, Writable element) {
                return aggregableMultiOp.tally(accumulator, element.toFloat());
            }

            @Override
            public U combine(U accu1, U accu2) {
                return aggregableMultiOp.combine(accu1, accu2);
            }

            @Override
            public U neutral() {
                return aggregableMultiOp.neutral();
            }

            @Override
            public List<Writable> summarize(U acc) {
                return aggregableMultiOp.summarize(acc);
            }
        };
    }


    public static <U> AggregableMultiOp<Writable, U> toStringWritable(final AggregableMultiOp<String, U> aggregableMultiOp){
        return new AggregableMultiOp<Writable, U>() {
            @Override
            public U tally(U accumulator, Writable element) {
                return aggregableMultiOp.tally(accumulator, element.toString());
            }

            @Override
            public U combine(U accu1, U accu2) {
                return aggregableMultiOp.combine(accu1, accu2);
            }

            @Override
            public U neutral() {
                return aggregableMultiOp.neutral();
            }

            @Override
            public List<Writable> summarize(U acc) {
                return aggregableMultiOp.summarize(acc);
            }
        };
    }

    public static <U> AggregableMultiOp<Writable, U> toByteWritable(final AggregableMultiOp<Byte, U> aggregableMultiOp){
        return new AggregableMultiOp<Writable, U>() {
            @Override
            public U tally(U accumulator, Writable element) {
                return aggregableMultiOp.tally(accumulator, ((ByteWritable)element).get());
            }

            @Override
            public U combine(U accu1, U accu2) {
                return aggregableMultiOp.combine(accu1, accu2);
            }

            @Override
            public U neutral() {
                return aggregableMultiOp.neutral();
            }

            @Override
            public List<Writable> summarize(U acc) {
                return aggregableMultiOp.summarize(acc);
            }
        };
    }
}
