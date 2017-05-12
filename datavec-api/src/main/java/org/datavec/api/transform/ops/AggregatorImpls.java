package org.datavec.api.transform.ops;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.datavec.api.writable.*;

import java.util.function.Function;

/**
 * Created by huitseeker on 4/28/17.
 */
public class AggregatorImpls {

    public static class AggregableFirst<T> extends AggregableReduceOp<T, T, T> {

        @Override
        public T tally(T accumulator, T element) {
            return accumulator == null ? element : accumulator;
        }

        @Override
        public T combine(T accu1, T accu2) {
            return accu1;
        }

        @Override
        public T neutral() {
            return null;
        }

        @Override
        public T summarize(T acc) {
            return acc;
        }
    }

    public static class AggregableLast<T> extends AggregableReduceOp<T, T, T> {

        @Override
        public T tally(T accumulator, T element) {
            return element;
        }

        @Override
        public T combine(T accu1, T accu2) {
            return accu2;
        }

        @Override
        public T neutral() {
            return null;
        }

        @Override
        public T summarize(T acc) {
            return acc;
        }
    }

    public static class AggregableSum<T extends Number> extends AggregableReduceOp<T, Double, Writable> {

        public Double tally(Double accumulator, T n) {
            return accumulator + n.doubleValue();
        }

        public Double combine(Double acc1, Double acc2) {
            return acc1 + acc2;
        }

        public Double neutral() {
            return 0D;
        }

        public Writable summarize(Double acc) {
            return new DoubleWritable(acc);
        }
    }

    public static class AggregableProd<T extends Number> extends AggregableReduceOp<T, Double, Writable> {

        public Double tally(Double accumulator, T n) {
            return accumulator * n.doubleValue();
        }

        public Double combine(Double acc1, Double acc2) {
            return acc1 * acc2;
        }

        public Double neutral() {
            return 1D;
        }

        public Writable summarize(Double acc) {
            return new DoubleWritable(acc);
        }
    }

    public static class AggregableCount<T> extends AggregableReduceOp<T, Long, Writable> {

        public Long tally(Long accumulator, T n) {
            return accumulator + 1;
        }

        public Long combine(Long acc1, Long acc2) {
            return acc1 + acc2;
        }

        public Long neutral() {
            return 0L;
        }

        public Writable summarize(Long acc) {
            return new LongWritable(acc);
        }
    }

    public static class AggregableMean<T extends Number> extends AggregableReduceOp<T, Pair<Long, Double>, Writable> {

        public Pair<Long, Double> tally(Pair<Long, Double> accumulator, T n) {

            Long count = accumulator.getLeft();
            Double mean = accumulator.getRight();

            // See Knuth TAOCP vol 2, 3rd edition, page 232
            if (count == 0) {
                return Pair.of(1L, n.doubleValue());
            } else {
                Long newCount = count + 1;
                Double newMean = mean + (n.doubleValue() - mean) / newCount;
                return Pair.of(newCount, newMean);
            }
        }

        public Pair<Long, Double> combine(Pair<Long, Double> acc1, Pair<Long, Double> acc2) {
            Long totalCount = acc1.getLeft() + acc2.getLeft();
            Double totalMean =
                    (acc1.getRight() * acc1.getLeft() + acc2.getRight() * acc1.getLeft()) / totalCount;
            return Pair.of(totalCount, totalMean);
        }

        public Pair<Long, Double> neutral() {
            return Pair.of(0L, 0D);
        }

        public Writable summarize(Pair<Long, Double> acc) {
            return new DoubleWritable(acc.getRight());
        }
    }

    public static class AggregableStdDev<T extends Number>
            extends AggregableReduceOp<T, Triple<Long, Double, Double>, Writable> {

        public Triple<Long, Double, Double> tally(Triple<Long, Double, Double> accumulator, T n) {

            Long count = accumulator.getLeft();
            Double mean = accumulator.getMiddle();
            Double s = accumulator.getRight();

            // See Knuth TAOCP vol 2, 3rd edition, page 232
            if (count == 0) {
                return Triple.of(1L, n.doubleValue(), 0D);
            } else {
                Long newCount = count + 1;
                Double newMean = mean + (n.doubleValue() - mean) / newCount;
                Double newS = s + (n.doubleValue() - mean) * (n.doubleValue() - newMean);
                return Triple.of(newCount, newMean, newS);
            }
        }

        public Triple<Long, Double, Double> combine(
                Triple<Long, Double, Double> acc1, Triple<Long, Double, Double> acc2) {
            Long totalCount = acc1.getLeft() + acc2.getLeft();
            Double totalMean =
                    (acc1.getMiddle() * acc1.getLeft() + acc2.getMiddle() * acc1.getLeft()) / totalCount;
            // the variance of the union is the sum of variances
            Double leftVariance = acc1.getRight() / (acc1.getLeft() - 1);
            Double rightvariance = acc2.getRight() / (acc2.getLeft() - 1);
            Double totalS = (leftVariance + rightvariance) * (totalCount - 1);
            return Triple.of(totalCount, totalMean, totalS);
        }

        public Triple<Long, Double, Double> neutral() {
            return Triple.of(0L, 0D, 0D);
        }

        public Writable summarize(Triple<Long, Double, Double> acc) {
            return new DoubleWritable(Math.sqrt(acc.getRight() / (acc.getLeft() - 1)));
        }
    }

    public static class AggregableFunction<T> extends AggregableReduceOp<T, T, T> {

        private DecoratedBiFunction<T, T, T> function;
        private T neutralElement;

        public AggregableFunction(DecoratedBiFunction<T, T, T> fun, T neut) {
            function = fun;
            neutralElement = neut;
        }

        public final T tally(T accumulator, T number) {
            return function.apply(accumulator, number);
        }

        public final T combine(T acc1, T acc2) {
            return function.apply(acc1, acc2);
        }

        public final T neutral() {
            return neutralElement;
        }

        public final T summarize(T accu) { return accu; }

    }

    public static final AggregableReduceOp<Long, Long, Long> minLong =
            new AggregableFunction<>(
                    new DecoratedBiFunction<Long, Long, Long>() {
                        @Override
                        public Long apply(Long x, Long y) {
                            return Math.min(x, y);
                        }
                    },
                    Long.MAX_VALUE);

    public static final AggregableReduceOp<Integer, Integer, Integer> minInt =
            new AggregableFunction<>(
                    new DecoratedBiFunction<Integer, Integer, Integer>() {
                        @Override
                        public Integer apply(Integer x, Integer y) {
                            return Math.min(x, y);
                        }
                    },
                    Integer.MAX_VALUE);

    public static final AggregableReduceOp<Double, Double, Double> minDouble =
            new AggregableFunction<>(
                    new DecoratedBiFunction<Double, Double, Double>() {
                        @Override
                        public Double apply(Double x, Double y) {
                            return Math.min(x, y);
                        }
                    }, Double.MAX_VALUE);

    public static final AggregableReduceOp<Float, Float, Float> minFloat =
            new AggregableFunction<>(
                    new DecoratedBiFunction<Float, Float, Float>() {
                        @Override
                        public Float apply(Float x, Float y) {
                            return Math.min(x, y);
                        }
                    },
                    Float.MAX_VALUE);

    public static final AggregableReduceOp<Long, Long, Long> maxLong =
            new AggregableFunction<>(
                    new DecoratedBiFunction<Long, Long, Long>() {
                        @Override
                        public Long apply(Long x, Long y) {
                            return Math.max(x, y);
                        }
                    }, Long.MIN_VALUE);

    public static final AggregableReduceOp<Integer, Integer, Integer> maxInt =
            new AggregableFunction<>(
                    new DecoratedBiFunction<Integer, Integer, Integer>() {
                        @Override
                        public Integer apply(Integer x, Integer y) {
                            return Math.min(x, y);
                        }
                    },
                    Integer.MIN_VALUE);

    public static final AggregableReduceOp<Double, Double, Double> maxDouble =
            new AggregableFunction<>(
                    new DecoratedBiFunction<Double, Double, Double>() {
                        @Override
                        public Double apply(Double x, Double y) {
                            return Math.min(x, y);
                        }
                    },
                    Double.MIN_VALUE);

    public static final AggregableReduceOp<Float, Float, Float> maxFloat =
            new AggregableFunction<>(
                    new DecoratedBiFunction<Float, Float, Float>() {
                        @Override
                        public Float apply(Float x, Float y) {
                            return Math.min(x, y);
                        }
                    },
                    Float.MIN_VALUE);

    public static final AggregableReduceOp<Long, Pair<Long, Long>, Long> rangeLong =
            AggregableReduceOp.compose(minLong, maxLong).andFinally(new Function<Pair<Long, Long>, Long>() {
                @Override
                public Long apply(Pair<Long, Long> longLongPair) {
                    return longLongPair.getRight() - longLongPair.getLeft();
                }
            });

    public static final AggregableReduceOp<Integer, Pair<Integer, Integer>, Integer> rangeInt =
            AggregableReduceOp.compose(minInt, maxInt).andFinally(new Function<Pair<Integer, Integer>, Integer>() {
                @Override
                public Integer apply(Pair<Integer, Integer> intIntPair) {
                    return intIntPair.getRight() - intIntPair.getLeft();
                }
            });

    public static final AggregableReduceOp<Double, Pair<Double, Double>, Double> rangeDouble =
            AggregableReduceOp.compose(minDouble, maxDouble).andFinally(new Function<Pair<Double, Double>, Double>() {
                @Override
                public Double apply(Pair<Double, Double> doubleDoublePair) {
                    return doubleDoublePair.getRight() - doubleDoublePair.getLeft();
                }
            });

    public static final AggregableReduceOp<Float, Pair<Float, Float>, Float> rangeFloat =
            AggregableReduceOp.compose(minFloat, maxFloat).andFinally(new Function<Pair<Float, Float>, Float>() {
                @Override
                public Float apply(Pair<Float, Float> floatFloatPair) {
                    return floatFloatPair.getRight() - floatFloatPair.getLeft();
                }
            });

    public static class AggregableCountUnique<T> extends AggregableReduceOp<T, HyperLogLogPlus, Writable> {

        @Override
        public HyperLogLogPlus tally(HyperLogLogPlus accumulator, T element) {
            accumulator.offer(element);
            return accumulator;
        }

        @Override
        public HyperLogLogPlus combine(HyperLogLogPlus accu1, HyperLogLogPlus accu2) {

            try {
                accu1.addAll(accu2);
            } catch (CardinalityMergeException e) {
                throw new RuntimeException(e);
            }
            return accu1;
        }

        @Override
        public HyperLogLogPlus neutral() {
        /*
         * This is based on streamlib's implementation of "HyperLogLog in Practice:
         * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
         * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
         *
         * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting
         * a nonzero `sp > p` in HyperLogLogPlus(p, sp) would trigger sparse
         * representation of registers, which may reduce the memory consumption
         * and increase accuracy when the cardinality is small.
         */
            Float p = 0.05F;
            return new HyperLogLogPlus((int) Math.ceil(2.0 * Math.log(1.054 / p) / Math.log(2)), 0);
        }

        @Override
        public Writable summarize(HyperLogLogPlus acc) {
            return new LongWritable(acc.cardinality());
        }
    }
}
