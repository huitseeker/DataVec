package org.datavec.api.transform.ops;

import scala.Function2;
import scala.Tuple2;
import scala.Tuple3;

import java.util.function.Function;

/**
 * Created by huitseeker on 4/28/17.
 */
public class AggregatebleReduceImpls {

    public static class AggregatableSum implements AggregatebleReduceOp<Number, Double, Double> {

        public Double add(Double accumulator,Number n){ return accumulator + n.doubleValue(); }
        public Double combine(Double acc1, Double acc2){ return acc1 + acc2; }
        public Double neutral(){ return 0D; }
        public Double finalAggregate(Double acc){ return acc; }
    }

    public static class AggregatableProd implements AggregatebleReduceOp<Number, Double, Double> {

        public Double add(Double accumulator,Number n){ return accumulator * n.doubleValue(); }
        public Double combine(Double acc1, Double acc2){ return acc1 * acc2; }
        public Double neutral(){ return 1D; }
        public Double finalAggregate(Double acc){ return acc; }
    }

    public static class AggregatableCount<T> implements AggregatebleReduceOp<T, Long, Long> {

        public Long add(Long accumulator,T n){ return accumulator + 1; }
        public Long combine(Long acc1, Long acc2){ return acc1 + acc2; }
        public Long neutral(){ return 0L; }
        public Long finalAggregate(Long acc){ return acc; }
    }

    public static class AggregatableMean implements AggregatebleReduceOp<Number, Tuple2<Long, Double>, Double> {


        public Tuple2<Long, Double> add(Tuple2<Long, Double> accumulator, Number n){

            Long count = accumulator._1();
            Double mean = accumulator._2();

            // See Knuth TAOCP vol 2, 3rd edition, page 232
            if (count == 0)
            {
                return new Tuple2<>(1L, n.doubleValue());
            }
            else
            {
                Long newCount = count + 1;
                Double newMean = mean + (n.doubleValue() - mean)/newCount;
                return new Tuple2<>(newCount; newMean);
            }
        }
        public Tuple2<Long, Double> combine(Tuple2<Long, Double> acc1, Tuple2<Long, Double> acc2){
            Long totalCount = acc1._1() + acc2._1();
            Double totalMean = (acc1._2() * acc1._1() + acc2._2() * acc1._1()) / totalCount;
            return new Tuple2<>(totalCount, totalMean);
        }
        public Tuple2<Long, Double> neutral(){ return new Tuple2<>(0L, 0D); }
        public Double finalAggregate(Tuple2<Long, Double> acc){ return acc._2(); }
    }

    public static class AggregatableStdDev implements AggregatebleReduceOp<Number, Tuple3<Long, Double, Double>, Double> {

        public Tuple3<Long, Double, Double> add(Tuple3<Long, Double, Double> accumulator, Number n){

            Long count = accumulator._1();
            Double mean = accumulator._2();
            Double s = accumulator._3();

            // See Knuth TAOCP vol 2, 3rd edition, page 232
            if (count == 0)
            {
                return new Tuple3<>(1L, n.doubleValue(), 0D);
            }
            else
            {
                Long newCount = count + 1;
                Double newMean = mean + (n.doubleValue() - mean)/newCount;
                Double newS = s + (n.doubleValue() - mean)*(n.doubleValue() - newMean);
                return new Tuple3<>(newCount, newMean, newS);
            }
        }
        public Tuple3<Long, Double, Double> combine(Tuple3<Long, Double, Double> acc1, Tuple3<Long, Double, Double> acc2){
            Long totalCount = acc1._1() + acc2._1();
            Double totalMean = (acc1._2() * acc1._1() + acc2._2() * acc1._1()) / totalCount;
            // the variance of the union is the sum of variances
            Double leftVariance = acc1._3()/(acc1._1() - 1);
            Double rightvariance = acc2._3()/(acc2._1() - 1);
            Double totalS = (leftVariance + rightvariance) * (totalCount -1);
            return new Tuple3<>(totalCount, totalMean, totalS);
        }
        public Tuple3<Long, Double, Double> neutral(){ return new Tuple3<>(0L, 0D, 0D); }
        public Double finalAggregate(Tuple3<Long, Double, Double> acc){ return Math.sqrt(acc._3()/(acc._1() - 1)); }
    }


    public class AggregatableFunction<T> implements AggregatebleReduceOp<T, T, T> {

        private Function2<T, T, T> function;
        private T neutralElement;

        public AggregatableFunction(Function2<T,T, T> fun, T neut){
            function = fun;
            neutralElement = neut;
        }

        public final T add(T accumulator, T number){ return function.apply(accumulator, number); }
        public final T combine(T acc1, T acc2){ return function.apply(acc1, acc2); }
        public final T neutral() { return neutralElement; }
        public final T finalAggregate(T accu) { return accu; }
    }

    public final AggregatebleReduceOp<Long, Long, Long> minLong = new AggregatableFunction<Long>(new Function2<Long, Long, Long>(){
        @Override
        public Long apply(Long x, Long y) { return Math.min(x, y);}
    }, Long.MAX_VALUE);

    public final AggregatebleReduceOp<Integer, Integer, Integer> minInt = new AggregatableFunction<Integer>(new Function2<Integer, Integer, Integer>(){
        @Override
        public Integer apply(Integer x, Integer y) { return Math.min(x, y);}
    }, Integer.MAX_VALUE);

    public final AggregatebleReduceOp<Double, Double, Double> minDouble = new AggregatableFunction<Double>(new Function2<Double, Double, Double>(){
        @Override
        public Double apply(Double x, Double y) { return Math.min(x, y);}
    }, Double.MAX_VALUE);

    public final AggregatebleReduceOp<Float, Float, Float> minFloat = new AggregatableFunction<Float>(new Function2<Float, Float, Float>(){
        @Override
        public Float apply(Float x, Float y) { return Math.min(x, y);}
    }, Float.MAX_VALUE);



}
