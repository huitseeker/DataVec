/*-
 *  * Copyright 2016 Skymind, Inc.
 *  *
 *  *    Licensed under the Apache License, Version 2.0 (the "License");
 *  *    you may not use this file except in compliance with the License.
 *  *    You may obtain a copy of the License at
 *  *
 *  *        http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *    Unless required by applicable law or agreed to in writing, software
 *  *    distributed under the License is distributed on an "AS IS" BASIS,
 *  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *    See the License for the specific language governing permissions and
 *  *    limitations under the License.
 */

package org.datavec.api.transform.reduce;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.sun.corba.se.spi.ior.Writeable;
import com.sun.java.swing.plaf.windows.WindowsTableHeaderUI;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.datavec.api.transform.ColumnType;
import org.datavec.api.transform.ReduceOp;
import org.datavec.api.transform.condition.Condition;
import org.datavec.api.transform.metadata.ColumnMetaData;
import org.datavec.api.transform.metadata.DoubleMetaData;
import org.datavec.api.transform.metadata.IntegerMetaData;
import org.datavec.api.transform.metadata.LongMetaData;
import org.datavec.api.transform.ops.AggregableMultiOp;
import org.datavec.api.transform.ops.AggregableReduceOp;
import org.datavec.api.transform.ops.AggregatorImpls;
import org.datavec.api.transform.ops.AggregatorOp;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.writable.*;
import org.nd4j.shade.jackson.annotation.JsonIgnoreProperties;
import org.nd4j.shade.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;

/**
 * A StringReducer is used to take a set of examples and reduce them.
 * The idea: suppose you have a large number of columns, and you want to combine/reduce the values in each column.<br>
 * StringReducer allows you to specify different reductions for differently for different columns: min, max, sum, mean etc.
 * See {@link Builder} and {@link ReduceOp} for the full list.<br>
 * <p>
 * Uses are:
 * (1) Reducing examples by a key
 * (2) Reduction operations in time series (windowing ops, etc)
 *
 * @author Alex Black
 */
@Data
@JsonIgnoreProperties({"schema", "keyColumnsSet"})
@EqualsAndHashCode(exclude = {"schema", "keyColumnsSet"})
public class MultiOpReducer implements IAssociativeReducer {

    private Schema schema;
    private final List<String> keyColumns;
    private final Set<String> keyColumnsSet;
    private final ReduceOp defaultOp;
    private final Map<String, List<ReduceOp>> opMap;
    private Map<String, AggregableColumnReduction> customReductions;

    private Set<String> ignoreInvalidInColumns;

    private MultiOpReducer(Builder builder) {
        this((builder.keyColumns == null ? null : Arrays.asList(builder.keyColumns)), builder.defaultOp, builder.opMap,
                        builder.customReductions, builder.ignoreInvalidInColumns);
    }

    public MultiOpReducer(@JsonProperty("keyColumns") List<String> keyColumns, @JsonProperty("defaultOp") ReduceOp defaultOp,
                          @JsonProperty("opMap") Map<String, List<ReduceOp>> opMap,
                          @JsonProperty("customReductions") Map<String, AggregableColumnReduction> customReductions,
                          @JsonProperty("ignoreInvalidInColumns") Set<String> ignoreInvalidInColumns) {
        this.keyColumns = keyColumns;
        this.keyColumnsSet = (keyColumns == null ? null : new HashSet<>(keyColumns));
        this.defaultOp = defaultOp;
        this.opMap = opMap;
        this.customReductions = customReductions;
        this.ignoreInvalidInColumns = ignoreInvalidInColumns;
    }

    @Override
    public void setInputSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Schema getInputSchema() {
        return schema;
    }

    @Override
    public List<String> getKeyColumns() {
        return keyColumns;
    }

    /**
     * Get the output schema, given the input schema
     */
    @Override
    public Schema transform(Schema schema) {
        int nCols = schema.numColumns();
        List<String> colNames = schema.getColumnNames();
        List<ColumnMetaData> meta = schema.getColumnMetaData();
        List<ColumnMetaData> newMeta = new ArrayList<>(nCols);

        for (int i = 0; i < nCols; i++) {
            String name = colNames.get(i);
            ColumnMetaData inMeta = meta.get(i);

            if (keyColumnsSet != null && keyColumnsSet.contains(name)) {
                //No change to key columns
                newMeta.add(inMeta);
                continue;
            }

            //First: check for a custom reductions on this column
            if (customReductions != null && customReductions.containsKey(name)) {
                AggregableColumnReduction reduction = customReductions.get(name);

                List<String> outName = reduction.getColumnsOutputName(name);
                List<ColumnMetaData> outMeta = reduction.getColumnOutputMetaData(outName, inMeta);

                newMeta.addAll(outMeta);

                continue;
            }

            //Otherwise: get the specified (built-in) reduction op
            //If no reduction op is specified for that column: use the default
            List<ReduceOp> lop = opMap.get(name);
            if (lop != null & !lop.isEmpty())
                for (i = 0; i < lop.size(); i++){
                    newMeta.add(getMetaForColumn(lop.get(i), name, inMeta));
                }
        }

        return schema.newSchema(newMeta);
    }

    private static ColumnMetaData getMetaForColumn(ReduceOp op, String name, ColumnMetaData inMeta) {
        inMeta = inMeta.clone();
        switch (op) {
            case Min:
                inMeta.setName("min(" + name + ")");
                return inMeta;
            case Max:
                inMeta.setName("max(" + name + ")");
                return inMeta;
            case Range:
                inMeta.setName("range(" + name + ")");
                return inMeta;
            case TakeFirst:
                inMeta.setName("first(" + name + ")");
                return inMeta;
            case TakeLast:
                inMeta.setName("last(" + name + ")");
                return inMeta;
            case Sum:
                String outName = "sum(" + name + ")";
                //Issue with sum: the input meta data restrictions probably won't hold. But the data _type_ should essentially remain the same
                ColumnMetaData outMeta;
                if (inMeta instanceof IntegerMetaData || inMeta instanceof LongMetaData) {
                    outMeta = new LongMetaData(outName);
                } else if (inMeta instanceof DoubleMetaData) {
                    outMeta = new DoubleMetaData(outName);
                } else {
                    //Sum doesn't really make sense to sum other column types anyway...
                    outMeta = inMeta;
                }
                outMeta.setName(outName);
                return outMeta;
            case Mean:
                return new DoubleMetaData("mean(" + name + ")");
            case Stdev:
                return new DoubleMetaData("stdev(" + name + ")");
            case Count:
                return new IntegerMetaData("count", 0, null);
            case CountUnique:
                //Always integer
                return new IntegerMetaData("countUnique(" + name + ")", 0, null);
            default:
                throw new UnsupportedOperationException("Unknown or not implemented op: " + op);
        }
    }

    @Override
    public AggregableMultiOp<List<Writable>, List<?>> aggregableReduce() {
        //Go through each writable, and reduce according to whatever strategy is specified

        if (schema == null)
            throw new IllegalStateException("Error: Schema has not been set");

        int nCols = schema.numColumns();
        List<String> colNames = schema.getColumnNames();

        List<AggregableMultiOp<Writable, ?>> ops = new ArrayList<>(nCols);

        for (int i = 0; i < nCols; i++) {
            String colName = colNames.get(i);
            if (keyColumnsSet != null && keyColumnsSet.contains(colName)) {
                ops.add(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableFirst()));
                continue;
            }


            // is this a *custom* reduction column?
            if (customReductions != null && customReductions.containsKey(colName)) {
                AggregableColumnReduction reduction = customReductions.get(colName);
                ops.add(reduction.reduceOp());
            }


            //What type of column is this?
            ColumnType type = schema.getType(i);

            //What ops are we performing on this column?
            List<ReduceOp> lop = opMap.get(colName);
            if (lop == null || lop.isEmpty())
                lop = Collections.singletonList(defaultOp);

            //Execute the reduction, store the result
            ops.add(reduceColumn(lop, type));

        }

        return AggregableMultiOp.parallel(ops);
    }

    public static AggregableMultiOp<Writable, ?> reduceColumn(List<ReduceOp> op, ColumnType type) {
        switch (type) {
            case Integer:
                return reduceIntColumn(op);
            case Long:
                return reduceLongColumn(op);
            case Float:
                return reduceFloatColumn(op);
            case Double:
                return reduceDoubleColumn(op);
            case String:
            case Categorical:
                return reduceStringOrCategoricalColumn(op);
            case Time:
                return reduceTimeColumn(op);
            case Bytes:
                return reduceBytesColumn(op);
            default:
                throw new UnsupportedOperationException("Unknown or not implemented column type: " + type);
        }
    }

    public static AggregableMultiOp<Writable, ?> reduceIntColumn(List<ReduceOp> lop) {

        Function<Integer, Writable> toWritable = new Function<Integer, Writable>(){

            @Override
            public Writable apply(Integer anInt) {
                return new IntWritable(anInt);
            }
        };

        AggregableMultiOp<Writable, ?> res = null;
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case Min:
                    AggregableMultiOp<Writable, Integer> minOp = AggregableMultiOp.toIntWritable(AggregableMultiOp.fromOp(AggregatorImpls.minInt.andFinally(toWritable)));
                    if (res == null)
                        res = minOp;
                    else {
                        res = res.andThen(minOp);
                    }
                case Max:
                    AggregableMultiOp<Writable, Integer> maxOp = AggregableMultiOp.toIntWritable(AggregableMultiOp.fromOp(AggregatorImpls.maxInt.andFinally(toWritable)));
                    if (res == null)
                        res = maxOp;
                    else {
                        res = res.andThen(maxOp);
                    }
                case Range:
                    AggregableMultiOp<Writable, Pair<Integer, Integer>> rangeOp = AggregableMultiOp.toIntWritable(AggregableMultiOp.fromOp(AggregatorImpls.rangeInt.andFinally(toWritable)));
                    if (res == null)
                        res = rangeOp;
                    else {
                        res = res.andThen(rangeOp);
                    }
                case Sum:
                    AggregableMultiOp<Writable, Double> sumOp = AggregableMultiOp.toIntWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableSum()));
                    if (res == null)
                        res = sumOp;
                    else {
                        res = res.andThen(sumOp);
                    }
                case Mean:
                    AggregableMultiOp<Writable, Pair<Integer, Double>> meanOp = AggregableMultiOp.toIntWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableMean()));
                    if (res == null)
                        res = meanOp;
                    else {
                        res = res.andThen(meanOp);
                    }
                case Stdev:
                    AggregableMultiOp<Writable, Triple<Integer, Double, Double>> stdDevOp = AggregableMultiOp.toIntWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableStdDev()));
                    if (res == null)
                        res = stdDevOp;
                    else {
                        res = res.andThen(stdDevOp);
                    }
                case Count:
                    AggregableMultiOp<Writable, Long> countOp = AggregableMultiOp.toIntWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableCount<Integer>()));
                    if (res == null)
                        res = countOp;
                    else {
                        res = res.andThen(countOp);
                    }
                case CountUnique:
                    AggregableMultiOp<Writable, HyperLogLogPlus> countDistinctOp = AggregableMultiOp.toIntWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableCountUnique<Integer>()));
                    if (res == null)
                        res = countDistinctOp;
                    else {
                        res = res.andThen(countDistinctOp);
                    }
                case TakeFirst:
                    AggregableMultiOp<Writable, Integer> takeFirstOp = AggregableMultiOp.toIntWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableFirst<Integer>()).andFinally(toWritable)));
                    if (res == null)
                        res = takeFirstOp;
                    else {
                        res = res.andThen(takeFirstOp);
                    }
                case TakeLast:
                    AggregableMultiOp<Writable, Integer> takeLastOp = AggregableMultiOp.toIntWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableLast<Integer>()).andFinally(toWritable)));
                    if (res == null)
                        res = takeLastOp;
                    else {
                        res = res.andThen(takeLastOp);
                    }
                default:
                    throw new UnsupportedOperationException("Unknown or not implement op: " + lop.get(i));
            }
        }
        return res;
    }

    public static AggregableMultiOp<Writable, ?> reduceLongColumn(List<ReduceOp> lop) {

        Function<Long, Writable> toWritable = new Function<Long, Writable>(){

            @Override
            public Writable apply(Long aLong) {
                return new LongWritable(aLong);
            }
        };

        AggregableMultiOp<Writable, ?> res = null;
        for (int i = 0; i < lop.size(); i++){
        switch (lop.get(i)) {
            case Min:
                AggregableMultiOp<Writable, Long> minOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(AggregatorImpls.minLong.andFinally(toWritable)));
                if (res == null)
                    res = minOp;
                else {
                    res = res.andThen(minOp);
                }
            case Max:
                AggregableMultiOp<Writable, Long> maxOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(AggregatorImpls.maxLong.andFinally(toWritable)));
                if (res == null)
                    res = maxOp;
                else {
                    res = res.andThen(maxOp);
                }
            case Range:
                AggregableMultiOp<Writable, Pair<Long, Long>> rangeOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(AggregatorImpls.rangeLong.andFinally(toWritable)));
                if (res == null)
                    res = rangeOp;
                else {
                    res = res.andThen(rangeOp);
                }
            case Sum:
                AggregableMultiOp<Writable, Double> sumOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableSum()));
                if (res == null)
                    res = sumOp;
                else {
                    res = res.andThen(sumOp);
                }
            case Mean:
                AggregableMultiOp<Writable, Pair<Long, Double>> meanOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableMean()));
                if (res == null)
                    res = meanOp;
                else {
                    res = res.andThen(meanOp);
                }
            case Stdev:
                AggregableMultiOp<Writable, Triple<Long, Double, Double>> stdDevOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableStdDev()));
                if (res == null)
                    res = stdDevOp;
                else {
                    res = res.andThen(stdDevOp);
                }
            case Count:
                AggregableMultiOp<Writable, Long> countOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableCount<Long>()));
                if (res == null)
                    res = countOp;
                else {
                    res = res.andThen(countOp);
                }
            case CountUnique:
                AggregableMultiOp<Writable, HyperLogLogPlus> countDistinctOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableCountUnique<Long>()));
                if (res == null)
                    res = countDistinctOp;
                else {
                    res = res.andThen(countDistinctOp);
                }
            case TakeFirst:
                AggregableMultiOp<Writable, Long> takeFirstOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableFirst<Long>()).andFinally(toWritable)));
                if (res == null)
                    res = takeFirstOp;
                else {
                    res = res.andThen(takeFirstOp);
                }
            case TakeLast:
                AggregableMultiOp<Writable, Long> takeLastOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableLast<Long>()).andFinally(toWritable)));
                if (res == null)
                    res = takeLastOp;
                else {
                    res = res.andThen(takeLastOp);
                }
            default:
                throw new UnsupportedOperationException("Unknown or not implement op: " + lop.get(i));
        }
        }
        return res;
    }

    public static AggregableMultiOp<Writable, ?> reduceDoubleColumn(List<ReduceOp> lop) {

        Function<Double, Writable> toWritable = new Function<Double, Writable>(){

            @Override
            public Writable apply(Double aDouble) {
                return new DoubleWritable(aDouble);
            }
        };

        AggregableMultiOp<Writable, ?> res = null;
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case Min:
                    AggregableMultiOp<Writable, Double> minOp = AggregableMultiOp.toDoubleWritable(AggregableMultiOp.fromOp(AggregatorImpls.minDouble.andFinally(toWritable)));
                    if (res == null)
                        res = minOp;
                    else {
                        res = res.andThen(minOp);
                    }
                case Max:
                    AggregableMultiOp<Writable, Double> maxOp = AggregableMultiOp.toDoubleWritable(AggregableMultiOp.fromOp(AggregatorImpls.maxDouble.andFinally(toWritable)));
                    if (res == null)
                        res = maxOp;
                    else {
                        res = res.andThen(maxOp);
                    }
                case Range:
                    AggregableMultiOp<Writable, Pair<Double, Double>> rangeOp = AggregableMultiOp.toDoubleWritable(AggregableMultiOp.fromOp(AggregatorImpls.rangeDouble.andFinally(toWritable)));
                    if (res == null)
                        res = rangeOp;
                    else {
                        res = res.andThen(rangeOp);
                    }
                case Sum:
                    AggregableMultiOp<Writable, Double> sumOp = AggregableMultiOp.toDoubleWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableSum()));
                    if (res == null)
                        res = sumOp;
                    else {
                        res = res.andThen(sumOp);
                    }
                case Mean:
                    AggregableMultiOp<Writable, Pair<Double, Double>> meanOp = AggregableMultiOp.toDoubleWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableMean()));
                    if (res == null)
                        res = meanOp;
                    else {
                        res = res.andThen(meanOp);
                    }
                case Stdev:
                    AggregableMultiOp<Writable, Triple<Double, Double, Double>> stdDevOp = AggregableMultiOp.toDoubleWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableStdDev()));
                    if (res == null)
                        res = stdDevOp;
                    else {
                        res = res.andThen(stdDevOp);
                    }
                case Count:
                    AggregableMultiOp<Writable, Long> countOp = AggregableMultiOp.toDoubleWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableCount<Double>()));
                    if (res == null)
                        res = countOp;
                    else {
                        res = res.andThen(countOp);
                    }
                case CountUnique:
                    AggregableMultiOp<Writable, HyperLogLogPlus> countDistinctOp = AggregableMultiOp.toDoubleWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableCountUnique<Double>()));
                    if (res == null)
                        res = countDistinctOp;
                    else {
                        res = res.andThen(countDistinctOp);
                    }
                case TakeFirst:
                    AggregableMultiOp<Writable, Double> takeFirstOp = AggregableMultiOp.toDoubleWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableFirst<Double>()).andFinally(toWritable)));
                    if (res == null)
                        res = takeFirstOp;
                    else {
                        res = res.andThen(takeFirstOp);
                    }
                case TakeLast:
                    AggregableMultiOp<Writable, Double> takeLastOp = AggregableMultiOp.toDoubleWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableLast<Double>()).andFinally(toWritable)));
                    if (res == null)
                        res = takeLastOp;
                    else {
                        res = res.andThen(takeLastOp);
                    }
                default:
                    throw new UnsupportedOperationException("Unknown or not implement op: " + lop.get(i));
            }
        }
        return res;
    }

    public static AggregableMultiOp<Writable, ?> reduceFloatColumn(List<ReduceOp> lop) {

        Function<Float, Writable> toWritable = new Function<Float, Writable>(){

            @Override
            public Writable apply(Float aFloat) {
                return new FloatWritable(aFloat);
            }
        };

        AggregableMultiOp<Writable, ?> res = null;
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case Min:
                    AggregableMultiOp<Writable, Float> minOp = AggregableMultiOp.toFloatWritable(AggregableMultiOp.fromOp(AggregatorImpls.minFloat.andFinally(toWritable)));
                    if (res == null)
                        res = minOp;
                    else {
                        res = res.andThen(minOp);
                    }
                case Max:
                    AggregableMultiOp<Writable, Float> maxOp = AggregableMultiOp.toFloatWritable(AggregableMultiOp.fromOp(AggregatorImpls.maxFloat.andFinally(toWritable)));
                    if (res == null)
                        res = maxOp;
                    else {
                        res = res.andThen(maxOp);
                    }
                case Range:
                    AggregableMultiOp<Writable, Pair<Float, Float>> rangeOp = AggregableMultiOp.toFloatWritable(AggregableMultiOp.fromOp(AggregatorImpls.rangeFloat.andFinally(toWritable)));
                    if (res == null)
                        res = rangeOp;
                    else {
                        res = res.andThen(rangeOp);
                    }
                case Sum:
                    AggregableMultiOp<Writable, Float> sumOp = AggregableMultiOp.toFloatWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableSum()));
                    if (res == null)
                        res = sumOp;
                    else {
                        res = res.andThen(sumOp);
                    }
                case Mean:
                    AggregableMultiOp<Writable, Pair<Float, Float>> meanOp = AggregableMultiOp.toFloatWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableMean()));
                    if (res == null)
                        res = meanOp;
                    else {
                        res = res.andThen(meanOp);
                    }
                case Stdev:
                    AggregableMultiOp<Writable, Triple<Float, Float, Float>> stdDevOp = AggregableMultiOp.toFloatWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableStdDev()));
                    if (res == null)
                        res = stdDevOp;
                    else {
                        res = res.andThen(stdDevOp);
                    }
                case Count:
                    AggregableMultiOp<Writable, Long> countOp = AggregableMultiOp.toFloatWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableCount<Float>()));
                    if (res == null)
                        res = countOp;
                    else {
                        res = res.andThen(countOp);
                    }
                case CountUnique:
                    AggregableMultiOp<Writable, HyperLogLogPlus> countDistinctOp = AggregableMultiOp.toFloatWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableCountUnique<Float>()));
                    if (res == null)
                        res = countDistinctOp;
                    else {
                        res = res.andThen(countDistinctOp);
                    }
                case TakeFirst:
                    AggregableMultiOp<Writable, Float> takeFirstOp = AggregableMultiOp.toFloatWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableFirst<Float>()).andFinally(toWritable)));
                    if (res == null)
                        res = takeFirstOp;
                    else {
                        res = res.andThen(takeFirstOp);
                    }
                case TakeLast:
                    AggregableMultiOp<Writable, Float> takeLastOp = AggregableMultiOp.toFloatWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableLast<Float>()).andFinally(toWritable)));
                    if (res == null)
                        res = takeLastOp;
                    else {
                        res = res.andThen(takeLastOp);
                    }
                default:
                    throw new UnsupportedOperationException("Unknown or not implement op: " + lop.get(i));
            }
        }
        return res;
    }


    public static AggregableMultiOp<Writable, ?> reduceStringOrCategoricalColumn(List<ReduceOp> lop) {

        Function<String, Writable> toWritable = new Function<String, Writable>(){

            @Override
            public Writable apply(String aString) {
                return new Text(aString);
            }
        };

        AggregableMultiOp<Writable, ?> res = null;
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case Count:
                    AggregableMultiOp<Writable, Long> countOp = AggregableMultiOp.toStringWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableCount<String>()));
                    if (res == null)
                        res = countOp;
                    else {
                        res = res.andThen(countOp);
                    }
                case CountUnique:
                    AggregableMultiOp<Writable, HyperLogLogPlus> countDistinctOp = AggregableMultiOp.toStringWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableCountUnique<String>()));
                    if (res == null)
                        res = countDistinctOp;
                    else {
                        res = res.andThen(countDistinctOp);
                    }
                case TakeFirst:
                    AggregableMultiOp<Writable, String> takeFirstOp = AggregableMultiOp.toStringWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableFirst<String>()).andFinally(toWritable)));
                    if (res == null)
                        res = takeFirstOp;
                    else {
                        res = res.andThen(takeFirstOp);
                    }
                case TakeLast:
                    AggregableMultiOp<Writable, String> takeLastOp = AggregableMultiOp.toStringWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableLast<String>()).andFinally(toWritable)));
                    if (res == null)
                        res = takeLastOp;
                    else {
                        res = res.andThen(takeLastOp);
                    }
                default:
                    throw new UnsupportedOperationException("Cannot execute op \"" + lop.get(i) + "\" on String/Categorical column "
                            + "(can only perform Count, CountUnique, TakeFirst and TakeLast ops on categorical columns)");
            }
        }
        return res;
    }

    public static AggregableMultiOp<Writable, ?> reduceTimeColumn(List<ReduceOp> lop) {

        Function<Long, Writable> toWritable = new Function<Long, Writable>(){

            @Override
            public Writable apply(Long aLong) {
                return new LongWritable(aLong);
            }
        };

        AggregableMultiOp<Writable, ?> res = null;
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case Min:
                    AggregableMultiOp<Writable, Long> minOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(AggregatorImpls.minLong.andFinally(toWritable)));
                    if (res == null)
                        res = minOp;
                    else {
                        res = res.andThen(minOp);
                    }
                case Max:
                    AggregableMultiOp<Writable, Long> maxOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(AggregatorImpls.maxLong.andFinally(toWritable)));
                    if (res == null)
                        res = maxOp;
                    else {
                        res = res.andThen(maxOp);
                    }
                case Mean:
                    AggregableMultiOp<Writable, Pair<Long, Double>> meanOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableMean()));
                    if (res == null)
                        res = meanOp;
                    else {
                        res = res.andThen(meanOp);
                    }
                case Count:
                    AggregableMultiOp<Writable, Long> countOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableCount<Long>()));
                    if (res == null)
                        res = countOp;
                    else {
                        res = res.andThen(countOp);
                    }
                case CountUnique:
                    AggregableMultiOp<Writable, HyperLogLogPlus> countDistinctOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp(new AggregatorImpls.AggregableCountUnique<Long>()));
                    if (res == null)
                        res = countDistinctOp;
                    else {
                        res = res.andThen(countDistinctOp);
                    }
                case TakeFirst:
                    AggregableMultiOp<Writable, Long> takeFirstOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableFirst<Long>()).andFinally(toWritable)));
                    if (res == null)
                        res = takeFirstOp;
                    else {
                        res = res.andThen(takeFirstOp);
                    }
                case TakeLast:
                    AggregableMultiOp<Writable, Long> takeLastOp = AggregableMultiOp.toLongWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableLast<Long>()).andFinally(toWritable)));
                    if (res == null)
                        res = takeLastOp;
                    else {
                        res = res.andThen(takeLastOp);
                    }
                default:
                    throw new UnsupportedOperationException("Reduction op \"" + lop.get(i) + "\" not supported on time columns");
            }
        }
        return res;
    }

    public static AggregableMultiOp<Writable, ?> reduceBytesColumn(List<ReduceOp> lop) {

        Function<Byte, Writable> toWritable = new Function<Byte, Writable>(){

            @Override
            public Writable apply(Byte aByte) {
                return new ByteWritable(aByte);
            }
        };

        AggregableMultiOp<Writable, ?> res = null;
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case TakeFirst:
                    AggregableMultiOp<Writable, Byte> takeFirstOp = AggregableMultiOp.toByteWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableFirst<Byte>()).andFinally(toWritable)));
                    if (res == null)
                        res = takeFirstOp;
                    else {
                        res = res.andThen(takeFirstOp);
                    }
                case TakeLast:
                    AggregableMultiOp<Writable, Byte> takeLastOp = AggregableMultiOp.toByteWritable(AggregableMultiOp.fromOp((new AggregatorImpls.AggregableLast<Byte>()).andFinally(toWritable)));
                    if (res == null)
                        res = takeLastOp;
                    else {
                        res = res.andThen(takeLastOp);
                    }
                default:
                    throw new UnsupportedOperationException("Cannot execute op \"" + lop.get(i) + "\" on String/Categorical column "
                            + "(can only perform Count, CountUnique, TakeFirst and TakeLast ops on categorical columns)");
            }
        }
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("StringReducer(");
        if (keyColumns != null) {
            sb.append("keyColumns=").append(keyColumns).append(",");
        }
        sb.append("defaultOp=").append(defaultOp);
        if (opMap != null) {
            sb.append(",opMap=").append(opMap);
        }
        if (customReductions != null) {
            sb.append(",customReductions=").append(customReductions);
        }
        if (ignoreInvalidInColumns != null) {
            sb.append(",ignoreInvalidInColumns=").append(ignoreInvalidInColumns);
        }
        sb.append(")");
        return sb.toString();
    }


    public static class Builder {

        private ReduceOp defaultOp;
        private Map<String, List<ReduceOp>> opMap = new HashMap<>();
        private Map<String, AggregableColumnReduction> customReductions = new HashMap<>();
        private Set<String> ignoreInvalidInColumns = new HashSet<>();
        private String[] keyColumns;


        /**
         * Create a StringReducer builder, and set the default column reduction operation.
         * For any columns that aren't specified explicitly, they will use the default reduction operation.
         * If a column does have a reduction operation explicitly specified, then it will override
         * the default specified here.
         *
         * @param defaultOp Default reduction operation to perform
         */
        public Builder(ReduceOp defaultOp) {
            this.defaultOp = defaultOp;
        }

        /**
         * Specify the key columns. The idea here is to be able to create a (potentially compound) key
         * out of multiple columns, using the toString representation of the values in these columns
         *
         * @param keyColumns Columns that will make up the key
         * @return
         */
        public Builder keyColumns(String... keyColumns) {
            this.keyColumns = keyColumns;
            return this;
        }

        private Builder add(ReduceOp op, String[] cols) {
            for (String s : cols) {
                opMap.put(s, Collections.singletonList(op));
            }
            return this;
        }

        /**
         * Reduce the specified columns by taking the minimum value
         */
        public Builder minColumns(String... columns) {
            return add(ReduceOp.Min, columns);
        }

        /**
         * Reduce the specified columns by taking the maximum value
         */
        public Builder maxColumn(String... columns) {
            return add(ReduceOp.Max, columns);
        }

        /**
         * Reduce the specified columns by taking the sum of values
         */
        public Builder sumColumns(String... columns) {
            return add(ReduceOp.Sum, columns);
        }

        /**
         * Reduce the specified columns by taking the mean of the values
         */
        public Builder meanColumns(String... columns) {
            return add(ReduceOp.Mean, columns);
        }

        /**
         * Reduce the specified columns by taking the standard deviation of the values
         */
        public Builder stdevColumns(String... columns) {
            return add(ReduceOp.Stdev, columns);
        }

        /**
         * Reduce the specified columns by counting the number of values
         */
        public Builder countColumns(String... columns) {
            return add(ReduceOp.Count, columns);
        }

        /**
         * Reduce the specified columns by taking the range (max-min) of the values
         */
        public Builder rangeColumns(String... columns) {
            return add(ReduceOp.Range, columns);
        }

        /**
         * Reduce the specified columns by counting the number of unique values
         */
        public Builder countUniqueColumns(String... columns) {
            return add(ReduceOp.CountUnique, columns);
        }

        /**
         * Reduce the specified columns by taking the first value
         */
        public Builder takeFirstColumns(String... columns) {
            return add(ReduceOp.TakeFirst, columns);
        }

        /**
         * Reduce the specified columns by taking the last value
         */
        public Builder takeLastColumns(String... columns) {
            return add(ReduceOp.TakeLast, columns);
        }

        /**
         * Reduce the specified column using a custom column reduction functionality.
         *
         * @param column          Column to execute the custom reduction functionality on
         * @param columnReduction Column reduction to execute on that column
         */
        public Builder customReduction(String column, AggregableColumnReduction columnReduction) {
            customReductions.put(column, columnReduction);
            return this;
        }

        /**
         * When doing the reduction: set the specified columns to ignore any invalid values.
         * Invalid: defined as being not valid according to the ColumnMetaData: {@link ColumnMetaData#isValid(Writable)}.
         * For numerical columns, this typically means being unable to parse the Writable. For example, Writable.toLong() failing for a Long column.
         * If the column has any restrictions (min/max values, regex for Strings etc) these will also be taken into account.
         *
         * @param columns Columns to set 'ignore invalid' for
         */
        public Builder setIgnoreInvalid(String... columns) {
            Collections.addAll(ignoreInvalidInColumns, columns);
            return this;
        }

        public MultiOpReducer build() {
            return new MultiOpReducer(this);
        }
    }


}
