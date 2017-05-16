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
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.datavec.api.transform.ColumnType;
import org.datavec.api.transform.ReduceOp;
import org.datavec.api.transform.metadata.ColumnMetaData;
import org.datavec.api.transform.metadata.DoubleMetaData;
import org.datavec.api.transform.metadata.IntegerMetaData;
import org.datavec.api.transform.metadata.LongMetaData;
import org.datavec.api.transform.ops.*;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.writable.*;
import org.nd4j.shade.jackson.annotation.JsonIgnoreProperties;
import org.nd4j.shade.jackson.annotation.JsonProperty;

import java.util.*;

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
    public IAggregableReduceOp<List<Writable>, List<Writable>> aggregableReducer() {
        //Go through each writable, and reduce according to whatever strategy is specified

        if (schema == null)
            throw new IllegalStateException("Error: Schema has not been set");

        int nCols = schema.numColumns();
        List<String> colNames = schema.getColumnNames();

        List<IAggregableReduceOp<Writable, List<Writable>>> ops = new ArrayList<>(nCols);

        for (int i = 0; i < nCols; i++) {
            String colName = colNames.get(i);
            if (keyColumnsSet != null && keyColumnsSet.contains(colName)) {
                IAggregableReduceOp<Writable, Writable> first = new AggregatorImpls.AggregableFirst<>();
                ops.add(new AggregableMultiOp<>(Collections.singletonList(first)));
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

        return new DispatchOp<>(ops);
    }

    public static IAggregableReduceOp<Writable, List<Writable>> reduceColumn(List<ReduceOp> op, ColumnType type) {
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

    public static IAggregableReduceOp<Writable, List<Writable>> reduceIntColumn(List<ReduceOp> lop) {

        List<IAggregableReduceOp<Integer, Writable>> res = new ArrayList<>(lop.size());
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case Prod:
                    res.add(new AggregatorImpls.AggregableProd<Integer>());
                    break;
                case Min:
                    res.add(new AggregatorImpls.AggregableMin<Integer>());
                    break;
                case Max:
                    res.add(new AggregatorImpls.AggregableMax<Integer>());
                    break;
                case Range:
                    res.add(new AggregatorImpls.AggregableRange<Integer>());
                    break;
                case Sum:
                    res.add(new AggregatorImpls.AggregableSum<Integer>());
                    break;
                case Mean:
                    res.add(new AggregatorImpls.AggregableMean<Integer>());
                    break;
                case Stdev:
                    res.add(new AggregatorImpls.AggregableStdDev<Integer>());
                    break;
                case Count:
                    res.add(new AggregatorImpls.AggregableCount<Integer>());
                    break;
                case CountUnique:
                    res.add(new AggregatorImpls.AggregableCountUnique<Integer>());
                    break;
                case TakeFirst:
                    res.add(new AggregatorImpls.AggregableFirst<Integer>());
                    break;
                case TakeLast:
                    res.add(new AggregatorImpls.AggregableLast<Integer>());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown or not implemented op: " + lop.get(i));
            }
        }
        return new IntWritableOp<>(new AggregableMultiOp<>(res));
    }

    public static IAggregableReduceOp<Writable, List<Writable>> reduceLongColumn(List<ReduceOp> lop) {

        List<IAggregableReduceOp<Long, Writable>> res = new ArrayList<>(lop.size());
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case Prod:
                    res.add(new AggregatorImpls.AggregableProd<Long>());
                    break;
                case Min:
                    res.add(new AggregatorImpls.AggregableMin<Long>());
                    break;
                case Max:
                    res.add(new AggregatorImpls.AggregableMax<Long>());
                    break;
                case Range:
                    res.add(new AggregatorImpls.AggregableRange<Long>());
                    break;
                case Sum:
                    res.add(new AggregatorImpls.AggregableSum<Long>());
                    break;
                case Mean:
                    res.add(new AggregatorImpls.AggregableMean<Long>());
                    break;
                case Stdev:
                    res.add(new AggregatorImpls.AggregableStdDev<Long>());
                    break;
                case Count:
                    res.add(new AggregatorImpls.AggregableCount<Long>());
                    break;
                case CountUnique:
                    res.add(new AggregatorImpls.AggregableCountUnique<Long>());
                    break;
                case TakeFirst:
                    res.add(new AggregatorImpls.AggregableFirst<Long>());
                    break;
                case TakeLast:
                    res.add(new AggregatorImpls.AggregableLast<Long>());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown or not implemented op: " + lop.get(i));
            }
        }
        return new LongWritableOp<>(new AggregableMultiOp<>(res));
    }

    public static IAggregableReduceOp<Writable, List<Writable>> reduceFloatColumn(List<ReduceOp> lop) {

        List<IAggregableReduceOp<Float, Writable>> res = new ArrayList<>(lop.size());
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case Prod:
                    res.add(new AggregatorImpls.AggregableProd<Float>());
                    break;
                case Min:
                    res.add(new AggregatorImpls.AggregableMin<Float>());
                    break;
                case Max:
                    res.add(new AggregatorImpls.AggregableMax<Float>());
                    break;
                case Range:
                    res.add(new AggregatorImpls.AggregableRange<Float>());
                    break;
                case Sum:
                    res.add(new AggregatorImpls.AggregableSum<Float>());
                    break;
                case Mean:
                    res.add(new AggregatorImpls.AggregableMean<Float>());
                    break;
                case Stdev:
                    res.add(new AggregatorImpls.AggregableStdDev<Float>());
                    break;
                case Count:
                    res.add(new AggregatorImpls.AggregableCount<Float>());
                    break;
                case CountUnique:
                    res.add(new AggregatorImpls.AggregableCountUnique<Float>());
                    break;
                case TakeFirst:
                    res.add(new AggregatorImpls.AggregableFirst<Float>());
                    break;
                case TakeLast:
                    res.add(new AggregatorImpls.AggregableLast<Float>());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown or not implemented op: " + lop.get(i));
            }
        }
        return new FloatWritableOp<>(new AggregableMultiOp<>(res));
    }

    public static IAggregableReduceOp<Writable, List<Writable>> reduceDoubleColumn(List<ReduceOp> lop) {

        List<IAggregableReduceOp<Double, Writable>> res = new ArrayList<>(lop.size());
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case Prod:
                    res.add(new AggregatorImpls.AggregableProd<Double>());
                    break;
                case Min:
                    res.add(new AggregatorImpls.AggregableMin<Double>());
                    break;
                case Max:
                    res.add(new AggregatorImpls.AggregableMax<Double>());
                    break;
                case Range:
                    res.add(new AggregatorImpls.AggregableRange<Double>());
                    break;
                case Sum:
                    res.add(new AggregatorImpls.AggregableSum<Double>());
                    break;
                case Mean:
                    res.add(new AggregatorImpls.AggregableMean<Double>());
                    break;
                case Stdev:
                    res.add(new AggregatorImpls.AggregableStdDev<Double>());
                    break;
                case Count:
                    res.add(new AggregatorImpls.AggregableCount<Double>());
                    break;
                case CountUnique:
                    res.add(new AggregatorImpls.AggregableCountUnique<Double>());
                    break;
                case TakeFirst:
                    res.add(new AggregatorImpls.AggregableFirst<Double>());
                    break;
                case TakeLast:
                    res.add(new AggregatorImpls.AggregableLast<Double>());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown or not implemented op: " + lop.get(i));
            }
        }
        return new DoubleWritableOp<>(new AggregableMultiOp<>(res));
    }

    public static IAggregableReduceOp<Writable, List<Writable>> reduceStringOrCategoricalColumn(List<ReduceOp> lop) {

        List<IAggregableReduceOp<String, Writable>> res = new ArrayList<>(lop.size());
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case Count:
                    res.add(new AggregatorImpls.AggregableCount<String>());
                    break;
                case CountUnique:
                    res.add(new AggregatorImpls.AggregableCountUnique<String>());
                    break;
                case TakeFirst:
                    res.add(new AggregatorImpls.AggregableFirst<String>());
                    break;
                case TakeLast:
                    res.add(new AggregatorImpls.AggregableLast<String>());
                    break;
                default:
                    throw new UnsupportedOperationException("Cannot execute op \"" + lop.get(i) + "\" on String/Categorical column "
                            + "(can only perform Count, CountUnique, TakeFirst and TakeLast ops on categorical columns)");
            }
        }

        return new StringWritableOp<>(new AggregableMultiOp<>(res));
    }

    public static IAggregableReduceOp<Writable, List<Writable>> reduceTimeColumn(List<ReduceOp> lop) {

        List<IAggregableReduceOp<Long, Writable>> res = new ArrayList<>(lop.size());
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case Min:
                    res.add(new AggregatorImpls.AggregableMin<Long>());
                    break;
                case Max:
                    res.add(new AggregatorImpls.AggregableMax<Long>());
                    break;
                case Range:
                    res.add(new AggregatorImpls.AggregableRange<Long>());
                    break;
                case Mean:
                    res.add(new AggregatorImpls.AggregableMean<Long>());
                    break;
                case Stdev:
                    res.add(new AggregatorImpls.AggregableMean<Long>());
                    break;
                case Count:
                    res.add(new AggregatorImpls.AggregableCount<Long>());
                    break;
                case CountUnique:
                    res.add(new AggregatorImpls.AggregableCountUnique<Long>());
                    break;
                case TakeFirst:
                    res.add(new AggregatorImpls.AggregableFirst<Long>());
                    break;
                case TakeLast:
                    res.add(new AggregatorImpls.AggregableLast<Long>());
                    break;
                default:
                    throw new UnsupportedOperationException("Reduction op \"" + lop.get(i) + "\" not supported on time columns");
            }
        }
        return new LongWritableOp<>(new AggregableMultiOp<>(res));
    }

    public static IAggregableReduceOp<Writable, List<Writable>> reduceBytesColumn(List<ReduceOp> lop) {

        List<IAggregableReduceOp<Byte, Writable>> res = new ArrayList<>(lop.size());
        for (int i = 0; i < lop.size(); i++){
            switch (lop.get(i)) {
                case TakeFirst:
                    res.add(new AggregatorImpls.AggregableFirst<Byte>());
                    break;
                case TakeLast:
                    res.add(new AggregatorImpls.AggregableLast<Byte>());
                    break;
                default:
                    throw new UnsupportedOperationException("Cannot execute op \"" + lop.get(i) + "\" on Bytes column "
                            + "(can only perform TakeFirst and TakeLast ops on bytes columns)");
            }
        }
        return new ByteWritableOp<>(new AggregableMultiOp<>(res));
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
