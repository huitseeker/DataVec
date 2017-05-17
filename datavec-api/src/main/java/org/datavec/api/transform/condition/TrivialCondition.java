package org.datavec.api.transform.condition;

import org.datavec.api.transform.schema.Schema;
import org.datavec.api.writable.Writable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by huitseeker on 5/17/17.
 */
public class TrivialCondition implements Condition {

    private Schema schema;
    private String name;

    protected TrivialCondition(String name){
        this.name = name;
    }

    @Override
    public boolean condition(List<Writable> list) {
        return true;
    }

    @Override
    public boolean condition(Object input) {
        return true;
    }

    @Override
    public boolean conditionSequence(List<List<Writable>> sequence) {
        return true;
    }

    @Override
    public boolean conditionSequence(Object sequence) {
        return true;
    }

    @Override
    public Schema transform(Schema inputSchema) {
        return inputSchema;
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
    public String outputColumnName() {
        return name;
    }

    @Override
    public String[] outputColumnNames() {
        return new String[]{name};
    }

    @Override
    public String[] columnNames() {
        return outputColumnNames();
    }

    @Override
    public String columnName() {
        return outputColumnName();
    }
}
