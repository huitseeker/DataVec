package org.datavec.api.transform.ops;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.datavec.api.writable.Text;
import org.datavec.api.writable.Writable;

import java.util.Collections;

/**
 * Created by huitseeker on 5/18/17.
 */
public class StringAggregatorImpls {

    public static abstract class AggregableStringReduce implements IAggregableReduceOp<String, Writable> {
        @Getter
        protected StringBuilder sb = new StringBuilder();
    }

    public static class AggregableStringAppend extends AggregableStringReduce {

        @Override
        public <W extends IAggregableReduceOp<String, Writable>> void combine(W accu) {
            if (accu instanceof AggregableStringAppend)
                sb.append(((AggregableStringAppend)accu).getSb());
        }

        @Override
        public void accept(String s) {
            sb.append(s);
        }

        @Override
        public Writable get() {
            return new Text(sb.toString());
        }
    }

    public static class AggregableStringPrepend extends AggregableStringReduce {

        @Override
        public <W extends IAggregableReduceOp<String, Writable>> void combine(W accu) {
            if (accu instanceof  AggregableStringPrepend)
                sb.append(((AggregableStringPrepend) accu).getSb());
        }

        @Override
        public void accept(String s) {
            String rev = new StringBuilder(s).reverse().toString();
            sb.append(rev);
        }

        @Override
        public Writable get() {
            return new Text(sb.toString());
        }
    }

    public static class AggregableStringReplace extends AggregatorImpls.AggregableLast<String>{
        // This operation is *weird*
        @Getter
        int callCount = 0;

        @Override
        public void accept(String s){
            callCount += 1;
            if (callCount > 2) throw new IllegalArgumentException("Unable to run replace on columns > 2");
            else super.accept(s);
        }

        @Override
        public <W extends IAggregableReduceOp<String, Writable>> void combine (W accu){
            if (accu instanceof AggregableStringReplace) {
                AggregableStringReplace accumulator = ((AggregableStringReplace) accu);
                if (callCount + accumulator.getCallCount() > 2)
                    throw new IllegalArgumentException("Unable to run replace on columns > 2");
                else
                    super.combine(accu);
            }
        }
    }

}
