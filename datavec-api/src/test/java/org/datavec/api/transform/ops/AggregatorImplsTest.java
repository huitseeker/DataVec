package org.datavec.api.transform.ops;

import org.datavec.api.writable.IntWritable;
import org.datavec.api.writable.Text;
import org.datavec.api.writable.Writable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by huitseeker on 5/14/17.
 */
public class AggregatorImplsTest {

    private List<Integer> intList = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
    private List<String> stringList = new ArrayList<>(Arrays.asList("arakoa", "abracadabra", "blast", "acceptance"));
    private List<Writable> mixedList = new ArrayList<Writable>(Arrays.asList(new IntWritable(1), new Text("abracadabra")));


    @Test
    public void AggregableFirstTest(){
        AggregatorImpls.AggregableFirst<Integer> first = new AggregatorImpls.AggregableFirst<>();
        for (int i = 0; i < intList.size(); i++){
            first.accept(intList.get(i));
        }
        assertTrue(first.get().toInt() == 1);

        AggregatorImpls.AggregableFirst<String> firstS = new AggregatorImpls.AggregableFirst<>();
        for (int i = 0; i < stringList.size(); i++){
            firstS.accept(stringList.get(i));
        }
        assertTrue(firstS.get().toString().equals("arakoa"));


        AggregatorImpls.AggregableFirst<Integer> reverse = new AggregatorImpls.AggregableFirst<>();
        for (int i = 0; i < intList.size(); i++){
            reverse.accept(intList.get(intList.size() - i - 1));
        }
        first.combine(reverse);
        assertTrue(first.get().toInt() == 1);
    }


    @Test
    public void AggregableLastTest(){
        AggregatorImpls.AggregableLast<Integer> last = new AggregatorImpls.AggregableLast<>();
        for (int i = 0; i < intList.size(); i++){
            last.accept(intList.get(i));
        }
        assertTrue(last.get().toInt() == 9);

        AggregatorImpls.AggregableLast<String> lastS = new AggregatorImpls.AggregableLast<>();
        for (int i = 0; i < stringList.size(); i++){
            lastS.accept(stringList.get(i));
        }
        assertTrue(lastS.get().toString().equals("acceptance"));


        AggregatorImpls.AggregableLast<Integer> reverse = new AggregatorImpls.AggregableLast<>();
        for (int i = 0; i < intList.size(); i++){
            reverse.accept(intList.get(intList.size() - i - 1));
        }
        last.combine(reverse);
        assertTrue(last.get().toInt() == 1);
    }

    @Test
    public void AggregableCountTest(){
        AggregatorImpls.AggregableCount<Integer> cnt = new AggregatorImpls.AggregableCount<>();
        for (int i = 0; i < intList.size(); i++){
            cnt.accept(intList.get(i));
        }
        assertTrue(cnt.get().toInt() == 9);

        AggregatorImpls.AggregableCount<String> lastS = new AggregatorImpls.AggregableCount<>();
        for (int i = 0; i < stringList.size(); i++){
            lastS.accept(stringList.get(i));
        }
        assertTrue(lastS.get().toInt() == 4);


        AggregatorImpls.AggregableCount<Integer> reverse = new AggregatorImpls.AggregableCount<>();
        for (int i = 0; i < intList.size(); i++){
            reverse.accept(intList.get(intList.size() - i - 1));
        }
        cnt.combine(reverse);
        assertTrue(cnt.get().toInt() == 18);
    }

    @Test
    public void AggregableMaxTest(){
        AggregatorImpls.AggregableMax<Integer> mx = new AggregatorImpls.AggregableMax<>();
        for (int i = 0; i < intList.size(); i++){
            mx.accept(intList.get(i));
        }
        assertTrue(mx.get().toInt() == 9);


        AggregatorImpls.AggregableMax<Integer> reverse = new AggregatorImpls.AggregableMax<>();
        for (int i = 0; i < intList.size(); i++){
            reverse.accept(intList.get(intList.size() - i - 1));
        }
        mx.combine(reverse);
        assertTrue(mx.get().toInt() == 9);
    }


    @Test
    public void AggregableRangeTest(){
        AggregatorImpls.AggregableRange<Integer> mx = new AggregatorImpls.AggregableRange<>();
        for (int i = 0; i < intList.size(); i++){
            mx.accept(intList.get(i));
        }
        assertTrue(mx.get().toInt() == 8);


        AggregatorImpls.AggregableRange<Integer> reverse = new AggregatorImpls.AggregableRange<>();
        for (int i = 0; i < intList.size(); i++){
            reverse.accept(intList.get(intList.size() - i - 1) + 9);
        }
        mx.combine(reverse);
        assertTrue(mx.get().toInt() == 17);
    }

    @Test
    public void AggregableMinTest(){
        AggregatorImpls.AggregableMin<Integer> mn = new AggregatorImpls.AggregableMin<>();
        for (int i = 0; i < intList.size(); i++){
            mn.accept(intList.get(i));
        }
        assertTrue(mn.get().toInt() == 1);


        AggregatorImpls.AggregableMin<Integer> reverse = new AggregatorImpls.AggregableMin<>();
        for (int i = 0; i < intList.size(); i++){
            reverse.accept(intList.get(intList.size() - i - 1));
        }
        mn.combine(reverse);
        assertTrue(mn.get().toInt() == 1);
    }

    @Test
    public void AggregableSumTest(){
        AggregatorImpls.AggregableSum<Integer> sm = new AggregatorImpls.AggregableSum<>();
        for (int i = 0; i < intList.size(); i++){
            sm.accept(intList.get(i));
        }
        assertTrue(sm.get().toDouble() == 45D);


        AggregatorImpls.AggregableSum<Integer> reverse = new AggregatorImpls.AggregableSum<>();
        for (int i = 0; i < intList.size(); i++){
            reverse.accept(intList.get(intList.size() - i - 1));
        }
        sm.combine(reverse);
        assertTrue(sm.get().toDouble() == 90D);
    }


    @Test
    public void AggregableMeanTest(){
        AggregatorImpls.AggregableMean<Integer> mn = new AggregatorImpls.AggregableMean<>();
        for (int i = 0; i < intList.size(); i++){
            mn.accept(intList.get(i));
        }
        assertTrue(mn.getCount() == 9);
        assertTrue(mn.get().toDouble() == 5D);


        AggregatorImpls.AggregableMean<Integer> reverse = new AggregatorImpls.AggregableMean<>();
        for (int i = 0; i < intList.size(); i++){
            reverse.accept(intList.get(intList.size() - i - 1));
        }
        mn.combine(reverse);
        assertTrue(mn.getCount() == 18);
        assertTrue(mn.get().toDouble() == 5D);
    }

    @Test
    public void AggregableStdDevTest(){
        AggregatorImpls.AggregableStdDev<Integer> sd = new AggregatorImpls.AggregableStdDev<>();
        for (int i = 0; i < intList.size(); i++){
            sd.accept(intList.get(i));
        }
        assertTrue(Math.abs(sd.get().toDouble() - 2.7386) < 0.0001);


        AggregatorImpls.AggregableMean<Integer> reverse = new AggregatorImpls.AggregableMean<>();
        for (int i = 0; i < intList.size(); i++){
            reverse.accept(intList.get(intList.size() - i - 1));
        }
        sd.combine(reverse);
        assertTrue(Math.abs(sd.get().toDouble() - 2.7386) < 0.0001);
    }

    @Test
    public void AggregableCountUniqueTest(){
        // at this low range, it's linear counting

        AggregatorImpls.AggregableCountUnique<Integer> cu = new AggregatorImpls.AggregableCountUnique<>();
        for (int i = 0; i < intList.size(); i++){
            cu.accept(intList.get(i));
        }
        assertTrue(cu.get().toInt() == 9);
        cu.accept(1);
        assertTrue(cu.get().toInt() == 9);

        AggregatorImpls.AggregableCountUnique<Integer> reverse = new AggregatorImpls.AggregableCountUnique<>();
        for (int i = 0; i < intList.size(); i++){
            reverse.accept(intList.get(intList.size() - i - 1));
        }
        cu.combine(reverse);
        assertTrue(cu.get().toInt() == 9);
    }


    @Test
    public void IncompatibleAggregatorTest(){
        AggregatorImpls.AggregableSum<Integer> sm = new AggregatorImpls.AggregableSum<>();
        for (int i = 0; i < intList.size(); i++){
            sm.accept(intList.get(i));
        }
        assertTrue(sm.get().toInt() == 45D);


        AggregatorImpls.AggregableMean<Integer> reverse = new AggregatorImpls.AggregableMean<>();
        for (int i = 0; i < intList.size(); i++){
            reverse.accept(intList.get(intList.size() - i - 1));
        }
        sm.combine(reverse);
        assertTrue(sm.get().toInt() == 45D);
    }

}