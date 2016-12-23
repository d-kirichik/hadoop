package iu9.trident.lab8;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by seven-teen on 23.12.16.
 */
public class DayAggregator implements CombinerAggregator<Map<Integer, Integer>> {
    @Override
    public Map<Integer, Integer> init(TridentTuple tuple) {
        Map<Integer, Integer> m = new HashMap<>();
        m.put(tuple.getInteger(0), 1);
        return m;
    }

    @Override
    public Map<Integer, Integer> combine(Map<Integer, Integer> a, Map<Integer, Integer> b) {
        Map<Integer, Integer> m = new HashMap<>();
        m.putAll(a);
        b.forEach((k, v) -> m.merge(k, v, (v1,v2) -> v1+v2));
        return m;
    }

    @Override
    public Map<Integer, Integer> zero() {
        return new HashMap<>();
    }
}

