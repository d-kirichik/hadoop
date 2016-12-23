package iu9.trident.lab8;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Map;
/**
 * Created by seven-teen on 23.12.16.
 */

public class PrintStatsFunction extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<Integer, Integer> m = (Map<Integer, Integer>) tuple.get(0);
        m.forEach((k,v) -> System.out.println(k + ":\t" + v));
    }
}
