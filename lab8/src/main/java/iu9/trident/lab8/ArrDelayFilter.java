package iu9.trident.lab8;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Created by seven-teen on 23.12.16.
 */
public class ArrDelayFilter extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        return !(tuple.getFloat(1)).equals(0f) || tuple.getBoolean(2);
    }
}
