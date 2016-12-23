package iu9.trident.lab8;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;


/**
 * Created by seven-teen on 23.12.16.
 */

public class ParseFlightFunction extends BaseFunction {

    private int day;
    private float delay;
    private boolean cancelled;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String line = tuple.getString(0);
        parseLine(line);
        collector.emit(new Values(this.day, this.delay, this.cancelled));
    }

    private void parseLine(String input){
        String[] tmp = input.split(",");
        if(!tmp[4].equals("\"DAY_OF_WEEK\"")){
            this.day = Integer.parseInt(tmp[4]);
        }
        if(!tmp[18].equals("\"ARR_DELAY_NEW\"") && ! tmp[18].isEmpty()) {
            this.delay = Float.parseFloat(tmp[18]);
        }
        if(tmp[19].equals("1.00")) {
            this.cancelled = true;
        }
    }
}
