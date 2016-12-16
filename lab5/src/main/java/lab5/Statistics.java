package lab5;

import java.io.Serializable;

/**
 * Created by seven-teen on 16.12.16.
 */
public class Statistics implements Serializable{
    private static final long serialVersionUID = 2L;

    private Integer count;
    private Integer delayedAndCancelled;
    private Float maxDelay;

    public Statistics(){
        this.count = 0;
        this.delayedAndCancelled = 0;
        this.maxDelay = 0f;
    }

    public static Statistics addFlight(Statistics a, FlightSerializable f){
        Statistics s = new Statistics();
        s.count = a.count + 1;
        if(f.getCancelled() || f.getArrDelayNew() > 0){
            s.delayedAndCancelled = a.delayedAndCancelled + 1;
            s.maxDelay = Math.max(a.maxDelay, f.getArrDelayNew());
        }
        return s;
    }

    public static Statistics mergeStatistics(Statistics a, Statistics b){
        Statistics s = new Statistics();
        s.count = a.count + b.count;
        s.delayedAndCancelled = a.delayedAndCancelled + b.delayedAndCancelled;
        s.maxDelay = Math.max(a.maxDelay, b.maxDelay);
        return s;
    }

    @Override
    public String toString(){
        return "All flights: " + count + ";\nPercent of delayed and cancelled: " + (float)(delayedAndCancelled)/(float)count * 100 + ";\nMax delay: " +
                maxDelay + ';';
    }
}
