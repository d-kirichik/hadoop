package lab2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by seven-teen on 17.11.16.
 */
public class FlightDataJoinComparator extends WritableComparator {
    protected FlightDataJoinComparator() {
        super(FlightAirportJoinerKey.class, true);
    }

    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        FlightAirportJoinerKey f = (FlightAirportJoinerKey) first;
        FlightAirportJoinerKey s = (FlightAirportJoinerKey) second;
        if(f.getAirportID() == null || s.getAirportID() == null)
            return 0;
        return f.getAirportID() - s.getAirportID();
    }
}
