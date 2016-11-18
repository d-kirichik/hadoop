package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by seven-teen on 17.11.16.
 */
public class FlightDataJoinPartitioner  extends Partitioner<FlightAirportJoinerKey, Text> {
    @Override
    public int getPartition(FlightAirportJoinerKey flightdata, Text text, int i) {
        if(flightdata.getAirportID() != null)
            return (flightdata.getAirportID() & Integer.MAX_VALUE) % i;
        return 1;
    }
}
