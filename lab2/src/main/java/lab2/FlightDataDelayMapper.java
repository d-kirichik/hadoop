package lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by seven-teen on 17.11.16.
 */
public class FlightDataDelayMapper extends Mapper<LongWritable, Text, FlightAirportJoinerKey, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FlightDataInput in = new FlightDataInput(value.toString());
        if (in.getArrDelayNew() == null)
                return;
        FlightAirportJoinerKey flightKey = new FlightAirportJoinerKey(in.getAirportID(), true);
        Text flightValue = new Text(in.getArrDelayNew().toString() + " 1");
        if (!in.getArrDelayNew().equals(0.0)) {
            context.write(flightKey, flightValue);
        }
    }
}
