package lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by seven-teen on 17.11.16.
 */
public class AirportDataNameMapper extends Mapper<LongWritable, Text, FlightAirportJoinerKey, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AirportDataInput in = new AirportDataInput(value.toString());
        FlightAirportJoinerKey airportKey = new FlightAirportJoinerKey(in.getCode(), false);
        Text airportValue = new Text(in.getName()+ " 0");
        context.write(airportKey, airportValue);
    }
}
