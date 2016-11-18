package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by seven-teen on 18.11.16.
 */
public class FlightDataJoinReducer extends Reducer<FlightAirportJoinerKey, Text, Integer, Text> {
    @Override
    protected void reduce(FlightAirportJoinerKey key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
        Long count = 0L;
        Double min = Double.MAX_VALUE, max = 0.0, sum = 0.0, curValue;
        String value = "";
        for (Text v : vals) {
            String[] tmp = v.toString().split(" ");
            if(tmp[1].equals("1")) {
                curValue = Double.parseDouble(tmp[0]);
                if (curValue < min) {
                    min = curValue;
                }
                if (curValue > max) {
                    max = curValue;
                }
                sum += curValue;
            }
            else{
                value = tmp[0];
            }
            count++;
        }

        if (count > 1) {
            context.write(key.getAirportID(), new Text(value + ", min = " + min.toString() + ", max = " + max.toString() +
                    ", average =" + ((Double) (sum / count)).toString()));
        }
    }
}
