package lab2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by seven-teen on 16.11.16.
 */
public class FlightDataInput implements Writable {


    private Integer airportID;
    private Double arrDelayNew;

    public Integer getAirportID() {
        return airportID;
    }

    public Double getArrDelayNew() {
        return arrDelayNew;
    }


    public FlightDataInput() {}

    public FlightDataInput(String str) {
        String[] tmp = str.split(",");
        if(!tmp[tmp.length - 5].equals("\"ARR_DELAY_NEW\"") && !tmp[tmp.length - 5].isEmpty()) {
            this.arrDelayNew = Double.parseDouble(tmp[tmp.length - 5]);
        }
        if(!tmp[tmp.length - 9].equals("\"DEST_AIRPORT_ID\"")){
            this.airportID = Integer.parseInt(tmp[tmp.length - 9]);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(this.arrDelayNew.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String tmp[] = dataInput.readLine().split(",");
        arrDelayNew = Double.parseDouble(tmp[0]);
        airportID = Integer.parseInt(tmp[1]);
    }

    @Override
    public String toString() {
        return this.arrDelayNew +
                "," + this.airportID;
    }
}
