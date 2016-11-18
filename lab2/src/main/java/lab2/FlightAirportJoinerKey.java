package lab2;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by seven-teen on 17.11.16.
 */
public class FlightAirportJoinerKey implements WritableComparable{

    private Integer airportID;
    private Boolean isFlight;

    public FlightAirportJoinerKey(){}

    public FlightAirportJoinerKey(Integer airportID, Boolean isFlight){
        this.airportID = airportID;
        this.isFlight = isFlight;
    }

    public Integer getAirportID() {
        return airportID;
    }

    @Override
    public String toString() {
        return airportID + "," + isFlight;
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof FlightAirportJoinerKey)){
            return -1;
        }
        FlightAirportJoinerKey second = (FlightAirportJoinerKey) o;
        if(this.airportID == null)
            return 1;
        else if(second.airportID == null)
            return 0;
        else return this.airportID - second.airportID;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(this.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String tmp[] = dataInput.readLine().split(",");
        if(tmp[0].equals("null"))
            return;
        airportID = Integer.parseInt(tmp[0]);
        isFlight = Boolean.parseBoolean(tmp[1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlightAirportJoinerKey)) return false;

        FlightAirportJoinerKey second = (FlightAirportJoinerKey) o;

        return (Integer.compare(second.airportID, airportID) != 0) &&  (isFlight == second.isFlight);
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
}
