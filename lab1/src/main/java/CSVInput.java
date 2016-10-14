import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by seven-teen on 14.10.16.
 */
public class CSVInput implements WritableComparable {

    protected double arr_delay;
    protected double airport_id;
    protected double air_time;
    protected boolean cancelled;

    public CSVInput() {
    }

    public CSVInput(String str) {
        String[] tmp = str.split(",");
        try {
            this.arr_delay = Double.parseDouble(tmp[tmp.length - 6]);
            this.airport_id = Double.parseDouble(tmp[tmp.length - 9]);
            this.air_time = Double.parseDouble(tmp[tmp.length - 2]);
            this.cancelled = tmp[tmp.length - 4].equals("1.00");
        }
        catch (NumberFormatException e){
            System.out.println(e.getMessage());
        }
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof CSVInput)){
            System.out.println("Smth goes wrong...");
            return -1;
        }
        CSVInput second = (CSVInput) o;
        if (this.cancelled && !second.cancelled)
            return 1;
        else if (!this.cancelled && second.cancelled)
            return -1;
        else if (this.arr_delay != second.arr_delay)
            return ((int)this.arr_delay - (int)second.arr_delay);
        else if (this.airport_id != second.airport_id)
            return ((int)this.airport_id - (int)second.airport_id);
        else if (this.air_time != second.air_time)
            return ((int)this.air_time - (int)second.air_time);
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(this.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String tmp[] = dataInput.readLine().split(",");
        arr_delay = Double.parseDouble(tmp[0]);
        airport_id = Double.parseDouble(tmp[1]);
        air_time = Double.parseDouble(tmp[2]);
        cancelled = Boolean.parseBoolean(tmp[3]);
    }

    @Override
    public String toString() {
        return this.arr_delay +
                "," + this.airport_id +
                ',' + this.air_time +
                ',' + this.cancelled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CSVInput)) return false;

        CSVInput csvInput = (CSVInput) o;

        if (Double.compare(csvInput.arr_delay, arr_delay) != 0) return false;
        if (Double.compare(csvInput.airport_id, airport_id) != 0) return false;
        if (Double.compare(csvInput.air_time, air_time) != 0) return false;
        return cancelled == csvInput.cancelled;

    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
}
