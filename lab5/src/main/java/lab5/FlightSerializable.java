package lab5;

import java.io.Serializable;

/**
 * Created by seven-teen on 16.12.16.
 */
public class FlightSerializable implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer originAirport;
    private Integer destAirport;
    private Float arrDelayNew;
    private Boolean cancelled;

    //public FlightSerializable(){}

    public FlightSerializable(Integer originAirport, Integer destAirport, Float arrDelayNew, Boolean cancelled){
        this.originAirport = originAirport;
        this.destAirport = destAirport;
        this.arrDelayNew = arrDelayNew;
        this.cancelled = cancelled;
    }

    public Boolean getCancelled() {
        return cancelled;
    }

    public Float getArrDelayNew() {
        return arrDelayNew;
    }

    public Integer getDestAirport() {
        return destAirport;
    }

    public Integer getOriginAirport() {
        return originAirport;
    }

}

