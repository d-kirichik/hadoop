package iu9.hbase.lab6;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by seven-teen on 17.12.16.
 */

public class FlightsDataHTableCreator {

    private static final int FL_DATE_INDEX = 5;
    private static final int AIRLINE_ID_INDEX = 7;

    public static Put fillColumn(final byte[] family, String input, int row){
        String[] columns = input.split(",");
        Put put = new Put(Bytes.toBytes(columns[FL_DATE_INDEX] + ' ' + columns[AIRLINE_ID_INDEX] + ' ' + row));
        put.addColumn(family,Bytes.toBytes("YEAR"),Bytes.toBytes(columns[0]));
        put.addColumn(family,Bytes.toBytes("QUARTER"),Bytes.toBytes(columns[1]));
        put.addColumn(family,Bytes.toBytes("MONTH"),Bytes.toBytes(columns[2]));
        put.addColumn(family,Bytes.toBytes("DAY_OF_MONTH"),Bytes.toBytes(columns[3]));
        put.addColumn(family,Bytes.toBytes("DAY_OF_WEEK"),Bytes.toBytes(columns[4]));
        put.addColumn(family,Bytes.toBytes("FL_DATE"),Bytes.toBytes(columns[5]));
        put.addColumn(family,Bytes.toBytes("UNIQUE_CARRIER"),Bytes.toBytes(columns[6]));
        put.addColumn(family,Bytes.toBytes("AIRLINE_ID"),Bytes.toBytes(columns[7]));
        put.addColumn(family,Bytes.toBytes("CARRIER"),Bytes.toBytes(columns[8]));
        put.addColumn(family,Bytes.toBytes("TAIL_NUM"),Bytes.toBytes(columns[9]));
        put.addColumn(family,Bytes.toBytes("FL_NUM"),Bytes.toBytes(columns[10]));
        put.addColumn(family,Bytes.toBytes("ORIGIN_AIRPORT_ID"),Bytes.toBytes(columns[11]));
        put.addColumn(family,Bytes.toBytes("ORIGIN_AIRPORT_SEQ_ID"),Bytes.toBytes(columns[12]));
        put.addColumn(family,Bytes.toBytes("ORIGIN_CITY_MARKET_ID"),Bytes.toBytes(columns[13]));
        put.addColumn(family,Bytes.toBytes("DEST_AIRPORT_ID"),Bytes.toBytes(columns[14]));
        put.addColumn(family,Bytes.toBytes("WHEELS_ON"),Bytes.toBytes(columns[15]));
        put.addColumn(family,Bytes.toBytes("ARR_TIME"),Bytes.toBytes(columns[16]));
        put.addColumn(family,Bytes.toBytes("ARR_DELAY"),Bytes.toBytes(columns[17]));
        put.addColumn(family,Bytes.toBytes("ARR_DELAY_NEW"),Bytes.toBytes(columns[18]));
        put.addColumn(family,Bytes.toBytes("CANCELLED"),Bytes.toBytes(columns[19]));
        put.addColumn(family,Bytes.toBytes("CANCELLATION_CODE"),Bytes.toBytes(columns[20]));
        put.addColumn(family,Bytes.toBytes("AIR_TIME"),Bytes.toBytes(columns[21]));
        put.addColumn(family,Bytes.toBytes("DISTANCE"),Bytes.toBytes(columns[22]));
        return put;
    }
}
