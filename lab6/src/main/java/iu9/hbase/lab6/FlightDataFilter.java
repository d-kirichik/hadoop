package iu9.hbase.lab6;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * Created by seven-teen on 17.12.16.
 */

public class FlightDataFilter extends FilterBase {

    private float arrDelayNew;
    private boolean remove;

    public FlightDataFilter() {
        super();
        this.remove = true;
    }

    public FlightDataFilter(float arrDelayNew) {
        this.arrDelayNew = arrDelayNew;
        this.remove = true;
    }

    @Override
    public void reset() throws IOException {
        this.remove = true;
    }


    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
        String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
        String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
        if (columnName.equals("ARR_DELAY_NEW") && !columnValue.isEmpty() && Float.parseFloat(columnValue) > this.arrDelayNew){
                this.remove = false;
        }
        else if (columnName.equals("CANCELLED") && Float.parseFloat(columnValue) == 1.0f){
                this.remove = false;
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRow() throws IOException {
        return this.remove;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return Bytes.toBytes(arrDelayNew);
    }

    @SuppressWarnings("unused")
    public static Filter parseFrom(byte[] pbBytes) throws DeserializationException {
        return new FlightDataFilter(Bytes.toFloat(pbBytes));
    }

}

