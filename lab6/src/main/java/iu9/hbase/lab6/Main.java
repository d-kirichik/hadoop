package iu9.hbase.lab6;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
/**
 * Created by seven-teen on 17.12.16.
 */
public class Main {

    private static final byte[] TABLE_NAME = Bytes.toBytes("flightsData");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("data");

    private static final byte[] START_ROW = Bytes.toBytes("2015-01-01");
    private static final byte[] STOP_ROW = Bytes.toBytes("2015-01-30");

    private static final float DELAY_TIME = 10.0f;

    public static void main(String[] args){
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeper.quorum","localhost");

        try(Connection connection = ConnectionFactory.createConnection()) {

            Admin admin = connection.getAdmin();
            if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
                admin.createTable(descriptor);
            }

            Table flights = connection.getTable(TableName.valueOf(TABLE_NAME));

            String flightsData = "/home/seven-teen/Downloads/664600583_T_ONTIME_sample.csv";
            try (BufferedReader reader = new BufferedReader(new FileReader(flightsData))) {
                String line;
                int rowNumber = 1;

                while ((line = reader.readLine()) != null) {
                    flights.put(FlightsDataHTableCreator.fillColumn(COLUMN_FAMILY, line, rowNumber));
                    rowNumber++;
                }
            }
            Scan scan = new Scan();
            scan.addFamily(COLUMN_FAMILY);
            scan.setFilter(new FlightDataFilter(DELAY_TIME));
            scan.setStartRow(START_ROW);
            scan.setStopRow(STOP_ROW);

            ResultScanner scanner = flights.getScanner(scan);
            try(FileWriter writer = new FileWriter("/home/seven-teen/Documents/P_P/lab6/output_lab6.txt", false)){
                for (Result res : scanner) {
                    writer.write(Bytes.toString(res.getRow()) + "; Delay:" + Bytes.toString(res.getValue(COLUMN_FAMILY, Bytes.toBytes("ARR_DELAY_NEW"))) +
                            "; Cancelled:" + Bytes.toString(res.getValue(COLUMN_FAMILY, Bytes.toBytes("CANCELLED"))) + '\n');
                    writer.flush();
                }
            }
            catch(IOException ex){
                System.out.println(ex.getMessage());
            }
            scanner.close();
            flights.close();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
}
