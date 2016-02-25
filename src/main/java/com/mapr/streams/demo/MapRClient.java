package com.mapr.streams.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Created by mlalapet on 2/24/16.
 */
public class MapRClient {
    private static Configuration conf;
    private static final String TABLE_NAME = "/tables/streammetrics";

    static {
        conf = HBaseConfiguration.create();
    }

    public static int getRecords(String prefixStr, int limit, Logger l) throws IOException {
        int count = 0;

        HTable profileTable = null;
        try {

            long ts1 = System.currentTimeMillis();

            profileTable = new HTable(conf, TABLE_NAME);

            byte[] prefix=Bytes.toBytes(prefixStr+"_10S");
            Scan scan = new Scan(prefix);
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            PrefixFilter prefixFilter = new PrefixFilter(prefix);
            filterList.addFilter(prefixFilter);
            filterList.addFilter(new PageFilter(limit));
            scan.setFilter(filterList);

            ResultScanner scanner = profileTable.getScanner (scan);

            for (Result result = scanner.next(); (result != null); result = scanner.next()) {
                for(KeyValue keyValue : result.list()) {
                    count = count + Integer.parseInt(Bytes.toString(keyValue.getValue()));
                }
            }

            long ts2 = System.currentTimeMillis();
            long durCP = ts2 - ts1;

            if (l != null)
                l.info("GetProfile ts durCP=" + durCP + "msec ");

        } catch (IOException e) {
            if (l != null)
                l.severe(e.toString());
            throw e;
        } finally {
            if (profileTable != null)
                profileTable.close();
        }
        return count;
    }

    public static void main(String[] args) throws IOException {

        Logger log = Logger.getLogger("my.logger");
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        handler.setLevel(Level.ALL);
        log.addHandler(handler);
        log.fine("hello world");

        int last10s = MapRClient.getRecords(args[0],1, log);
        int last1m = MapRClient.getRecords(args[0],6, log);
        int last5m = MapRClient.getRecords(args[0],30, log);
        int last15m = MapRClient.getRecords(args[0],90, log);
        int last1hr = MapRClient.getRecords(args[0],360, log);

        System.out.println(" 10s "+last10s+" 1m "+last1m+" 5m "+last5m+" 15m "+last15m+" 1hr "+last1hr);

    }
}
