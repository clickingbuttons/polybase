import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.OHLCV;
import polygon.models.Trade;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseWriter {
    static HBaseWriter instance = new HBaseWriter();
    final static Logger logger = LogManager.getLogger(HBaseWriter.class);

    Connection connection;

    HBaseWriter() {
        Configuration config = HBaseConfiguration.create();

        try {
            HBaseAdmin.available(config);
            connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HBaseWriter getInstance() {
        return instance;
    }

    public void createTable(String tableName, String familyName) {
        ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(familyName))
                .setCompressionType(Compression.Algorithm.SNAPPY)
                .build();

        TableDescriptor table = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(tableName))
                .setColumnFamily(cf)
                .build();
        try {
            connection.getAdmin().createTable(table);
            logger.info("Creating table {}", tableName);
        } catch (TableExistsException e) {
            logger.info("Table {} already exists!", tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeTrades(List<Trade> trades, String symbol) {
        byte[] cf = Bytes.toBytes("tf");

        List<Put> puts = new ArrayList<>();
        long lastMillis = 0;
        int streakCounter = 0;
        for (Trade t : trades) {
            if (lastMillis == t.timeMillis) streakCounter++;
            else streakCounter = 0;
            String key = String.format("%10s#%19d#%3d", symbol, t.timeMillis, streakCounter);
            Put p = new Put(Bytes.toBytes(key));

            p.setTimestamp(t.timeMillis);
            p.addColumn(cf, Bytes.toBytes("symbol"), Bytes.toBytes(symbol));
            p.addColumn(cf, Bytes.toBytes("size"), Bytes.toBytes(t.size));
            p.addColumn(cf, Bytes.toBytes("price"), Bytes.toBytes(t.price));
            p.addColumn(cf, Bytes.toBytes("conditions"), Bytes.toBytes(t.encodeConditions()));
            p.addColumn(cf, Bytes.toBytes("exchange"), Bytes.toBytes(Integer.parseInt(t.exchange)));

            lastMillis = t.timeMillis;
            puts.add(p);
        }
        try {
            Table tradeTable = connection.getTable(TableName.valueOf("trade"));
            tradeTable.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeCandles(List<OHLCV> candles, String symbol, String aggType) {
        byte[] cf = Bytes.toBytes("af");

        List<Put> puts = new ArrayList<>();
        for (OHLCV c : candles) {
            String key = String.format("%10s#%19d", symbol, c.timeMillis);
            Put p = new Put(Bytes.toBytes(key));

            p.setTimestamp(c.timeMillis);
            p.addColumn(cf, Bytes.toBytes("symbol"), Bytes.toBytes(symbol));
            p.addColumn(cf, Bytes.toBytes("open"), Bytes.toBytes(c.open));
            p.addColumn(cf, Bytes.toBytes("high"), Bytes.toBytes(c.high));
            p.addColumn(cf, Bytes.toBytes("low"), Bytes.toBytes(c.low));
            p.addColumn(cf, Bytes.toBytes("close"), Bytes.toBytes(c.close));
            p.addColumn(cf, Bytes.toBytes("volume"), Bytes.toBytes(c.volume));

            puts.add(p);
        }
        try {
            Table aggTable = connection.getTable(TableName.valueOf("agg" + aggType));
            aggTable.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
