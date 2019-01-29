package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.OHLCV;
import polygon.models.Trade;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class HBaseWriter {
    final static Logger logger = LogManager.getLogger(HBaseWriter.class);

    static Connection connection;
    static {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "127.0.0.1");
        config.setInt("hbase.zookeeper.property.clientPort", 2181);
//        config.set("hbase.master", "localhost:60000");
//        config.set("zookeeper.znode.parent", "/");
        try {
            HBaseAdmin.available(config);
            connection = ConnectionFactory.createConnection(config);
        } catch (MasterNotRunningException e) {
            logger.error("HBase and/or Zookeeper is not running");
            System.exit(-1);
        } catch (IOException e) {
            logger.error("Problem connecting to HBase: {}", e);
            System.exit(-1);
        }
    }

    public static void createTable(String tableName, String familyName) {
        try {
            ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(familyName))
                    .setCompressionType(Compression.Algorithm.SNAPPY)
                    .setDataBlockEncoding(DataBlockEncoding.PREFIX)
                    .setCompactionCompressionType(Compression.Algorithm.SNAPPY)
                    .build();

            TableDescriptor table = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(tableName))
                    .setColumnFamily(cf)
                    .setDurability(Durability.SKIP_WAL)
                    .setCompactionEnabled(true)
                    .build();

            connection.getAdmin().createTable(table);
            logger.info("Created table {}", tableName);
        } catch (TableExistsException e) {
            logger.info("Table {} already exists!", tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeTrades(List<Trade> trades, String symbol) {
        byte[] cf = HColumnEnum.TRADE_COL_FAMILY.bytes;
        byte[] symbolBytes = Bytes.toBytes(String.format("%5s", symbol));

        long lastMillis = 0;
        short streakCounter = 0;
        List<Put> puts = new ArrayList<>(trades.size());
        for (Trade t : trades) {
            if (lastMillis == t.timeMillis) streakCounter++;
            else streakCounter = 0;

            ByteBuffer key = ByteBuffer.allocate(5 + 13 + 2);
            key.put(symbolBytes);
            // Long.MAX_VALUE = 9,223,372,036,854,775,807 = 19 digits
            // 9,999,999,999,999 = Saturday, November 20, 2286 = 13 digits
            key.put(Bytes.toBytes(String.format("%13d", t.timeMillis)));
//            key.put(Bytes.toBytes(t.timeMillis));
            key.put(Bytes.toBytes(streakCounter));
            Put p = new Put(key.array());

            p.setTimestamp(t.timeMillis);
            p.addColumn(cf,
                    HColumnEnum.TRADE_COL_EXCHANGE.bytes, Bytes.toBytes(Short.parseShort(t.exchange)));
            p.addColumn(cf,
                    HColumnEnum.TRADE_COL_SYMBOL.bytes, Bytes.toBytes(symbol));
            p.addColumn(cf,
                    HColumnEnum.TRADE_COL_SIZE.bytes, Bytes.toBytes(t.size));
            p.addColumn(cf,
                    HColumnEnum.TRADE_COL_PRICE.bytes, Bytes.toBytes(t.price));
            p.addColumn(cf,
                    HColumnEnum.TRADE_COL_CONDITION.bytes, Bytes.toBytes(t.encodeConditions()));

            lastMillis = t.timeMillis;
            puts.add(p);
        }
        try {
            Table tradeTable = connection.getTable(TableName.valueOf("trade"));

            tradeTable.put(puts);
            tradeTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeCandles(List<OHLCV> candles, String symbol, String aggType) {
        byte[] cf = HColumnEnum.AGG_COL_FAMILY.bytes;
        byte[] symbolBytes = Bytes.toBytes(String.format("%5s", symbol));

        List<Put> puts = new ArrayList<>(candles.size());
        for (OHLCV c : candles) {
            ByteBuffer key = ByteBuffer.allocate(5 + 13);
            key.put(symbolBytes);
            key.put(Bytes.toBytes(String.format("%13d", c.timeMillis)));
//            key.put(Bytes.toBytes(c.timeMillis));
            Put p = new Put(key.array());

            p.setTimestamp(c.timeMillis);
            p.addColumn(cf,
                    HColumnEnum.AGG_COL_SYMBOL.bytes, Bytes.toBytes(symbol));
            p.addColumn(cf,
                    HColumnEnum.AGG_COL_OPEN.bytes, Bytes.toBytes(c.open));
            p.addColumn(cf,
                    HColumnEnum.AGG_COL_HIGH.bytes, Bytes.toBytes(c.high));
            p.addColumn(cf,
                    HColumnEnum.AGG_COL_LOW.bytes, Bytes.toBytes(c.low));
            p.addColumn(cf,
                    HColumnEnum.AGG_COL_CLOSE.bytes, Bytes.toBytes(c.close));
            p.addColumn(cf,
                    HColumnEnum.AGG_COL_VOLUME.bytes, Bytes.toBytes(c.volume));

            puts.add(p);
        }
        try {
            Table aggTable = connection.getTable(TableName.valueOf("agg" + aggType));

            aggTable.put(puts);
            aggTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
