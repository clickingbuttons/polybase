import hbase.HColumnEnum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import polygon.models.OHLCV;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import static junit.framework.TestCase.assertEquals;

public class ReadTest {
    final static Logger logger = LogManager.getLogger(ReadTest.class);

    @Test
    public void ScanAgg1sTest() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Configuration config = HBaseConfiguration.create();
        Table agg1s = null;
        try {
            HBaseAdmin.available(config);
            Connection connection = ConnectionFactory.createConnection(config);
            agg1s = connection.getTable(TableName.valueOf("agg1d"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (agg1s != null) {
            Scan scan = new Scan();

            Calendar from = Calendar.getInstance();
            Calendar to = Calendar.getInstance();
            try {
                Date fromDate = sdf.parse("2019-01-25");
                Date toDate = sdf.parse("2019-01-28");

                from.setTime(fromDate);
                to.setTime(toDate);
            } catch (ParseException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            from.set(Calendar.HOUR_OF_DAY, 4);
            to.set(Calendar.HOUR_OF_DAY, 18);

            byte[] symbolBytes = Bytes.toBytes(String.format("%5s", "GME"));
            ByteBuffer fromKey = ByteBuffer.allocate(5 + 13 + 2);
            fromKey.put(symbolBytes);
            fromKey.put(Bytes.toBytes(String.format("%13d", from.getTimeInMillis())));
            fromKey.put(Bytes.toBytes((short) 0));

            ByteBuffer toKey = ByteBuffer.allocate(5 + 13 + 2);
            toKey.put(symbolBytes);
            toKey.put(Bytes.toBytes(String.format("%13d", to.getTimeInMillis())));
            toKey.put(Bytes.toBytes(Short.MAX_VALUE));


            scan.withStartRow(fromKey.array());
            scan.withStopRow(toKey.array(), true);

            long startTime = System.currentTimeMillis();
            int count = 0;
            try {
                ResultScanner scanner = agg1s.getScanner(scan);
                for (Result result = scanner.next(); (result != null); result = scanner.next()) {
                    byte[] key = result.getRow();

//                    assertEquals(20, key.length);
                    String symbol = Bytes.toString(Arrays.copyOfRange(key, 0, 5));
                    long timestamp = Long.parseLong(Bytes.toString(Arrays.copyOfRange(key, 5, 5 + 13)));

                    OHLCV res = new OHLCV(timestamp);
                    res.open = Bytes.toDouble(result.getValue(HColumnEnum.AGG_COL_FAMILY.bytes,
                            HColumnEnum.AGG_COL_OPEN.bytes));
                    res.high = Bytes.toDouble(result.getValue(HColumnEnum.AGG_COL_FAMILY.bytes,
                            HColumnEnum.AGG_COL_HIGH.bytes));
                    res.low = Bytes.toDouble(result.getValue(HColumnEnum.AGG_COL_FAMILY.bytes,
                            HColumnEnum.AGG_COL_LOW.bytes));
                    res.close = Bytes.toDouble(result.getValue(HColumnEnum.AGG_COL_FAMILY.bytes,
                            HColumnEnum.AGG_COL_CLOSE.bytes));
                    res.volume = Bytes.toInt(result.getValue(HColumnEnum.AGG_COL_FAMILY.bytes,
                            HColumnEnum.AGG_COL_VOLUME.bytes));

                    count++;
                    logger.info("{}: {}", symbol, res);
                }
                scanner.close();
                agg1s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            long duration = (System.currentTimeMillis() - startTime);
            logger.info("Took {}ms for {} rows", duration, count);
        }
    }
}
