package polygon;

import common.OHLCV;
import aggregators.OHLCVAggregator;
import aggregators.TradeAggregator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.Trade;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BackfillSymbolTask implements Runnable {
    private Date day;
    private String symbol;
    private AtomicInteger progress;
    private PolygonClient client;
    private int total;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    final static Logger logger = LogManager.getLogger(PolygonClient.class);

    public BackfillSymbolTask(String day, String symbol, PolygonClient client, AtomicInteger progress, int total) {
        this.client = client;
        try {
            this.day = sdf.parse(day);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        this.symbol = symbol;
        this.progress = progress;
        this.total = total;
    }

    public void logDone(int numTrades) {
        if (progress.incrementAndGet() % 1000 == 0) {
            logger.info("{} Finished {} / {}", sdf.format(day), progress, total);
        }
        logger.debug("{} {} Got {} trades ({} / {})", sdf.format(day), symbol, numTrades, progress, total);
    }

    public void run() {
        List<Trade> trades = client.getTradesForSymbol(sdf.format(day), symbol);
        // Sort trades by timestamp
        trades.sort((a, b) -> a.t < b.t ? 1 : 0);

        // Aggregate trades per-second
        List<OHLCV> candles_1s = TradeAggregator.aggregateTrades(trades, 1000);
        List<OHLCV> candles_1m = client.getHistAggForSymbol(sdf.format(day), symbol, "minute");
        List<OHLCV> candles_5m = OHLCVAggregator.aggregate(candles_1m, 1000 * 60 * 5);
        OHLCV daily = client.getHistAggForSymbol(sdf.format(day), symbol, "day").get(1);

//        for (OHLCV c : test) {
//            logger.debug(c);
//        }
//
//        int wrong = 0;
//        int p1 = 0, p2 = 0;
//        while (p1 < candles_1m.size() && p2 < test.size()) {
//            if (candles_1m.get(p1).time < test.get(p2).time) {
//                p2++;
//            }
//            else if (candles_1m.get(p1).time > test.get(p2).time) {
//                p1++;
//            }
//            else if (candles_1m.get(p1).compareTo(test.get(p2)) != 0) {
//                logger.error("{} {}", candles_1m.get(p1), test.get(p2));
//                wrong++;
//            }
//            p1++;
//            p2++;
//        }
//        logger.info("{} {} wrong", symbol, wrong);

        // For dailies, throw out candles before 21:00 = 1547845200 = 2019-01-18 21:00
//        logger.info("{} candles:", symbol);
//        for (OHLCV c : candles_1m)
//            logger.info(c);


//        logger.debug("{} ticks:", symbol);
//        for (Trade t : trades) {
//            if (t.p == 29.27)
//                logger.debug(t);
//        }
        OHLCV candle_1d = TradeAggregator.aggregateDay(trades, day);
        if (candle_1d.compareTo(daily) != 0) {
            logger.error("{} daily1:\t{}", symbol, candle_1d);
            logger.error("{} daily2:\t{}", symbol, daily);
        }

        logDone(trades.size());
    }
}
