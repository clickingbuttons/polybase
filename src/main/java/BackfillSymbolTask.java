import aggregators.OHLCVAggregator;
import polygon.models.OHLCV;
import aggregators.TradeAggregator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.Trade;
import polygon.PolygonClient;
import hbase.HBaseWriter;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public class BackfillSymbolTask implements Runnable {
    private Calendar day;
    private String symbol;
    private BackfillDayStats stats;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private final PolygonClient client = new PolygonClient();
    final static Logger logger = LogManager.getLogger(BackfillSymbolTask.class);

    public BackfillSymbolTask(Calendar day, String symbol, BackfillDayStats stats) {
        this.day = (Calendar) day.clone();
        this.symbol = symbol;
        this.stats = stats;
    }

    public void logDone() {
        int num = stats.curNumSymbols.getAndIncrement();
        if (num % 500 == 0 && num != 0) {
            logger.info("{} Finished {} / {} ({} have trades)",
                    sdf.format(day.getTime()), stats.curNumSymbols, stats.numSymbols, stats.symbolsWithTrades.size());
        }
    }

    public void run() {
        List<Trade> trades = client.getTradesForSymbol(sdf.format(day.getTime()), symbol);
        // Sort trades by timestamp
        trades.sort((a, b) -> a.timeMillis < b.timeMillis ? 1 : 0);
        int numTrades = trades.size();

        List<OHLCV> candles1s = null;
        List<OHLCV> candles1m;
        OHLCV candle1d;

        if (numTrades > 0) {
            stats.symbolsWithTrades.add(symbol);
            logger.debug("{} {} had {} trades from {} to {}",
                    sdf.format(day.getTime()), symbol, numTrades,
                    trades.get(0).timeMillis, trades.get(trades.size() - 1).timeMillis);
            // The Magic
            candles1s = TradeAggregator.aggregate(trades, 1000);

            candles1m = OHLCVAggregator.aggregate(candles1s, 1000 * 60);
            candle1d = TradeAggregator.aggregateDay(trades, day);
        }
        else {
//            logger.warn("No trades for {} on {}", symbol, sdf.format(day.getTime()));
            candles1m = client.getHistMinutesForSymbol(day, symbol);
            if (candles1m.size() > 0) stats.symbolsWith1m.add(symbol);
            candle1d = client.getHistDayForSymbol(day, symbol);
            if (candle1d != null) stats.symbolsWith1d.add(symbol);
        }
        List<OHLCV> candles5m = OHLCVAggregator.aggregate(candles1m, 1000 * 60 * 5);
        List<OHLCV> candles1h = OHLCVAggregator.aggregate(candles5m, 1000 * 60 * 60);

        HBaseWriter writer = new HBaseWriter();
        if (numTrades > 0) {
            writer.writeTrades(trades, symbol);
            trades.clear();
        }
        if (candles1s != null) {
            writer.writeCandles(candles1s, symbol, "1s");
            candles1s.clear();
        }
        if (candles1m.size() > 0) {
            writer.writeCandles(candles1m, symbol, "1m");
            writer.writeCandles(candles5m, symbol, "5m");
            writer.writeCandles(candles1h, symbol, "1h");
            candles1m.clear();
            candles5m.clear();
            candles1h.clear();
        }
        if (candle1d != null) {
            writer.writeCandles(Arrays.asList(candle1d), symbol, "1d");
        }

        logDone();
    }
}
