import aggregators.TradeAggregator;
import polygon.models.OHLCV;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import polygon.PolygonClient;
import polygon.models.Trade;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

// TODO: Thread pool but keep debug messages separate
public class AggTest {
    private final PolygonClient client = new PolygonClient();
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private final static Logger logger = LogManager.getLogger(AggTest.class);

    // Don't bother stopping execution, just report all at end
    @Rule
    public ErrorCollector collector = new ErrorCollector();

    // Whitelist: ARCI
    @Test
    public void dayAggTest() {
        Calendar day = Calendar.getInstance();
        try {
            day.setTime(sdf.parse("2019-01-18"));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        List<String> symbols = Main.loadSymbols();
        int prevSize = symbols.size();
        symbols.removeIf(s -> s.contains(" ") || s.contains("#") || s.contains("/"));
        logger.info("Removed {} weird symbols", prevSize - symbols.size());
        Collections.shuffle(symbols);
        symbols = symbols.subList(0, 9);
        logger.info("Symbols: {}", symbols);

        for (String symbol : symbols) {
            logger.info("Testing {} on {}", symbol, sdf.format(day.getTime()));
            List<Trade> trades = client.getTradesForSymbol(sdf.format(day.getTime()), symbol);
            // Sort trades by timestamp
            trades.sort((a, b) -> a.timeMillis < b.timeMillis ? 1 : 0);

            OHLCV candle1d = TradeAggregator.aggregateDay(trades, day);
            OHLCV candle1dPolygon = client.getHistDayForSymbol(day, symbol);
            if (trades.size() > 0 && candle1dPolygon != null) {
                boolean isGood = candle1d.compareTo(candle1dPolygon) == 0;
                String errMsg = String.format("%s %s\nexpected: %s\nactual: %s\n",
                        sdf.format(day.getTime()), symbol, candle1d, candle1dPolygon);
                if (!isGood) {
                    logger.error(errMsg);
                }
                collector.checkThat(errMsg, true, is(isGood));
            }
        }
    }

    public boolean checkMinutes(List<OHLCV> expected, List<OHLCV> actual, String symbol) {
        int wrong = 0;
        int p1 = 0, p2 = 0;
        while (p1 < expected.size() && p2 < actual.size()) {
            if (expected.get(p1).timeMillis < actual.get(p2).timeMillis) {
                p2++;
            } else if (expected.get(p1).timeMillis > actual.get(p2).timeMillis) {
                p1++;
            } else if (expected.get(p1).compareTo(actual.get(p2)) != 0) {
                logger.error("{} {} {}", symbol, expected.get(p1), actual.get(p2));
                wrong++;
            }
            p1++;
            p2++;
        }
        if (wrong > 0) {
            logger.error("{} {} wrong", symbol, wrong);
            return false;
        }

        return true;
    }

    @Test
    public void minuteAggTest() {
        Calendar day = Calendar.getInstance();
        try {
            day.setTime(sdf.parse("2019-01-18"));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        List<String> symbols = Main.loadSymbols();
        int prevSize = symbols.size();
        symbols.removeIf(s -> s.contains(" ") || s.contains("#") || s.contains("/"));
        logger.info("Removed {} weird symbols", prevSize - symbols.size());
        Collections.shuffle(symbols);
        symbols = symbols.subList(0, 9);
        logger.info("Symbols: {}", symbols);

        for (String symbol : symbols) {
            logger.info("Testing {} on {}", symbol, sdf.format(day.getTime()));
            List<Trade> trades = client.getTradesForSymbol(sdf.format(day.getTime()), symbol);
            // Sort trades by timestamp
            trades.sort((a, b) -> a.timeMillis < b.timeMillis ? 1 : 0);

            List<OHLCV> candles1m = TradeAggregator.aggregate(trades, 1000 * 60);
            List<OHLCV> candles1mPolygon = client.getHistMinutesForSymbol(day, symbol);
            if (trades.size() > 0 && candles1mPolygon != null) {
                boolean isGood = checkMinutes(candles1m, candles1mPolygon, symbol);
                collector.checkThat(sdf.format(day.getTime()) + symbol, true, is(isGood));
            }
        }
    }
}
