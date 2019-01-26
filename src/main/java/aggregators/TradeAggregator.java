package aggregators;

import common.OHLCV;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.Trade;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class TradeAggregator {
    static final int minMinuteSize = 100;
    static final int minDaySize = 100;
    final static Logger logger = LogManager.getLogger(TradeAggregator.class);

    public static List<OHLCV> aggregateTrades(List<Trade> trades, int resolution) {
        List<OHLCV> res = new ArrayList<>();

        OHLCV curBucket = new OHLCV(trades.get(0).t / resolution);
        for (Trade t : trades) {
            long time = t.t / resolution;
            if (time > curBucket.time) {
                if (curBucket.open != 0) { // Substantial number of trades...
                    curBucket.time *= resolution;
                    res.add(curBucket);
                }
                curBucket = new OHLCV(time);
            }

            if (t.s >= minMinuteSize && !t.hasFlag(53)) {
                if (curBucket.open == 0) {
                    logger.debug("{} open from tick at {}", curBucket.time * resolution, t.t);
                    curBucket.open = t.p;
                    curBucket.high = t.p;
                    curBucket.low = t.p;
                }
                if (curBucket.high < t.p && !t.isExcluded()) {
                    logger.debug("{} high from tick at {}", curBucket.time * resolution, t.t);
                    curBucket.high = t.p;
                }
                if (curBucket.low > t.p && !t.isExcluded()) {
                    logger.debug("{} low from tick at {}", curBucket.time * resolution, t.t);
                    curBucket.low = t.p;
                }
                logger.debug("{} close from tick at {}", curBucket.time * resolution, t.t);
                curBucket.close = t.p;
            }

            curBucket.volume += t.s;
        }
        if (curBucket.open != 0) { // Substantial number of trades...
            curBucket.time *= resolution;
            res.add(curBucket);
        }

        return res;
    }

    public static OHLCV aggregateDay(List<Trade> trades, Date day) {
        Calendar end = Calendar.getInstance();
        end.setTime(new Date(day.getTime()));
        end.set(Calendar.HOUR_OF_DAY, 16);

        OHLCV res = new OHLCV(end.getTimeInMillis());
        for (Trade t : trades) {
            if (!t.hasFlag(12) && t.s >= minDaySize) {
                if (res.open == 0) {
                    logger.debug("daily open from tick at {}", t.t);
                    res.open = t.p;
                    res.high = t.p;
                    res.low = t.p;
                }
                if (res.high < t.p && !t.isExcluded()) {
                    logger.debug("daily high from tick at {}", t.t);
                    res.high = t.p;
                }
                if (res.low > t.p && !t.isExcluded()) {
                    logger.debug("daily low from tick at {}", t.t);
                    res.low = t.p;
                }
                logger.debug("daily close from tick at {}", t.t);
                res.close = t.p;
            }

            res.volume += t.s;
        }

        return res;
    }
}
