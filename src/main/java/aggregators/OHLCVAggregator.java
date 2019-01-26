package aggregators;

import common.OHLCV;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class OHLCVAggregator {
    static final int minSize = 100;

    public static List<OHLCV> aggregate(List<OHLCV> candles, int resolution) {
        List<OHLCV> res = new ArrayList<>();

        OHLCV curBucket = new OHLCV(candles.get(0).time / resolution);
        for (OHLCV c : candles) {
            long time = c.time / resolution;
            if (time > curBucket.time) {
                curBucket.time *= resolution;
                res.add(curBucket);
                curBucket = new OHLCV(time);
            }

            if (curBucket.open == 0) curBucket.open = c.open;
            if (curBucket.high < c.high) curBucket.high = c.high;
            if (curBucket.low > c.low) curBucket.low = c.low;
            curBucket.close = c.close;
            curBucket.volume += c.volume;
        }
        curBucket.time *= resolution;
        res.add(curBucket);

        return res;
    }

    public static OHLCV aggregateDay(List<OHLCV> candles_1m, Date day) {
        Calendar start = Calendar.getInstance();
        start.setTime(new Date(day.getTime()));
        start.set(Calendar.HOUR_OF_DAY, 9);
        start.set(Calendar.MINUTE, 29);

        Calendar end = Calendar.getInstance();
        end.setTime(new Date(day.getTime()));
        end.set(Calendar.HOUR_OF_DAY, 16);

        OHLCV res = new OHLCV(end.getTimeInMillis());
        for (OHLCV c : candles_1m) {
            Calendar d = Calendar.getInstance();
            d.setTime(new Date(c.time));
            if (d.after(start) && d.before(end) && c.volume >= minSize) {
                if (res.open == 0) res.open = c.open;
                if (res.high < c.high) res.high = c.high;
                if (res.low > c.low) res.low = c.low;
                res.close = c.close;
            }

            res.volume += c.volume;
        }

        return res;
    }
}
