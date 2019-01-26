package common;

import com.google.gson.annotations.SerializedName;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OHLCV implements Comparable<OHLCV> {
    @SerializedName("o")
    public double open;
    @SerializedName("h")
    public double high;
    @SerializedName("l")
    public double low;
    @SerializedName("c")
    public double close;
    @SerializedName("v")
    public int volume;
    @SerializedName("t")
    public long time; // ms

    public OHLCV(long t) {
        open = 0;
        high = Double.MIN_VALUE;
        low = Double.MAX_VALUE;
        close = 0;
        volume = 0;
        time = t;
    }

    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

        return String.format("%d (%s)\t\to: %4.3f h: %4.3f l: %4.3f c: %4.3f v: %d",
                time,
                sdf.format(new Date(time)),
                open,
                high,
                low,
                close,
                volume);
    }

    @Override
    public int compareTo(OHLCV o) {
//        if (Math.abs(open - o.open) < (open + o.open) / 2 * .02 &&
//                Math.abs(high - o.high) < (high + o.high) / 2 * .02 &&
//                Math.abs(low - o.low) < (low + o.low) / 2 * .02 &&
//                Math.abs(close - o.close) < (close + o.close) / 2 * .02
//                && volume == o.volume
//                && time == o.time)
//            return 0;
        if (Math.abs(open - o.open) < .02 &&
                Math.abs(high - o.high) < .02 &&
                Math.abs(low - o.low) < .02 &&
                Math.abs(close - o.close) < .02
//                && volume == o.volume
                && time == o.time)
            return 0;
        return -1;
    }
}
