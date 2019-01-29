import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BackfillDayStats {
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    Calendar day;
    public int numSymbols;
    public AtomicInteger curNumSymbols = new AtomicInteger(0);

    public List<String> symbolsWithTrades = new ArrayList<>();
    public List<String> symbolsWith1m = new ArrayList<>();
    public List<String> symbolsWith1d = new ArrayList<>();
    public long timeElapsed;

    public BackfillDayStats(Calendar day, int numSymbols) {
        this.day = day;
        this.numSymbols = numSymbols;
    }

    public String toString() {
        return String.format("%s stats: %d symbols with trades %d symbols with 1m %d symbols with 1d",
                sdf.format(day.getTime()), symbolsWithTrades.size(), symbolsWith1m.size(), symbolsWith1d.size());
    }
}
