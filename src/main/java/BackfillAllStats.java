import java.util.ArrayList;
import java.util.List;

public class BackfillAllStats {
    int symbolTradeCount = 0;
    int symbol1mCount = 0;
    int symbol1dCount = 0;

    public List<Long> timesElapsed = new ArrayList<>();

    public void add(BackfillDayStats day) {
        symbolTradeCount += day.symbolsWithTrades.size();
        symbol1mCount += day.symbolsWith1m.size();
        symbol1dCount += day.symbolsWith1d.size();

        timesElapsed.add(day.timeElapsed);
    }

    public String toString() {
        long totalMs = timesElapsed.stream().mapToLong(Long::longValue).sum();
        long averageMs = totalMs / timesElapsed.size();

        return String.format("Finished backfilling {} days in {}s (average {}s):\n{} trades\n{} 1m\n{} 1d",
                timesElapsed.size(), totalMs / 1000, averageMs / 1000,
                symbolTradeCount, symbol1mCount, symbol1dCount);
    }
}
