import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.BackfillSymbolTask;
import polygon.PolygonClient;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    final static PolygonClient client = new PolygonClient();
    final static Logger logger = LogManager.getLogger(Main.class);
    final static int cores = Runtime.getRuntime().availableProcessors();

    static void backfillDay(String day, List<String> symbols) {
        AtomicInteger count = new AtomicInteger(0);
        logger.info("Backfilling " + day, symbols.size());
        List<Runnable> tasks = new ArrayList<>();

        for (String sym : symbols) {
            tasks.add(new BackfillSymbolTask(day, sym, new PolygonClient(), count, symbols.size()));
        }

        ExecutorService pool = Executors.newFixedThreadPool(cores * 2);
        for (Runnable r : tasks) {
            pool.execute(r);
        }

        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }

    public static void main(String args[]) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar from, to;
        try {
            Date fromDate = sdf.parse("2019-01-01");
            Date toDate = sdf.parse("2019-01-18");

            from = Calendar.getInstance();
            from.setTime(fromDate);

            to = Calendar.getInstance();
            to.setTime(toDate);
        } catch (ParseException e) {
            logger.error(e);
            return;
        }

        long startTime = System.currentTimeMillis();

        List<String> symbols = new ArrayList<>();
        symbols.add("AAPL");
//        symbols.add("AA");
//        symbols.add("CRON");
//        List<String> symbols = client.getSymbols();
        int prevSize = symbols.size();
        symbols.removeIf(s -> s.contains(" ") || s.contains("#") || s.contains("/"));
        logger.info("Removed {} weird symbols", prevSize - symbols.size());

        while (to.after(from)) {
            backfillDay(sdf.format(to.getTime()), symbols);
            to.add(Calendar.DATE, -1);
        }
//        backfillDay("2018-02-02", symbols);

        long duration = (System.currentTimeMillis() - startTime);
        logger.info("Took {} seconds.\n", duration / 1000);
    }
}
