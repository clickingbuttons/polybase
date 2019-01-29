import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.PolygonClient;
import polygon.models.Symbol;
import hbase.HBaseWriter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    final static Logger logger = LogManager.getLogger(Main.class);
    final static int threadCount = 50;
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    final static PolygonClient client = new PolygonClient();


    static BackfillDayStats backfillDay(Calendar day, List<String> symbols) {
        BackfillDayStats res = new BackfillDayStats(day, symbols.size());
        List<Runnable> tasks = new ArrayList<>();

        for (String sym : symbols) {
            tasks.add(new BackfillSymbolTask(day, sym, res));
        }

        // Only about 1/4 of 30770 symbols have trades and will actually use CPU,
        // and the rest are just doing HTTP requests to check
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        for (Runnable r : tasks) {
            pool.execute(r);
        }

        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return res;
    }

    public static void saveSymbols(List<String> symbols) {
        try {
            BufferedWriter outputWriter = new BufferedWriter(new FileWriter("symbols.txt"));
            outputWriter.write(String.join(",", symbols));
            outputWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<String> loadSymbols() {
        try {
            String content = new String(Files.readAllBytes(Paths.get("symbols.txt")));
            return new java.util.ArrayList<>(Arrays.asList(content.split(",")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static boolean isBadSymbol(Symbol s) {
        String type = s.type == null ? "" : s.type.toLowerCase();
        if (s.symbol.length() > 5
                || s.symbol.matches("^.*[^a-zA-Z].*$") // These don't work with Polygon most of the time anyways...
                || type.contains("warrant")
                || type.contains("rights")
                || type.contains("issued")) {
            logger.debug("Removed {}", s);
            return true;
        }

        return false;
    }

    private static void initTables() {
        HBaseWriter.createTable("trade", "t");
        HBaseWriter.createTable("agg1s", "a");
        HBaseWriter.createTable("agg1m", "a");
        HBaseWriter.createTable("agg5m", "a");
        HBaseWriter.createTable("agg1h", "a");
        HBaseWriter.createTable("agg1d", "a");
    }

    private static void backfill(Calendar from, Calendar to) {
//        List<String> strings = new ArrayList<>();
//        strings.add("AAPL");
//        strings.add("AAU");
//        strings.add("CRON");
//        strings.add("AADR");
//        strings.add("AAAU");
//        strings.add("AAMC");
//        strings.add("ACES");
//        strings.add("ACGLO");
//        strings.add("ACT");
//        strings.add("ADRD");

//        logger.info("Loading symbols...");
//        List<Symbol> symbols = client.getSymbols();
//        int prevSize = symbols.size();
//        symbols.removeIf(Main::isBadSymbol);
//        List<String> strings = symbols.stream().map(s -> s.symbol).collect(Collectors.toList());
//        saveSymbols(strings);
//        logger.info("Removed {} weird symbols. {} left.", prevSize - symbols.size(), symbols.size());
//        logger.debug("Symbols: {}", strings);

        List<String> strings = loadSymbols();

        BackfillAllStats allStats = new BackfillAllStats();
        while (to.after(from)) {
            logger.info("Backfilling {} symbols on {}", strings.size(), sdf.format(to.getTime()));

            long startTime = System.currentTimeMillis();
            BackfillDayStats dayStats = backfillDay(to, strings);
            dayStats.timeElapsed = System.currentTimeMillis() - startTime;

            logger.info(dayStats);
            allStats.add(dayStats);

            to.add(Calendar.DATE, -1);
        }
    }

    public static void main(String args[]) {
        Calendar from = Calendar.getInstance();
        Calendar to = Calendar.getInstance();
        try {
            Date fromDate = sdf.parse("2019-01-01");
            Date toDate = sdf.parse("2019-01-25"); // new Date()

            from.setTime(fromDate);
            to.setTime(toDate);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        long startTime = System.currentTimeMillis();
        initTables();
        backfill(from, to);
        long duration = (System.currentTimeMillis() - startTime);
        logger.info("Took {} seconds", duration / 1000);
        HBaseWriter.close();
    }
}
