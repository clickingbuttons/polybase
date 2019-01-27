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
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    final static Logger logger = LogManager.getLogger(Main.class);
    final static int threadCount = 50;
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    final static PolygonClient client = new PolygonClient();


    static void backfillDay(Calendar day, List<String> symbols) {
        if (day.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
            return;
        }
        AtomicInteger count = new AtomicInteger(0);
        AtomicInteger success = new AtomicInteger(0);
        logger.info("Backfilling {} symbols on {} using {} threads", symbols.size(), sdf.format(day.getTime()), threadCount);
        List<Runnable> tasks = new ArrayList<>();

        for (String sym : symbols) {
            tasks.add(new BackfillSymbolTask(day, sym, count, success, symbols.size()));
        }

        // Only about 1/4 of 30770 the symbols have trades and will actually use CPU
        // The rest are just doing HTTP requests to check
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
        logger.info("Backfilled {} / {} symbols on {}", success.get(), symbols.size(), sdf.format(day.getTime()));
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
        while (to.after(from)) {
            long startTime = System.currentTimeMillis();
            backfillDay(to, strings);
            to.add(Calendar.DATE, -1);
            long duration = (System.currentTimeMillis() - startTime);
            logger.info("{} took {} seconds", sdf.format(to.getTime()), duration / 1000);
        }
    }

    public static void main(String args[]) {
        Calendar from, to;
        try {
            Date fromDate = sdf.parse("2019-01-02");
            Date toDate = sdf.parse("2019-01-25"); // new Date()

            from = Calendar.getInstance();
            from.setTime(fromDate);

            to = Calendar.getInstance();
            to.setTime(toDate);
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        long startTime = System.currentTimeMillis();
        initTables();
        backfill(from, to);
        long duration = (System.currentTimeMillis() - startTime);
        logger.info("Took {} seconds", duration / 1000);
        HBaseWriter.close();
    }
}
