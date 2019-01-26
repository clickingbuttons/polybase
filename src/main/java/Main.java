import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.PolygonClient;
import polygon.models.Symbol;

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
import java.util.stream.Collectors;

public class Main {
    final static Logger logger = LogManager.getLogger(Main.class);
    final static int cores = Runtime.getRuntime().availableProcessors();
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    final static PolygonClient client = new PolygonClient();


    static void backfillDay(Calendar day, List<String> symbols) {
        AtomicInteger count = new AtomicInteger(0);
        logger.info("Backfilling {} ({} symbols)", sdf.format(day.getTime()), symbols.size());
        List<Runnable> tasks = new ArrayList<>();

        for (String sym : symbols) {
            tasks.add(new BackfillSymbolTask(day, sym, count, symbols.size()));
        }

        ExecutorService pool = Executors.newFixedThreadPool(cores * 2);
        for (Runnable r : tasks) {
            pool.execute(r);
        }

        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
        if (s.symbol.endsWith("USDT")
            || s.symbol.matches("^.*[^a-zA-Z].*$") // These don't work with Polygon most of the time anyways...
            || type.contains("warrant")
            || type.contains("rights")
            || type.contains("issued")) {
            logger.debug("Removed {}", s);
            return true;
        }

        return false;
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
//        logger.debug("Symbols: {}", symbols);

        List<String> strings = loadSymbols();
        while (to.after(from)) {
            backfillDay(to, strings);
            to.add(Calendar.DATE, -1);
        }
    }

    public static void main(String args[]) {
        Calendar from, to;
        try {
            Date fromDate = sdf.parse("2019-01-02");
            Date toDate = sdf.parse("2019-01-24"); // new Date()

            from = Calendar.getInstance();
            from.setTime(fromDate);

            to = Calendar.getInstance();
            to.setTime(toDate);
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        long startTime = System.currentTimeMillis();
        HBaseWriter.getInstance().createTable("trade", "tf");
        HBaseWriter.getInstance().createTable("agg1m", "af");
        HBaseWriter.getInstance().createTable("agg5m", "af");
        HBaseWriter.getInstance().createTable("agg1h", "af");
        HBaseWriter.getInstance().createTable("agg1d", "af");
        backfill(from, to);
        long duration = (System.currentTimeMillis() - startTime);
        logger.info("Took {} seconds.\n", duration / 1000);
    }
}
