package polygon;

import com.google.common.util.concurrent.RateLimiter;
import polygon.models.OHLCV;
import com.google.gson.Gson;
import polygon.models.Trade;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import polygon.models.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class PolygonClient {
    static String baseUrl = "https://api.polygon.io/v1";
    static String apiKey = System.getenv("POLYGON_KEY");
    static Gson gson = new Gson(); // Thread safe
    final static Logger logger = LogManager.getLogger(PolygonClient.class);
    // Not ideal, but we start getting 500s
    final static RateLimiter rateLimiter = RateLimiter.create(300);

    private static String doRequest(String uri) {
        StringBuffer content;
        for (int i = 0; i < 10; i++) {
            try {
                rateLimiter.acquire();
                URL url = new URL(uri);
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestProperty("Accept", "application/json");
                con.setRequestMethod("GET");
                con.setDoOutput(true);

                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                con.disconnect();

                int code = con.getResponseCode();
                if (code == 200)
                    return content.toString();
                Thread.sleep((i + 1) * (i + 1) * 1000);
            } catch (MalformedURLException|ProtocolException|InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                if (!e.getMessage().contains("500"))
                    e.printStackTrace();
            }
        }

        logger.error("Retries exceeded for request {}", uri);
        return null;
    }

    public static List<Trade> getTradesForSymbol(String day, String symbol) {
        List<Trade> trades = new ArrayList<>();

        long offset = 0;
        while(true) {
            String url = String.format("%s/historic/trades/%s/%s?apiKey=%s&limit=10000&offset=%d",
                    baseUrl, symbol, day, apiKey, offset);
            // logger.debug(url);
            String content = doRequest(url);
            if (content == null)
                return null;
            TradeResponse r = gson.fromJson(content, TradeResponse.class);
            if (r.ticks == null) // Last page
                return trades;
            trades.addAll(r.ticks);
            offset = r.ticks.get(r.ticks.size() - 1).timeMillis;
        }
    }

    public static List<OHLCV> getHistMinutesForSymbol(Calendar day, String symbol) {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        int limit = 60 * 24;
        String url = String.format("%s/historic/agg/minute/%s?apiKey=%s&limit=%d&from=%s",
                baseUrl, symbol, apiKey, limit, sdf.format(day.getTime()));
//        logger.debug(url);
        String content = doRequest(url);
        if (content == null)
            return null;
        AggResponse r = gson.fromJson(content, AggResponse.class);
        return r.ticks;
    }

    public static OHLCV getHistDayForSymbol(Calendar day, String symbol) {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar nextDay = (Calendar) day.clone();
        nextDay.add(Calendar.DATE, 1);

        int limit = 60 * 60 * 24;
        String url = String.format("%s/historic/agg/day/%s?apiKey=%s&limit=%d&from=%s&to=%s",
                baseUrl, symbol, apiKey, limit, sdf.format(nextDay.getTime()), sdf.format(nextDay.getTime()));
//        logger.debug(url);
        String content = doRequest(url);
        AggResponse r = gson.fromJson(content, AggResponse.class);
        if (r.ticks.size() > 0)
            return r.ticks.get(0);

        return null;
    }

    public static List<Symbol> getSymbols() {
        List<Symbol> symbols = new ArrayList<>();

        int perPage = 10000;
        for (int page = 1;; page++) {
            String url = String.format("%s/meta/symbols?apiKey=%s&sort=symbol&perpage=%d&page=%d",
                    baseUrl, apiKey, perPage, page);
            String content = doRequest(url);
            if (content == null)
                return null;
            SymbolResponse r = gson.fromJson(content, SymbolResponse.class);
            for (Symbol s : r.symbols) {
                symbols.add(s);
            }

            if (r.symbols.size() < perPage) // Last page
                return symbols;
        }
    }
}
