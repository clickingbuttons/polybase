package polygon.models;

import common.OHLCV;

import java.util.List;
import java.util.Map;

public class AggResponse {
    public String symbol;
    public String aggType;
    public Map<String, String> map;
    public List<OHLCV> ticks;
}
