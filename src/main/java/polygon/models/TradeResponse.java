package polygon.models;

import java.util.List;
import java.util.Map;

public class TradeResponse {
    public String day;
    public Map<String, String> map;
    public int msLatency;
    public String status;
    public String symbol;
    public List<Trade> ticks;
    public String type;
}
