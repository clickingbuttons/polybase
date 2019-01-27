package hbase;

import org.apache.hadoop.hbase.util.Bytes;

public enum HColumnEnum {
    TRADE_COL_FAMILY (Bytes.toBytes("t")),
    TRADE_COL_PRICE (Bytes.toBytes("p")),
    TRADE_COL_SIZE (Bytes.toBytes("v")),
    TRADE_COL_EXCHANGE (Bytes.toBytes("e")),
    TRADE_COL_CONDITION (Bytes.toBytes("c")),
    TRADE_COL_SYMBOL (Bytes.toBytes("s")),

    AGG_COL_FAMILY (Bytes.toBytes("a")),
    AGG_COL_OPEN (Bytes.toBytes("o")),
    AGG_COL_HIGH (Bytes.toBytes("h")),
    AGG_COL_LOW (Bytes.toBytes("l")),
    AGG_COL_CLOSE (Bytes.toBytes("c")),
    AGG_COL_VOLUME (Bytes.toBytes("v")),
    AGG_COL_SYMBOL (Bytes.toBytes("s"))
            ;

    public final byte[] bytes;

    HColumnEnum (byte[] bytes) {
        this.bytes = bytes;
    }
}
