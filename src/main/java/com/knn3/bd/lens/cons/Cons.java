package com.knn3.bd.lens.cons;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author zhouyong
 * @File Cons
 * @Time 2022/12/19 19:00
 * @Description 工程描述
 */
public class Cons {
    public static final String SEP = "\u0001";
    public static final String CURRENCY = "polygon_lens_currency";
    public static final String TREASURY = "polygon_lens_treasury";
    public static final String TREASURY_FEE = "polygon_lens_treasury_fee";
    public static final String PUBLICATION = "polygon_lens_publication";
    public static final String COLLECT = "polygon_lens_collect";

    public static Map<Integer, List<String>> FEE_MAP;
    public static Map<String, Integer> FEE_VALUE_MAP;

    static {
        FEE_MAP = new HashMap<>();
        FEE_MAP.put(1, Arrays.asList("amount", "currency", "recipient", "referralFee", "followerOnly"));
        FEE_MAP.put(2, Arrays.asList("collectLimit", "amount", "currency", "recipient", "referralFee", "followerOnly"));
        FEE_MAP.put(3, Arrays.asList("amount", "currency", "recipient", "referralFee", "followerOnly", "endTimestamp"));
        FEE_MAP.put(4, Arrays.asList("collectLimit", "amount", "currency", "recipient", "referralFee", "followerOnly", "endTimestamp"));

        FEE_VALUE_MAP = new HashMap<>();
        FEE_VALUE_MAP.put("0x1292e6df9a4697daafddbd61d5a7545a634af33d", 1);
        FEE_VALUE_MAP.put("0xef13efa565fb29cd55ecf3de2beb6c69bd988212", 2);
        FEE_VALUE_MAP.put("0xbf4e6c28d7f37c867ce62cf6ccb9efa4c7676f7f", 3);
        FEE_VALUE_MAP.put("0x7b94f57652cc1e5631532904a4a038435694636b", 4);
    }
}
