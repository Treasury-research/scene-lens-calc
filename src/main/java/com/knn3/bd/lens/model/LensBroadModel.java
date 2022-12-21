package com.knn3.bd.lens.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @Author zhouyong
 * @File LensBroadModel
 * @Time 2022/12/21 16:18
 * @Description 工程描述
 */
@Setter
@Getter
@ToString
public class LensBroadModel {
    private Long timestamp;
    /**
     * polygon_lens_currency
     */
    private String currency;
    private Integer decimals;

    private Integer transactionIndex;
    private Long blockNumber;
    /**
     * polygon_lens_treasury
     */
    private String newTreasury;
    /**
     * polygon_lens_treasury_fee
     */
    private Long newTreasuryFee;
}
