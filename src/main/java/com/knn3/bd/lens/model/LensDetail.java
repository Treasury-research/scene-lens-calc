package com.knn3.bd.lens.model;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigInteger;

/**
 * @Author zhouyong
 * @File LensDetail
 * @Time 2022/12/20 12:40
 * @Description 工程描述
 */
@Setter
@Getter
@ToString
public class LensDetail {
    @JsonPropertyDescription("付款者地址")
    private String collector;
    @JsonPropertyDescription("[文章:post,评论:comment,转载:mirror]")
    private String pubType;
    @JsonPropertyDescription("作者id")
    private String proId;
    @JsonPropertyDescription("发布内容id")
    private String pubId;
    @JsonPropertyDescription("原创作者id")
    private String rootProId;
    @JsonPropertyDescription("原创内容id")
    private String rootPubId;
    @JsonPropertyDescription("收费模式[1:收费,2:限数,3:限时,4:限数限时]")
    private Integer feeType;
    @JsonPropertyDescription("收款方类型[0:原创,1:转载]")
    private Integer recipientType;
    @JsonPropertyDescription("区块数")
    private Long blkNum;
    @JsonPropertyDescription("交易hash")
    private String hash;
    @JsonPropertyDescription("log索引")
    private Integer logIdx;
    @JsonPropertyDescription("交易索引")
    private Integer idx;
    @JsonPropertyDescription("提成[转载]的收款地址")
    private String mirAddr;
    @JsonPropertyDescription("明细时间")
    private String timestamp;
    @JsonPropertyDescription("金额")
    private String amount;
    @JsonPropertyDescription("货币类型[地址]")
    private String currency;
    @JsonPropertyDescription("提成[转载]收费比例")
    private BigInteger referralFee;
    @JsonPropertyDescription("原创的收款地址")
    private String originAddr;
    private Integer decimals;
    @JsonPropertyDescription("平台地址")
    private String platAddr;
    @JsonPropertyDescription("平台收费比例")
    private Long platRate;
    @JsonPropertyDescription("平台收款金额")
    private Double platAmount;
    @JsonPropertyDescription("转载收款金额")
    private Double mirAmount;
    @JsonPropertyDescription("原创收款金额")
    private Double originAmount;
}
