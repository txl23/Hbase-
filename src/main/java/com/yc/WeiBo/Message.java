package com.yc.WeiBo;

import lombok.Data;

/**
 * @program: Hbase
 * @description:
 * @author: 汤僖龙
 * @create: 2021-08-13 10:14
 */
@Data
public class Message {
    private String uid;
    private String timestamp;
    private String content;
}
