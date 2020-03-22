package com.gree.bdc.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class OrderArriveTime implements Serializable {
    //订单号
    private String orderNumber;
    //客户地址
    private String customerAddress;
    //到达时间
    private String arriveTime;
    //完成时间
    private String finishTime;
}
