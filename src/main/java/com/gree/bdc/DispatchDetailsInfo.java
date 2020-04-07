package com.gree.bdc;


import com.gree.bdc.entity.OrderArriveTime;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

import java.io.PrintStream;
import java.util.*;

/**
 * @author hadoop
 */
public class DispatchDetailsInfo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("dispatch-details-handle")
                .master("local")
                .getOrCreate();

        //创建dataframe
        // Create an instance of a Bean class
        OrderArriveTime orderArriveTime1 = new OrderArriveTime();
        orderArriveTime1.setOrderNumber("order_1");
        orderArriveTime1.setCustomerAddress("康乐园");
        orderArriveTime1.setArriveTime("2020-03-04 12:12:20");
        orderArriveTime1.setFinishTime("2020-03-04 12:22:20");
        OrderArriveTime orderArriveTime2 = new OrderArriveTime();
        orderArriveTime2.setOrderNumber("order_2");
        orderArriveTime2.setCustomerAddress("康乐园");
        orderArriveTime2.setArriveTime("2020-03-04 12:12:20");
        orderArriveTime2.setFinishTime("2020-03-04 12:40:20");
        OrderArriveTime orderArriveTime3 = new OrderArriveTime();
        orderArriveTime3.setOrderNumber("order_3");
        orderArriveTime3.setCustomerAddress("康乐园");
        orderArriveTime3.setArriveTime("2020-03-04 12:12:20");
        orderArriveTime3.setFinishTime("2020-03-04 13:40:20");
        OrderArriveTime orderArriveTime4 = new OrderArriveTime();
        orderArriveTime4.setOrderNumber("order_4");
        orderArriveTime4.setCustomerAddress("清苑");
        orderArriveTime4.setArriveTime("2020-03-04 12:12:20");
        orderArriveTime4.setFinishTime("2020-03-04 13:22:20");


        // Encoders are created for Java beans
        Encoder<OrderArriveTime> personEncoder = Encoders.bean(OrderArriveTime.class);
        Dataset<OrderArriveTime> javaBeanDS = spark.createDataset(
                Arrays.asList(orderArriveTime1, orderArriveTime2, orderArriveTime3, orderArriveTime4),
                personEncoder
        );
        javaBeanDS.show();

        Dataset<OrderArriveTime> orderArriveTimeDataSet = javaBeanDS.groupByKey(new MapFunction<OrderArriveTime, String>() {
            @Override
            public String call(OrderArriveTime orderArriveTime) throws Exception {
                return orderArriveTime.getCustomerAddress();
            }
        }, Encoders.STRING()).flatMapGroups(new FlatMapGroupsFunction<String, OrderArriveTime, OrderArriveTime>() {
            @Override
            public Iterator<OrderArriveTime> call(String s, Iterator<OrderArriveTime> iterator) throws Exception {

                String arriveTime = "";
                String finishTime = "";

                List<OrderArriveTime> orderArriveTimeList = new ArrayList<>();
                while (iterator.hasNext()) {
                    OrderArriveTime orderArriveTime = new OrderArriveTime();
                    OrderArriveTime next = iterator.next();
                    if (arriveTime.equals("")) {
                        arriveTime = next.getArriveTime();
                    } else {
                        arriveTime = finishTime;
                    }
                    finishTime = next.getFinishTime();
                    orderArriveTime.setOrderNumber(next.getOrderNumber());
                    orderArriveTime.setCustomerAddress(next.getCustomerAddress());
                    orderArriveTime.setArriveTime(arriveTime);
                    orderArriveTime.setFinishTime(finishTime);
                    System.out.println(orderArriveTime);
                    orderArriveTimeList.add(orderArriveTime);
                }

                return orderArriveTimeList.iterator();
            }
        }, Encoders.bean(OrderArriveTime.class));
        orderArriveTimeDataSet.show();

        //需要实现的逻辑为：
        //根据客户地址进行分组，然后根据完成时间排序，完成时间为第一的则到达时间为现有到达时间加三分钟，
        //完成时间第二的到达时间为第一的完成时间加三分钟，以此类推得到每个订单的真实到达时间
          //JavaRDD<OrderArriveTime> orderArriveTimeJavaRDD = javaBeanDS.javaRDD();

//        JavaRDD<Object> map = orderArriveTimeJavaRDD.groupBy(OrderArriveTime::getCustomerAddress)
//                .map(new Function<Tuple2<String, Iterable<OrderArriveTime>>, Object>() {
//
//                    @Override
//                    public Object call(Tuple2<String, Iterable<OrderArriveTime>> stringIterableTuple2) throws Exception {
//                        String customerAddress = stringIterableTuple2._1;
//                        Iterator<OrderArriveTime> iterator = stringIterableTuple2._2.iterator();
//                        String arriveTime = "";
//                        String finishTime = "";
//
//                        List list = new ArrayList();
//                        while (iterator.hasNext()) {
//                            OrderArriveTime orderArriveTime = new OrderArriveTime();
//                            OrderArriveTime next = iterator.next();
//                            if (arriveTime.equals("")) {
//                                arriveTime = next.getArriveTime();
//                            } else {
//                                arriveTime = finishTime;
//                            }
//                            finishTime = next.getFinishTime();
//                            orderArriveTime.setOrderNumber(next.getOrderNumber());
//                            orderArriveTime.setCustomerAddress(next.getCustomerAddress());
//                            orderArriveTime.setArriveTime(arriveTime);
//                            orderArriveTime.setFinishTime(finishTime);
//
//                            list.add(orderArriveTime);
//
//                        }
//                        Iterator iterator1 = list.iterator();
//                        Object next = null;
//                        while (iterator1.hasNext()) {
//                             next = iterator1.next();
//                        }
//                        return next;
//                    }
//
//                });



//        map.collect().forEach(ars ->{
//            System.out.println(ars.toString());
//        });


//        ArrayList list = new ArrayList();
//        orderArriveTimeJavaRDD.groupBy(OrderArriveTime::getCustomerAddress).foreach(
//
//                new VoidFunction<Tuple2<String, Iterable<OrderArriveTime>>>() {
//
//                    @Override
//                    public void call(Tuple2<String, Iterable<OrderArriveTime>> stringIterableTuple2) throws Exception {
//                        String customerAddress = stringIterableTuple2._1;
//                        Iterator<OrderArriveTime> iterator = stringIterableTuple2._2.iterator();
//                        String arriveTime = "";
//                        String finishTime = "";
//                        OrderArriveTime orderArriveTime = new OrderArriveTime();
//                        while (iterator.hasNext()) {
//
//                            OrderArriveTime next = iterator.next();
//                            if (arriveTime.equals("")) {
//                                arriveTime = next.getArriveTime();
//                            } else {
//                                arriveTime = finishTime;
//                            }
//                            finishTime = next.getFinishTime();
//                            orderArriveTime.setOrderNumber(next.getOrderNumber());
//                            orderArriveTime.setCustomerAddress(next.getCustomerAddress());
//                            orderArriveTime.setArriveTime(arriveTime);
//                            orderArriveTime.setFinishTime(finishTime);
//                            list.add(orderArriveTime);
//
//                        }
//
//                        System.out.println("1" + orderArriveTime.toString());
//                    }
//
//                });
//
//
//
//        spark.createDataset(list,personEncoder).show();


    }
}
