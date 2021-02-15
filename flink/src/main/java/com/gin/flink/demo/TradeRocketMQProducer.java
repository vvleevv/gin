/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gin.flink.demo;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TradeRocketMQProducer {

    private static final Logger log = LoggerFactory.getLogger(TradeRocketMQProducer.class);

    private static final int MESSAGE_NUM = 10000;

    // Producer config
    private static final String NAME_SERVER_ADDR = "10.0.0.21:9876";
    private static final String PRODUCER_GROUP = "GID_SIMPLE_PRODUCER";
    private static final String TOPIC = "SOURCE_TOPIC";
    private static final String TAGS = "*";
    private static final String KEY_PREFIX = "KEY";

    private static RPCHook getAclRPCHook() {
        final String ACCESS_KEY = "${AccessKey}";
        final String SECRET_KEY = "${SecretKey}";
        return new AclClientRPCHook(new SessionCredentials(ACCESS_KEY, SECRET_KEY));
    }

    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer(
                PRODUCER_GROUP, getAclRPCHook(), true, null);
        producer.setNamesrvAddr(NAME_SERVER_ADDR);

        // When using aliyun products, you need to set up channels
        producer.setAccessChannel(AccessChannel.CLOUD);

        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < MESSAGE_NUM; i++) {
            String content = "{\"address\":\"广东省韶关市翁源县周陂镇阳东村新围8组\",\"amount\":{\"amount\":7.20,\"cent\":720,\"centFactor\":100,\"currency\":\"CNY\"},\"areaId\":105,\"areaName\":\"广东区域\",\"billOfLading\":0,\"clientType\":\"MINI_PROGRAM\",\"itemList\":[{\"afterSalePeriod\":\"48.0\",\"afterSalePeriodMeasure\":\"HOUR\",\"areaId\":105,\"barCode\":\"\",\"brandId\":37139,\"commission\":{\"amount\":0.90,\"cent\":90,\"centFactor\":100,\"currency\":\"CNY\"},\"deliveryTime\":1612598400000,\"directMining\":false,\"expiryDateEnd\":1612537200000,\"expiryDateStart\":1612454400000,\"getProduct\":false,\"isCombinedSku\":false,\"isPoints\":1,\"itemAdjustedPrice\":{\"amount\":8.99,\"cent\":899,\"centFactor\":100,\"currency\":\"CNY\"},\"itemDescription\":\"鸿津尚品 爽口墨鱼饼 240g/包 新老包装随机 240g/包 新老包装随机\",\"itemListPrice\":{\"amount\":10.90,\"cent\":1090,\"centFactor\":100,\"currency\":\"CNY\"},\"lineId\":0,\"lineSort\":0,\"logisticsAmt\":{\"amount\":0.00,\"cent\":0,\"centFactor\":100,\"currency\":\"CNY\"},\"nextDayPickUp\":true,\"orderId\":\"210205001105079330571717\",\"packingNumber\":1.0,\"platformAmt\":{\"amount\":0.89,\"cent\":89,\"centFactor\":100,\"currency\":\"CNY\"},\"presaleActivityId\":698461,\"presaleQty\":0.0,\"productId\":671737,\"productName\":\"鸿津尚品 爽口墨鱼饼 240g/包 新老包装随机\",\"productType\":\"CHOICE\",\"promotionAmt\":{\"amount\":0.00,\"cent\":0,\"centFactor\":100,\"currency\":\"CNY\"},\"qty\":1.0,\"reservedAmt\":{\"amount\":5.76,\"cent\":576,\"centFactor\":100,\"currency\":\"CNY\"},\"saleUnit\":\"包\",\"shipmentQty\":0.0,\"sku\":\"0651695\",\"skuContent\":\"240g/包 新老包装随机\",\"skuSn\":\"002473878\",\"spuSn\":\"20210204090498105474480\",\"stockOut\":false,\"storageAmt\":{\"amount\":0.00,\"cent\":0,\"centFactor\":100,\"currency\":\"CNY\"},\"storeId\":66880001237043,\"subOrderId\":\"21020500210507933249333423\",\"supplyPrice\":{\"amount\":7.20,\"cent\":720,\"centFactor\":100,\"currency\":\"CNY\"},\"thumbnailsUrl\":\"http://image.xsyxsc.com/item/20201221/VG9RUw==.jpg\",\"tmCreate\":1612487876582,\"totalCashAmt\":{\"amount\":8.99,\"cent\":899,\"centFactor\":100,\"currency\":\"CNY\"},\"totalTicketAmt\":{\"amount\":0.00,\"cent\":0,\"centFactor\":100,\"currency\":\"CNY\"},\"unionPayMid\":\"\",\"userId\":568810000562563793,\"userLimitQty\":0.0,\"vendorAddress\":\"珠海市金湾区红旗镇小林红灯围1厂房2楼西面、3楼\",\"vendorCode\":\"440404200001\",\"vendorId\":76880000135281,\"vendorName\":\"珠海市欣扬食品有限公司\",\"vendorShortName\":\"欣扬食品\",\"vendorTelephone\":\"15917354526\",\"volume\":0.0,\"warehouseFee\":{\"amount\":0.00,\"cent\":0,\"centFactor\":100,\"currency\":\"CNY\"},\"warehouseId\":0,\"warehouseName\":\"\",\"warehouseType\":\"\",\"weight\":0.0}],\"logisticsAmt\":{\"amount\":0.00,\"cent\":0,\"centFactor\":100,\"currency\":\"CNY\"},\"memberIsShow\":true,\"openId\":\"oIu8u5etp6R3E3tszj3sFV8f8UL0\",\"orderDate\":1612487876552,\"orderId\":\"210205001105079330571717\",\"orderStatus\":\"NEED_PAY\",\"orderTotal\":{\"amount\":8.99,\"cent\":899,\"centFactor\":100,\"currency\":\"CNY\"},\"orderType\":\"CHOICE\",\"payChannel\":\"\",\"payType\":\"ONLINE\",\"phone\":\"13927843694\",\"platformAmt\":{\"amount\":0.89,\"cent\":89,\"centFactor\":100,\"currency\":\"CNY\"},\"presale\":true,\"promotionAmt\":{\"amount\":0.00,\"cent\":0,\"centFactor\":100,\"currency\":\"CNY\"},\"receiver\":\"心如止水\",\"storageAmt\":{\"amount\":0.00,\"cent\":0,\"centFactor\":100,\"currency\":\"CNY\"},\"storeAddress\":\"广东省韶关市翁源县周陂镇阳东村新围8组\",\"storeId\":66880001237043,\"storeIsShow\":true,\"storeName\":\"许景有小卖部\",\"storeNo\":\"440229100365\",\"storeTel\":\"13927843694\",\"ticketStatus\":\"\",\"totalCashAmt\":{\"$ref\":\"$.orderTotal\"},\"totalCommission\":{\"amount\":0.90,\"cent\":90,\"centFactor\":100,\"currency\":\"CNY\"},\"totalQty\":1.0,\"totalTicketAmt\":{\"amount\":0.00,\"cent\":0,\"centFactor\":100,\"currency\":\"CNY\"},\"userId\":568810000562563793,\"userName\":\"13927843694\",\"valetOrder\":false,\"warehouseId\":0,\"warehouseName\":\"\",\"wechatImage\":\"http://image.xsyxsc.com/user/newHeadImage/5ef9d13174272a68fe33290fed37c50564b0e137da439a8a\",\"wechatName\":\"心如止水\"}";
            Message msg = new Message(TOPIC, TAGS, KEY_PREFIX + i, content.getBytes());
            try {
                SendResult sendResult = producer.send(msg);
                assert sendResult != null;
                System.out.printf("send result: %s %s\n",
                        sendResult.getMsgId(), sendResult.getMessageQueue().toString());
                Thread.sleep(3000);
            } catch (Exception e) {
                log.info("send message failed. {}", e.toString());
            }
        }
    }
}
