package org.apache.rocketmq.console.model;

import lombok.Data;

@Data
public class MessageVo {
    /**
     * 序列号
     */
    private Integer serialNumber;
    /**
     * 消息Key
     */
    private String msgKey;
    /**
     * 消息内容
     */
    private String msgContent;
}
