package org.apache.rocketmq.console.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

@Slf4j
@Configuration
public class MessageMqProducer {
    @Resource
    private RMQConfigure configure;

    @Bean
    //@ConditionalOnMissingBean
    public DefaultMQProducer messageProducer() {
        DefaultMQProducer producer = new DefaultMQProducer(configure.getProducerGroup());
        producer.setNamesrvAddr(configure.getNamesrvAddr());
        producer.setInstanceName("message_producer");
        //producer.setMaxMessageSize(maxMessageSize);
        //producer.setSendMsgTimeout(sendMsgTimeout);
        //producer.setVipChannelEnabled(false);
        producer.setRetryTimesWhenSendAsyncFailed(5);
        try {
            producer.start();
            log.info("================>消息生产者创建完成，ProducerGroupName: {}<================", configure.getProducerGroup());
        } catch (MQClientException e) {
            log.error("failed to start producer.", e);
            throw new RuntimeException(e);
        }

        return producer;
    }
}
