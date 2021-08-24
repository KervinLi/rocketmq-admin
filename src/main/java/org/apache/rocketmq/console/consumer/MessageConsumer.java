package org.apache.rocketmq.console.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.console.config.RMQConfigure;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
@SpringBootApplication
public class MessageConsumer implements ApplicationRunner {

    @Resource
    private RMQConfigure configure;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(configure.getConsumerGroup());
        consumer.setConsumeThreadMax(2);
        consumer.setConsumeThreadMin(1);

        // Specify name server addresses.
        consumer.setNamesrvAddr(configure.getNamesrvAddr());

        // Subscribe one more more topics to consume.
        consumer.subscribe(configure.getCommonTopic(), "*");

        consumer.setInstanceName("messageConsumer");

        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                String msg = new String(msgs.get(0).getBody(), StandardCharsets.UTF_8);
                String id = msgs.get(0).getMsgId();
                String key = msgs.get(0).getKeys();
                try {
                    //模拟1秒消费一个消息
                    Thread.sleep(1000);
                    log.info("消费消息数据成功：key:{},body:{}",key, msg);
                } catch (Exception e) {
                    log.error("消息消费失败{}，将再次订阅：{}", id, e.getMessage());
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //Launch the consumer instance.
        consumer.start();
        log.info("消息消费者监听 启动完成！");
    }
}
