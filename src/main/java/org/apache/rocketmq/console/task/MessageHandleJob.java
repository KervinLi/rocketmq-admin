package org.apache.rocketmq.console.task;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.console.config.RMQConfigure;
import org.apache.rocketmq.console.model.MessageVo;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@EnableScheduling
public class MessageHandleJob {
    @Resource
    private RMQConfigure configure;

    @Resource
    @Qualifier("messageProducer")
    private DefaultMQProducer messageProducer;

    //计数器
    private AtomicInteger count = new AtomicInteger(1);

    /**
     * 60S执行一次 启动延迟10秒钟执行
     * 上一次开始执行时间点之后多长时间再执行
     */
    @Scheduled(fixedRate = 60000L,initialDelay = 10000L)
    public void testJobFixedRate() throws InterruptedException, UnsupportedEncodingException {
        log.info("->TID:{}任务开始{}",Thread.currentThread().getId(), System.currentTimeMillis());
        for(int i=0;i<100;i++){
            String msgKey = UUID.randomUUID().toString();
            int andIncrement = count.getAndIncrement();
            MessageVo vo = new MessageVo();
            vo.setMsgKey(msgKey);
            vo.setSerialNumber(andIncrement);
            vo.setMsgContent("第"+andIncrement+"个消息");
            String msgVo = JSON.toJSONString(vo);
            Message message = new Message(configure.getCommonTopic(),
                    configure.getMessageTag(), msgKey,
                    msgVo.getBytes(RemotingHelper.DEFAULT_CHARSET));

            try {
                SendResult sendResult = messageProducer.send(message);
                log.info("生成者推送成功: {}", sendResult);
            } catch (MQClientException e) {
                e.printStackTrace();
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (MQBrokerException e) {
                e.printStackTrace();
            }
        }
        log.info("<-TID:{}任务结束{}",Thread.currentThread().getId(), System.currentTimeMillis());
    }
}
