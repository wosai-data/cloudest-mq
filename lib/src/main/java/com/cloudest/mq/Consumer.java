package com.cloudest.mq;

import java.util.List;

public interface Consumer {
 
    /**
     * 和消息中心建立连接，加入分组，订阅消息频道。broker连接信息属于实现类逻辑的一部分，不在接口暴露。
     * @param channel
     * @param group
     */
    void subscribe(String channel, String group);
 
    /**
     * 同步读取Consumer应该看到的消息，如果没有消息，则阻塞。
     * @return
     */
    List<Message> get();
 
    /**
     * 确认消息处理成功。Kafka的消息是严格有序的，所以会隐含确认之前收到的（同一个分区下面）所有消息，所以如果又多线程并发处理多条消息，业务代码必须确保当前消息之前所有的消息都已经处理完成。
     * @param message
     */
    void ack(Message message);
 
    /**
     * 优雅断开连接
     *
     */
    void close();
}
