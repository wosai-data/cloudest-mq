package com.cloudest.mq;
 
public interface Producer {
    /**
     * 同步往channel发送消息。成功返回后，消息会保证持久化保存到channel中。
     * @param channel 频道名称，必须预先创建好，否则此方法调用的结果未定义。
     * @param key （key，value）构成消息内容
     * @param value  （key，value）构成消息内容
     * @throws MessagingException
     */
    void post(String channel, byte[] key, byte[] value) throws MessagingException;
 
 
    /**
     * 优雅断开连接
     *
     */
    void close();
}