package com.cloudest.mq;


public interface Message {
 
    byte[] getKey();
    byte[] getValue();
    Object getMeta(String metaKey);

}