package com.cloudest.mq.example.wosai;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TradeMessage {
    public String event;
    public String weixin_appid;
    public String subject;
    public String originAmount;
    public String gateway;
    public String merchant;
    public String amount;
    public String txSn;
    public String tsn;
    public Long order_ctime;
    public String store_id;
    public String merchant_id;
    public String operator;
    public Integer provider;
    public String flag;

    public TradeMessage() {
        this.event = "customer-checkout";
    }

    // for test
    @JsonIgnore
    public boolean isRevokeMessage() {
        return event != null && event.equals("customer-revoke");
    }

    // for test
    public void setRevoke() {
        event = "customer-revoke";
    }
}
