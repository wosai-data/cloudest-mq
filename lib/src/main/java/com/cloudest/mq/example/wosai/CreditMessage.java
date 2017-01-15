package com.cloudest.mq.example.wosai;


public class CreditMessage {
    public String Event;
    public String Subject;
    public String Merchant;
    public String Amount;
    public String TxSn;
    public String Flag;
    public String Gateway;
    public String Weixin_appId;

    public CreditMessage() {
    }

    public CreditMessage(TradeMessage tradeMessage) {
        this.Event = tradeMessage.event;
        this.Subject = tradeMessage.subject;
        this.Merchant = tradeMessage.merchant;
        this.Amount = tradeMessage.amount;
        this.TxSn = tradeMessage.txSn;
        this.Flag = tradeMessage.flag;
        this.Gateway = tradeMessage.gateway;
        this.Weixin_appId = tradeMessage.weixin_appid;
    }
}
