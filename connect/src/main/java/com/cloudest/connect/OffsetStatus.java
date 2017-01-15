package com.cloudest.connect;

public class OffsetStatus {
    private long offset;
    private boolean consumed;
    public OffsetStatus(long offset) {
        this.offset = offset;
        this.consumed = false;
    }
    public long getOffset() {
        return offset;
    }
    public void setOffset(long offset) {
        this.offset = offset;
    }
    public boolean isConsumed() {
        return consumed;
    }
    public void setConsumed(boolean consumed) {
        this.consumed = consumed;
    }
}
