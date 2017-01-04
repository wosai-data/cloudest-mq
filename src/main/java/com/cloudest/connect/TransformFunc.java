package com.cloudest.connect;

public interface TransformFunc<IK, IV, OK, OV> {
    Record<OK, OV> apply(Record<IK, IV> input);
}
