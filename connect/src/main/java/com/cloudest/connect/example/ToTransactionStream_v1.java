package com.cloudest.connect.example;

import com.cloudest.connect.AbstractToStream;
import com.cloudest.connect.FilterFunc;
import com.cloudest.mq.tool.ToolOptions;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by terry on 2017/6/1.
 *
 * for wosai-marketing transaction
 *
 * shell command
 *
 * nohup /app/hadoop/cloudest-connect-1.0-SNAPSHOT/bin/run-class.sh com.cloudest.connect.example.ToTransactionStream_v1 \
 * -B s1-kafka-001:9092,s1-kafka-002:9092,s1-kafka-003:9092 \
 * -O stream.upay_transaction_v1 \
 * --schema-namespace com.wosai.upay.model.dao \
 * --schema-name Transaction_v1 \
 * -G to-transaction-stream-v1 \
 * --reset \
 * -I binlog.rdsqgs2e3jze0vvflopbe.wosai-marketing.upay_transactionflowing:0
 * -S http://s1-kafka-001:8081,http://s1-kafka-002:8081,http://s1-kafka-003:8081 > to-transaction-stream-v1.log &
 *
 */
public class ToTransactionStream_v1 extends AbstractToStream {

    @Override
    protected FilterFunc createFilterFunc() {

        return new FilterFunc() {

            // 成功， 退款成功， 关闭
            private final List<String> finalStatus = new ArrayList<String>() {
                {
                    add("1");
                    add("2");
                    add("404");
                }
            };

            @Override
            public boolean apply(Object key, Object value) {

                GenericRecord after = (GenericRecord)((GenericRecord)value).get("after");
                String status = String.valueOf(after.get("status"));
                if (status != null || !status.equals(" ")) {
                    if (finalStatus.contains(status))
                        return true;
                }

                return false;
            }

        };

    }

    public static void main(String[] args){

        ToolOptions options = buildToolOptions("ToOrderStream");
        if (!options.parse(args)) {
            return;
        }
        new ToTransactionStream_v1().run(options);

    }

}
