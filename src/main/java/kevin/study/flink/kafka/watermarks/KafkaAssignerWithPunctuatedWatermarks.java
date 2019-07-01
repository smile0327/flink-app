package kevin.study.flink.kafka.watermarks;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class KafkaAssignerWithPunctuatedWatermarks
        implements AssignerWithPunctuatedWatermarks<Tuple2<String, Long>> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Tuple2<String, Long> o, long l) {
        // 利用提取的时间戳创建Watermark
        return new Watermark(l);
    }

    @Override
    public long extractTimestamp(Tuple2<String, Long> o, long l) {
       // 提取时间戳
        return o.f1;
    }
}