package kevin.study.flink.kafka.task;

import kevin.study.flink.kafka.watermarks.KafkaAssignerWithPunctuatedWatermarks;
import kevin.study.flink.kafka.schema.KafkaWithTsMsgSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Properties;

public class KafkaWithEventTimeExample {
    public static void main(String[] args) throws Exception {
        // 用户参数获取
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // Stream 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 Event-time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Source的topic
        String sourceTopic = "Notice-Topi";
        // Sink的topic
        String sinkTopic = "Notice-Topic";
        // broker 地址
        String broker = "10.172.246.231:9092,10.172.246.232:9092,10.172.246.233:9092";

        // 属性参数 - 实际投产可以在命令行传入
        Properties p = parameterTool.getProperties();
        p.putAll(parameterTool.getProperties());
        p.put("bootstrap.servers", broker);

        env.getConfig().setGlobalJobParameters(parameterTool);
        // 创建消费者
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<Tuple2<String, Long>>(
                sourceTopic,
                new KafkaWithTsMsgSchema(),
                p);
        // 读取Kafka消息
        TypeInformation<Tuple2<String, Long>> typeInformation = new TupleTypeInfo<Tuple2<String, Long>>(
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

        env.setParallelism(1);

        DataStream<Tuple2<String, Long>> input = env
                .addSource(consumer)
                .returns(typeInformation)
                // 提取时间戳，并生产Watermark
                .assignTimestampsAndWatermarks(new KafkaAssignerWithPunctuatedWatermarks());

        // 数据处理
        DataStream<Tuple2<String, Long>> result = input
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
                .max(0);
        // 创建生产者
        FlinkKafkaProducer producer = new FlinkKafkaProducer<Tuple2<String, Long>>(
                sinkTopic,
                new KeyedSerializationSchemaWrapper<Tuple2<String, Long>>(new KafkaWithTsMsgSchema()),
                p,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
        // 将数据写入Kafka指定Topic中
        result.addSink(producer);
        // 执行job
        env.execute("Kafka With Event-time Example");
    }
}