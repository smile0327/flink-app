package kevin.study.flink.app01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 20:59 2019/3/24
 * @ProjectName: flink-app
 */
public class WordCountApp {

    public static void main(String[] args) throws Exception {

        String host = "10.172.246.231";
        Integer port = 9999;

        //flink的入口类
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream(host, port, "\n");

        text.flatMap(new FlatMapFunction<String , Tuple2<String , Integer>>(){
            @Override
            public void flatMap(String s, Collector<Tuple2<String , Integer>> collector) throws Exception {
                String[] split = s.split("\\W+");
                for (String word : split) {
                    collector.collect(new Tuple2<>(word , 1));
                }
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t2, Tuple2<String, Integer> t1) throws Exception {
                return new Tuple2<>(t1.f0 , t1.f1 + t2.f1);
            }
        }).print();

        env.execute("socket word count test");


    }

}
