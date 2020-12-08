package app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: app
 * ClassName: DauApp01
 *
 * @author 18729 created on date: 2020/12/8 11:55
 * @version 1.0
 * @since JDK 1.8
 */
public class DauApp01 {
    public static void main(String[] args) throws Exception {
        String kafkaBrokers = "hadoop01:9092";
        String zkBrokers = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
        String topic = "GMALL_STARTUP_0105";
        String groupId = "DAU_GROUP";

        System.out.println("===============》 flink任务开始  ==============》");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);
        properties.setProperty("zookeeper.connect", zkBrokers);
        properties.setProperty("group.id", groupId);
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), properties);
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer010);
        DataStream<String> userData = kafkaData.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) {
                System.out.println(">>>>>>接收topic报文:"+s+"<<<<<");
                return s;
            }
        });

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop01", 9200, "http"));
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        Map<String, Object> json = new HashMap<>();
                        JSONObject jsonObject = JSON.parseObject(element);

                        Long ts = jsonObject.getLong("ts");
                        String format = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts));
                        String[] s = format.split(" ");
                        jsonObject.put("dt",s[0]);
                        jsonObject.put("hr",s[1]);

                        String common = jsonObject.getString("common");
                        JSONObject jsonObject1 = JSON.parseObject(common);

                        json.put("mid",jsonObject1.getString("mid"));
                        json.put("uid",jsonObject1.getString("uid"));
                        json.put("ar",jsonObject1.getString("ar"));
                        json.put("ch",jsonObject1.getString("ch"));
                        json.put("vc",jsonObject1.getString("vc"));
                        json.put("dt",jsonObject.getString("dt"));
                        json.put("hr",jsonObject.getString("hr"));
                        json.put("mi","00");
                        json.put("ts",jsonObject.getLong("ts"));
                        System.out.println("data:"+element);

                        return Requests.indexRequest()
                                .index("gmall_dau_info_" + jsonObject.getString("dt"))
                                .type("_doc")
                                .id(jsonObject1.getString("mid"))
                                .source(json);
                    }
                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        esSinkBuilder.setBulkFlushMaxActions(1);
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders()
//                }
//        );
        esSinkBuilder.setRestClientFactory(new util.RestClientFactoryImpl());
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

        userData.addSink(esSinkBuilder.build());

        env.execute("data2es");

    }

}

