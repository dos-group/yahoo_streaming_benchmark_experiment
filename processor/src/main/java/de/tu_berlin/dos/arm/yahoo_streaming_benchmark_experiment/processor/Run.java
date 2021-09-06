package de.tu_berlin.dos.arm.yahoo_streaming_benchmark_experiment.processor;

import de.tu_berlin.dos.arm.yahoo_streaming_benchmark_experiment.common.utils.FileReader;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.*;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);
    private static final int MAX_EVENT_DELAY = 20;

    public static class EventFilterBolt implements FilterFunction<AdEvent> {

        @Override
        public boolean filter(AdEvent adEvent) throws Exception {

            return adEvent.getEt().equals("view");
        }
    }

    public static class EventMapper implements MapFunction<AdEvent, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> map(AdEvent adEvent) throws Exception {

            return new Tuple2<>(adEvent.getId(), adEvent.getTs());
        }
    }

    public static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, Long>, Tuple3<String, String, Long>> {

        transient RedisAdCampaignCache redisAdCampaignCache;

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            parameterTool.getRequired("redis.host");
            LOG.info("Opening connection with Jedis to " + parameterTool.getRequired("redis.host"));
            this.redisAdCampaignCache = new RedisAdCampaignCache(parameterTool.getRequired("redis.host"));
            this.redisAdCampaignCache.prepare();
        }

        @Override
        public void flatMap(Tuple2<String, Long> input, Collector<Tuple3<String, String, Long>> out) throws Exception {

            String ad_id = input.getField(0);
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if (campaign_id == null) {
                campaign_id = "UNKNOWN";
                //return;
            }

            Tuple3<String, String, Long> tuple = new Tuple3<>(campaign_id, input.getField(0), input.getField(1));
            out.collect(tuple);
        }
    }


    public static class CampaignProcessor extends ProcessWindowFunction<Tuple3<String, String, Long>, Tuple3<String, Long, Long>, Tuple, TimeWindow> {

        private String redisServerHostname;

        @Override
        public void open(Configuration parameters) throws Exception {

            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            parameterTool.getRequired("redis.host");
            LOG.info("Opening connection with Jedis to " + parameterTool.getRequired("redis.host"));

            redisServerHostname = parameterTool.getRequired("redis.host");
        }

        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<Tuple3<String, Long, Long>> out) throws Exception {

            // get campaign key
            String campaign_id = tuple.getField(0);
            // count the number of ads for this campaign
            Iterator<Tuple3<String, String, Long>> iterator = elements.iterator();
            long count = 0;
            while (iterator.hasNext()) {
                count++;
                iterator.next();
            }
            // create output of operator
            out.collect(new Tuple3<>(campaign_id, count, context.window().getEnd()));

            // write campaign id, the ad count, the timestamp of the window to redis
            // Redis.execute(campaign_id, count, context.window().getEnd(), redisServerHostname);
        }
    }

    public static class PrepareMessage implements MapFunction<Tuple3<String, Long, Long>, String> {
        @Override
        public String map(Tuple3<String, Long, Long> value) throws Exception {
            return String.format(
                    "{campaign_id: %s, count: '%d', window: %d}",
                    value.f0, value.f1, value.f2);
        }
    }

    public static void main(final String[] args) throws Exception {

        // ensure checkpoint interval is supplied as an argument
        if (args.length != 6) {
            throw new IllegalStateException("Required Command line argument: jobName brokerList consumerTopic producerTopic partitions checkpointInterval");
        }
        String jobName = args[0];
        String brokerList = args[1];
        String consumerTopic = args[2];
        String producerTopic = args[3];
        int partitions = Integer.parseInt(args[4]);
        int checkpointInterval = Integer.parseInt(args[5]);

        // retrieve properties from file
        Properties props = FileReader.GET.read("advertising.properties", Properties.class);

        // creating map for global properties
        Map<String, String> propsMap = new HashMap<>();
        for (final String name: props.stringPropertyNames()) {
            propsMap.put(name, props.getProperty(name));
        }

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(-1);

        // setting global properties from file
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(propsMap));

        env.disableOperatorChaining();

        // configuring RocksDB state backend to use HDFS
        String backupFolder = props.getProperty("hdfs.backupFolder");
        StateBackend backend = new RocksDBStateBackend(backupFolder, true);
        env.setStateBackend(backend);

        // start a checkpoint based on supplied interval
        env.enableCheckpointing(checkpointInterval);

        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // checkpoints have to complete within two minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(3600000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        // enable externalized checkpoints which are deleted after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // no external services which could take some time to respond, therefore 1
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(100);


        // setup Kafka consumer
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", brokerList);           // Broker default host:port
        kafkaConsumerProps.setProperty("group.id", UUID.randomUUID().toString());  // Consumer group ID
        kafkaConsumerProps.setProperty("auto.offset.reset", "latest");             // Always read topic from start

        FlinkKafkaConsumer<AdEvent> myConsumer =
            new FlinkKafkaConsumer<>(
                consumerTopic,
                new AdEventSchema(),
                kafkaConsumerProps);

        // setup Kafka producer
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("bootstrap.servers", brokerList);
        kafkaProducerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "3600000");
        kafkaProducerProps.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        kafkaProducerProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        kafkaProducerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        kafkaProducerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");
        kafkaProducerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        kafkaProducerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        FlinkKafkaProducer<String> myProducer =
            new FlinkKafkaProducer<>(
                producerTopic,
                (KafkaSerializationSchema<String>) (value, aLong) -> {
                    return new ProducerRecord<>(producerTopic, value.getBytes());
                },
                kafkaProducerProps,
                Semantic.EXACTLY_ONCE);
        myProducer.setWriteTimestampToKafka(true);

        // configure event-time and watermarks
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.getConfig().setAutoWatermarkInterval(1000L);

        // assign a timestamp extractor to the consumer
        myConsumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)));

        // create direct kafka stream
        DataStream<AdEvent> messageStream =
            env.addSource(myConsumer)
                .name("DeserializeBolt")
                .setParallelism(partitions);

        messageStream
            //Filter the records if event type is "view"
            .filter(new EventFilterBolt())
            .name("EventFilterBolt")
            // project the event
            .map(new EventMapper())
            .name("project")
            // perform join with redis data
            .flatMap(new RedisJoinBolt())
            .name("RedisJoinBolt")
            // process campaign
            .keyBy(0)
            .window(TumblingEventTimeWindows.of(Time.seconds(60)))
            .process(new CampaignProcessor())
            .name("CampaignProcessor")
            .map(new PrepareMessage())
            .name("PrepareMessage")
            .addSink(myProducer)
            .name("KafkaSink-" + RandomStringUtils.random(10, true, true));

        env.execute(jobName);
    }
}
