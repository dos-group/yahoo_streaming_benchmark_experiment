package de.tu_berlin.dos.yahoo_benchmark.flink;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AdEventTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<AdEvent> {

    public AdEventTSExtractor(int maxEventDelay) {
        super(Time.seconds(maxEventDelay));
    }

    @Override
    public long extractTimestamp(AdEvent adEvent) {

        return adEvent.getEvent_time();
    }
}
