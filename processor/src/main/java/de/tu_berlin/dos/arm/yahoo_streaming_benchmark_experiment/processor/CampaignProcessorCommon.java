package de.tu_berlin.dos.arm.yahoo_streaming_benchmark_experiment.processor;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class CampaignProcessorCommon {
    private static final Logger LOG = Logger.getLogger(CampaignProcessorCommon.class);
    private Jedis jedis;
    private Jedis flush_jedis;
    private Long lastWindowMillis;
    // Bucket -> Campaign_id -> Window
    private LRUHashMap<Long, HashMap<String, Window>> campaign_windows;
    private Set<CampaignWindowPair> need_flush;

    private long processed = 0;

    private static final Long time_divisor = 10000L; // 10 second windows

    public CampaignProcessorCommon(String redisServerHostname) {
        jedis = new Jedis(redisServerHostname);
        flush_jedis = new Jedis(redisServerHostname);
    }

    public void prepare() {

        campaign_windows = new LRUHashMap<>(10);
        lastWindowMillis = System.currentTimeMillis();
        need_flush = new HashSet<>();

        Runnable flusher = new Runnable() {
            public void run() {
                try {
                    while (true) {
                        Thread.sleep(1000);
                        flushWindows();
                        lastWindowMillis = System.currentTimeMillis();
                    }
                } catch (InterruptedException e) {
                    LOG.error("Interrupted", e);
                }
            }
        };
        new Thread(flusher).start();
    }

    public void execute(String campaign_id, String event_time) {
        Long timeBucket = Long.parseLong(event_time) / time_divisor;
        Window window = getWindow(timeBucket, campaign_id);
        window.seenCount++;

        CampaignWindowPair newPair = new CampaignWindowPair(campaign_id, window);
        synchronized(need_flush) {
            need_flush.add(newPair);
        }
        processed++;
    }

    private void writeWindow(String campaign, Window win) {
        String windowUUID = flush_jedis.hmget(campaign, win.timestamp).get(0);
        if (windowUUID == null) {
            windowUUID = UUID.randomUUID().toString();
            flush_jedis.hset(campaign, win.timestamp, windowUUID);

            String windowListUUID = flush_jedis.hmget(campaign, "windows").get(0);
            if (windowListUUID == null) {
                windowListUUID = UUID.randomUUID().toString();
                flush_jedis.hset(campaign, "windows", windowListUUID);
            }
            flush_jedis.lpush(windowListUUID, win.timestamp);
        }

        synchronized (campaign_windows) {
            flush_jedis.hincrBy(windowUUID, "seen_count", win.seenCount);
            win.seenCount = 0L;
        }
        flush_jedis.hset(windowUUID, "time_updated", Long.toString(System.currentTimeMillis()));
        flush_jedis.lpush("time_updated", Long.toString(System.currentTimeMillis()));
    }

    public void flushWindows() {
        synchronized (need_flush) {
            for (CampaignWindowPair pair : need_flush) {
                writeWindow(pair.campaign, pair.window);
            }
            need_flush.clear();
        }
    }

    public static Window redisGetWindow(Long timeBucket, Long time_divisor) {

        Window win = new Window();
        win.timestamp = Long.toString(timeBucket * time_divisor);
        win.seenCount = 0L;
        return win;
    }

    // Needs to be rewritten now that redisGetWindow has been simplified.
    // This can be greatly simplified.
    public Window getWindow(Long timeBucket, String campaign_id) {
        synchronized (campaign_windows) {
            HashMap<String, Window> bucket_map = campaign_windows.get(timeBucket);
            if (bucket_map == null) {
                // Try to pull from redis into cache.
                Window redisWindow = redisGetWindow(timeBucket, time_divisor);
                if (redisWindow != null) {
                    bucket_map = new HashMap<String, Window>();
                    campaign_windows.put(timeBucket, bucket_map);
                    bucket_map.put(campaign_id, redisWindow);
                    return redisWindow;
                }

                // Otherwise, if nothing in redis:
                bucket_map = new HashMap<String, Window>();
                campaign_windows.put(timeBucket, bucket_map);
            }

            // Bucket exists. Check the window.
            Window window = bucket_map.get(campaign_id);
            if (window == null) {
                // Try to pull from redis into cache.
                Window redisWindow = redisGetWindow(timeBucket, time_divisor);
                if (redisWindow != null) {
                    bucket_map.put(campaign_id, redisWindow);
                    return redisWindow;
                }

                // Otherwise, if nothing in redis:
                window = new Window();
                window.timestamp = Long.toString(timeBucket * time_divisor);
                window.seenCount = 0L;
                bucket_map.put(campaign_id, redisWindow);
            }
            return window;
        }
    }
}
