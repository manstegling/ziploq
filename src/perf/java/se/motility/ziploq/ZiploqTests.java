package se.motility.ziploq;

import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.Ziploq;
import se.motility.ziploq.testapi.Producer;
import se.motility.ziploq.testapi.ProducerState;

public class ZiploqTests {

    private static final Logger LOG = LoggerFactory.getLogger(ZiploqTests.class);
    
    private ZiploqTests() {
        throw new UnsupportedOperationException("Do not instantiate utility class.");
    }

    public static long performTakeTest(ProducerState state) throws InterruptedException {
        Comparator<Object> cmp = state.comparator();
        for(Producer s : state.producers()) {
            s.getThread().start();
        }
        long max = 0;
        Entry<Object> entry;
        Object last = null;
        while ((entry = state.ziploq().take()) != Ziploq.getEndSignal()) {
            long ts = entry.getBusinessTs();
            if (ts < max) {
                throw new IllegalStateException("Last timestamp:" + max + ", new timestamp: " + ts);
            }
            if (cmp != null && cmp.compare(last, entry.getMessage()) > 0) {
                throw new IllegalStateException("Message " + entry.getMessage() + " came after " + last);
            }
            max = ts;
            last = entry.getMessage();
        }
        for(Producer t : state.producers()) {
            t.getThread().join();
        }
        LOG.info("Iteration completed successfully. Last timestamp: {}", max);
        return max;
    }
    
    public static long performStreamTest(ProducerState state) throws InterruptedException {
        for(Producer s : state.producers()) {
            s.getThread().start();
        }
        long max = state.ziploq().stream()
                        .mapToLong(Entry::getBusinessTs)
                        .reduce(0L, (l1,l2) -> (l1 >= l2) ? l1 : l2);
        for(Producer t : state.producers()) {
            t.getThread().join();
        }
        LOG.info("Iteration completed successfully. Last timestamp: {}", max);
        return max;
    }
    
    public enum WaitMode {
        /**
         * Using blocking consumer. Assumed to be standard case.
         */
        BLOCK(BackPressureStrategy.BLOCK, Thread::yield),
        /**
         * Using dropping consumer but re-tries immediately
         * (after yield) to insert message on fail. Shows that
         * busy spinning degrades performance.
         */
        YIELD(BackPressureStrategy.DROP, Thread::yield),
        /**
         * Using dropping consumers and sleep 1ms on failed offer.
         * Should produce similar results as using BLOCK
         */
        ONE_MS(BackPressureStrategy.DROP, () -> {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {}
        });
        
        public final BackPressureStrategy bps;
        public final Runnable ws;
        
        private WaitMode(BackPressureStrategy bps, Runnable ws) {
            this.bps = bps;
            this.ws = ws;
        }
        
    }
    
}
