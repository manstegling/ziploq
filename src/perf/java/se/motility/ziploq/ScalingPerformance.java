package se.motility.ziploq;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.SynchronizedConsumer;
import se.motility.ziploq.api.Ziploq;
import se.motility.ziploq.api.ZiploqFactory;

@Warmup(iterations = 2)
@Measurement(iterations = 8)
@Timeout(time = 600)
@Fork(value = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ScalingPerformance {

    @Benchmark
    public long scale1(Producer state) {
        Entry<Msg> entry = state.ziploq.poll();
        state.lastSource = entry.getMessage().source;
        return entry.getMessage().timestamp;
    }

    @State(Scope.Thread)
    public static class Producer {

        private static final int BUFFER_SZ = 2048;
        private static final Comparator<Msg> COMPARATOR = Comparator.comparing(m -> m.payload, String::compareTo);
        private final Random r = new Random(13371337L);

        @Param({"2", "8", "32", "64", "128", "256", "1024"})
        public int producers;

        public Map<String, SynchronizedConsumer<Msg>> producerList;
        public Map<String, Long> timestampMap;
        public String lastSource = null;
        public Ziploq<Msg> ziploq;

        @Setup(Level.Iteration)
        public void doSetup() {
            this.ziploq = ZiploqFactory.create(COMPARATOR);
            this.producerList = new HashMap<>();
            this.timestampMap = new HashMap<>();
            Msg m;
            for (int i = 0; i < producers; i++) {
                String name = "SCALING_TEST_" + i;
                SynchronizedConsumer<Msg> consumer = ziploq
                        .registerOrdered(BUFFER_SZ, BackPressureStrategy.DROP, name);
                producerList.put(name, consumer);
                timestampMap.put(name, 0L);
                for (int j = 0; j < BUFFER_SZ; j++) {
                    m = generateMsg(name);
                    if (!consumer.onEvent(m, m.timestamp)) {
                        throw new IllegalStateException("Unexpected drop of " + lastSource + "@" + m.timestamp);
                    }
                }
            }
        }

        @Setup(Level.Iteration)
        public void destroy() {
            this.producerList = null;
            this.ziploq = null;
            this.timestampMap = null;
            this.lastSource = null;
        }

        @Setup(Level.Invocation)
        public void setUpPerInvocation() {
            // refill queue
            if (lastSource != null) {
                Msg m = generateMsg(lastSource);
                if (!producerList.get(lastSource).onEvent(m, m.timestamp)) {
                    throw new IllegalStateException("Unexpected drop of " + lastSource + "@" + m.timestamp);
                }
            }
        }

        private Msg generateMsg(String source) {
            long ts = timestampMap.merge(source, (long) r.nextInt(2), Long::sum);
            return new Msg(ts, source,r.nextInt() + "_payload");
        }

        public Ziploq<Msg> ziploq() {
            return ziploq;
        }
    }

    private static class Msg {
        private final long timestamp;
        private final String source;
        private final String payload;
        public Msg(long timestamp, String source, String payload) {
            this.timestamp = timestamp;
            this.source = source;
            this.payload = payload;
        }
    }

}


