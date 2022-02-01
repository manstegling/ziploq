package se.motility.ziploq;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

import se.motility.ziploq.ZiploqTests.WaitMode;
import se.motility.ziploq.api.FlowConsumer;
import se.motility.ziploq.api.ZipFlow;
import se.motility.ziploq.api.ZiploqFactory;
import se.motility.ziploq.testapi.Producer;
import se.motility.ziploq.testapi.ProducerState;
import se.motility.ziploq.testimpl.UnorderedProducer;

@Warmup(iterations = 2)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.SECONDS)
@Timeout(time = 600)
@Fork(value = 1)
@BenchmarkMode(Mode.SingleShotTime)
public class UnorderedInputPerformance {

    private static final String TEST_SOURCE = "SOURCE";
    
    // -- Performance tests -- //
    
    @Benchmark
    @Threads(1)
    public long basicTake(BasicProducers state) throws InterruptedException {
        return ZiploqTests.performTakeTest(state);
    }
    
    @Benchmark
    @Threads(1)
    public long basicStream(BasicProducers state) throws InterruptedException {
        return ZiploqTests.performStreamTest(state);
    }
    
    // -- State Classes -- //
    
    @State(Scope.Thread)
    public static class BasicProducers implements ProducerState {
        
        @Param({"500000000"})
        public int totalMessages;
        @Param({"2", "4", "10"})
        public int producers;
        @Param({"BLOCK", "YIELD", "ONE_MS"})
        public WaitMode mode;
        @Param({"20000"})
        public int capacity;
        @Param({"1.0"})
        public double msgsPerMilli;
        @Param({"1000"})
        public int delay;
        
        public ZipFlow<Object> ziploq;
        public List<Producer> producerList;
        
        @Setup(Level.Invocation)
        public synchronized void doSetup() {
            long systemDelay = 1000L;
            this.ziploq = ZiploqFactory.create(systemDelay, comparator());
            this.producerList = new ArrayList<>();
            int messagesPerProducer = (totalMessages / producers) + 1;
            for (int i=0; i<producers; i++) {
                FlowConsumer<Object> consumer = ziploq.registerUnordered(
                        delay, capacity, mode.bps, TEST_SOURCE, null);
                producerList.add(new UnorderedProducer(consumer, messagesPerProducer, msgsPerMilli, delay, mode.ws));
            }
        }

        @Override
        public ZipFlow<Object> ziploq() {
            return ziploq;
        }
        @Override
        public List<Producer> producers() {
            return producerList;
        }
        @Override
        public Comparator<Object> comparator() {
            return null;
        }
    }
    
}
