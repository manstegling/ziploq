package se.motility.ziploq;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
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
import se.motility.ziploq.api.SynchronizedConsumer;
import se.motility.ziploq.api.Ziploq;
import se.motility.ziploq.api.ZiploqFactory;
import se.motility.ziploq.testapi.Producer;
import se.motility.ziploq.testapi.ProducerState;
import se.motility.ziploq.testimpl.OrderedProducer;
import se.motility.ziploq.testimpl.OrderedProducerNoSystemTs;
import se.motility.ziploq.testimpl.OrderedProducerWithDrift;
import se.motility.ziploq.testimpl.OrderedProducerWithJumps;
import se.motility.ziploq.testimpl.QuietOrderedProducer;

@Warmup(iterations = 2)
@Measurement(iterations = 15)
@OutputTimeUnit(TimeUnit.SECONDS)
@Timeout(time = 600)
@Fork(value = 1)
@BenchmarkMode(Mode.SingleShotTime)
public class OrderedInputPerformance {
    
    private static final String TEST_SOURCE = "SOURCE";
    
    // -- Performance tests -- //

    @Benchmark
    @Threads(1)
    public long basicTakeNoSystemTs(SimpleProducers state) throws InterruptedException {
        return ZiploqTests.performTakeTest(state);
    }
    
    @Benchmark
    @Threads(1)
    public long basicStreamNoSystemTs(SimpleProducers state) throws InterruptedException {
        return ZiploqTests.performStreamTest(state);
    }
    
    @Benchmark
    @Threads(1)
    public long basicTake(SimpleProducers state) throws InterruptedException {
        return ZiploqTests.performTakeTest(state);
    }
    
    @Benchmark
    @Threads(1)
    public long basicStream(SimpleProducers state) throws InterruptedException {
        return ZiploqTests.performStreamTest(state);
    }
    
    @Benchmark
    @Threads(1)
    public long mixedTake(MixedProducers state) throws InterruptedException {
        return ZiploqTests.performTakeTest(state);
    }
    
    @Benchmark
    @Threads(1)
    public long mixedStream(MixedProducers state) throws InterruptedException {
        return ZiploqTests.performStreamTest(state);
    }
    
    @Benchmark
    @Threads(1)
    public long driftingTake(DriftingProducers state) throws InterruptedException {
        return ZiploqTests.performTakeTest(state);
    }
    
    @Benchmark
    @Threads(1)
    public long driftingStream(DriftingProducers state) throws InterruptedException {
        return ZiploqTests.performStreamTest(state);
    }
    
    // -- State Classes -- //
    
    @State(Scope.Thread)
    public static class SimpleProducers implements ProducerState {
        
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
        
        public Ziploq<Object> ziploq;
        public List<Producer> producerList;
        
        @Setup(Level.Invocation)
        public synchronized void doSetup() {
            long systemDelay = 1000L;
            this.ziploq = ZiploqFactory.create(systemDelay, Optional.ofNullable(comparator()));
            this.producerList = new ArrayList<>();
            int messagesPerProducer = (totalMessages / producers) + 1;
            for (int i=0; i<producers; i++) {
                SynchronizedConsumer<Object> consumer = ziploq.registerOrdered(capacity, mode.bps, TEST_SOURCE);
                producerList.add(new OrderedProducerNoSystemTs(consumer, messagesPerProducer, msgsPerMilli, mode.ws));
            }
        }

        @Override
        public Ziploq<Object> ziploq() {
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
        
        public Ziploq<Object> ziploq;
        public List<Producer> producerList;
        
        @Setup(Level.Invocation)
        public synchronized void doSetup() {
            long systemDelay = 1000L;
            this.ziploq = ZiploqFactory.create(systemDelay, Optional.ofNullable(comparator()));
            this.producerList = new ArrayList<>();
            int messagesPerProducer = (totalMessages / producers) + 1;
            for (int i=0; i<producers; i++) {
                SynchronizedConsumer<Object> consumer = ziploq.registerOrdered(capacity, mode.bps, TEST_SOURCE);
                producerList.add(new OrderedProducer(consumer, messagesPerProducer, msgsPerMilli, mode.ws));
            }
        }

        @Override
        public Ziploq<Object> ziploq() {
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
    
    @State(Scope.Thread)
    public static class MixedProducers implements ProducerState {
        
        @Param({"500000000"})
        public int totalMessages;
        @Param({"BLOCK", "YIELD", "ONE_MS"})
        public WaitMode mode;
        @Param({"20000"})
        public int capacity;
        @Param({"1"})
        public int producerSets;
        
        public Ziploq<Object> ziploq;
        public List<Producer> producerList;
        
        @Setup(Level.Invocation)
        public synchronized void doSetup() {
            long systemDelay = 1000L;
            this.ziploq = ZiploqFactory.create(systemDelay, Optional.ofNullable(comparator()));
            this.producerList = new ArrayList<>();
            int messagesPerProducer = (totalMessages / (3*producerSets)) + 1;
            for(int i=0;i<producerSets;i++) {
                SynchronizedConsumer<Object> consumer1 = ziploq.registerOrdered(capacity, mode.bps, TEST_SOURCE);
                producerList.add(new OrderedProducerWithDrift(consumer1, messagesPerProducer,
                        (int)systemDelay, mode.ws));
                SynchronizedConsumer<Object> consumer2 = ziploq.registerOrdered(capacity, mode.bps, TEST_SOURCE);
                producerList.add(new OrderedProducerWithJumps(consumer2, messagesPerProducer,
                        100, 100, mode.ws));
                SynchronizedConsumer<Object> consumer3 = ziploq.registerOrdered(capacity, mode.bps, TEST_SOURCE);
                producerList.add(new QuietOrderedProducer(consumer3, messagesPerProducer, 0.5, mode.ws));
            }
        }

        @Override
        public Ziploq<Object> ziploq() {
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
    
    @State(Scope.Thread)
    public static class DriftingProducers implements ProducerState {
        
        @Param({"500000000"})
        public int totalMessages;
        @Param({"BLOCK", "YIELD", "ONE_MS"})
        public WaitMode mode;
        @Param({"20000"})
        public int capacity;
        @Param({"1"})
        public int producerSets;
        
        public Ziploq<Object> ziploq;
        public List<Producer> producerList;
        
        @Setup(Level.Invocation)
        public synchronized void doSetup() {
            long systemDelay = 1000L;
            this.ziploq = ZiploqFactory.create(systemDelay, Optional.ofNullable(comparator()));
            this.producerList = new ArrayList<>();
            int messagesPerProducer = (totalMessages / (3*producerSets)) + 1;
            for(int i=0;i<producerSets;i++) {
                SynchronizedConsumer<Object> consumer1 = ziploq.registerOrdered(capacity, mode.bps, TEST_SOURCE);
                producerList.add(new OrderedProducerWithDrift(consumer1, messagesPerProducer,
                        (int)systemDelay, mode.ws));
                SynchronizedConsumer<Object> consumer2 = ziploq.registerOrdered(capacity, mode.bps, TEST_SOURCE);
                producerList.add(new OrderedProducerWithDrift(consumer2, messagesPerProducer,
                        (int)systemDelay, mode.ws));
                SynchronizedConsumer<Object> consumer3 = ziploq.registerOrdered(capacity, mode.bps, TEST_SOURCE);
                producerList.add(new OrderedProducerWithDrift(consumer3, messagesPerProducer,
                        (int)systemDelay, mode.ws));
            }
        }

        @Override
        public Ziploq<Object> ziploq() {
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
