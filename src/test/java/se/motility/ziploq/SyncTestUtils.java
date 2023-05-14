package se.motility.ziploq;

import static org.junit.Assert.*;

import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.FlowConsumer;

public class SyncTestUtils {
    
    public enum MsgObject {
        OBJECT_1,
        OBJECT_2,
        OBJECT_3,
        OBJECT_4,
        OBJECT_5,
        OBJECT_6,
        OBJECT_7;
    }
    
    public static final long ZERO = 0L;
    public static final long TS_1 = 1L;
    
    /**
     * A comparator for sorting the declared MsgObjects used for testing purposes
     */
    public static final Comparator<MsgObject> COMPARATOR = (o1,o2) -> o1.compareTo(o2);
    
    public static void failOnException(RunnableWithException runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            fail();
        }
    }
    
    public static void verify(TestEntry expected, Entry<MsgObject> actual) {
        assertEquals(expected.getMessage(), actual.getMessage());
        assertEquals(expected.getBusinessTs(), actual.getBusinessTs());
        assertEquals(expected.getSystemTs(), actual.getSystemTs());
    }

    public static TestEntry consume(Queue<TestEntry> q, MsgObject obj, long businessTs, long systemTs) {
        TestEntry e = new TestEntry(obj, businessTs, systemTs, true);
        if (!q.add(e)) {
            throw new IllegalStateException("Could not add obj " + obj + " to queue @ " + businessTs);
        }
        return e;
    }

    public static TestEntry consume(FlowConsumer<MsgObject> consumer, MsgObject obj, long businessTs, long systemTs) {
        boolean accepted = consumer.onEvent(obj, businessTs, systemTs);
        return new TestEntry(obj, businessTs, systemTs, accepted);
    }
    
    public static void addToQueue(FlowConsumer<MsgObject> consumer, int messages) {
        long time = 100L;
        for (int i = 0; i < messages; i++) {
            if (i % 100 == 0) {
                time += 100;
            }
            time++;
            consume(consumer, MsgObject.OBJECT_1, time, ZERO);
        }
        consumer.complete();
    }
    
    public static void addToQueueUnordered(FlowConsumer<MsgObject> consumer, int messages) {
        long time = 100L;
        for (int i = 0; i < messages; i++) {
            if (i % 100 == 0) {
                time += 100;
            }
            time++;
            consume(consumer, MsgObject.OBJECT_1, time + (i % 2 == 0 ? 2 : -2), ZERO);
        }
        consumer.complete();
    }
    
    public static class TestEntry implements Entry<MsgObject> {
        private static final long serialVersionUID = 1L;
        
        private final MsgObject object;
        private final long businessTs;
        private final long systemTs;
        private final boolean accepted;
        
        TestEntry(MsgObject object, long businessTs, long systemTs, boolean accepted) {
            this.object = object;
            this.businessTs = businessTs;
            this.systemTs = systemTs;
            this.accepted = accepted;
        }

        @Override
        public MsgObject getMessage() {
            return object;
        }

        @Override
        public long getBusinessTs() {
            return businessTs;
        }

        @Override
        public long getSystemTs() {
            return systemTs;
        }
        
        public boolean isAccepted() {
            return accepted;
        }
    }
    
    @FunctionalInterface
    public static interface RunnableWithException {
        void run() throws Exception;
    }
    
    
    public static class AsyncTestThread {
        private final ExecutorService executorService;
        private final Future<?> future;
        
        private Throwable ex;

        public AsyncTestThread(RunnableWithException runnable){
            this.executorService = Executors.newSingleThreadExecutor();
            this.future = executorService.submit(wrap(runnable));
        }
        
        /** 
         * Joins the thread after the runnable has completed. If it does not complete within
         * 2 seconds or has completed exceptionally, an {@code AssertionError} is thrown.
         * */
        public void join() {
            failOnException(() -> complete(2_000L));
        }
        
        private Runnable wrap(RunnableWithException runnable) {
            return () -> {
                try {
                    runnable.run();
                } catch(Throwable e){
                    ex = e;
                }
            };
        }
        
        public boolean isRunning() {
            return !future.isDone();
        }

        private void complete(long timeoutMillis) throws InterruptedException{
            executorService.shutdown();
            executorService.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS);
            while(!future.isDone()) {
                Thread.yield();
            }
            if (ex != null) {
                throw new AssertionError("Exception in test thread: " + ex.getMessage(), ex);
            }
        }
    }
    
    @FunctionalInterface
    public static interface Take<T> {
        T get() throws InterruptedException;
    }
    
    public static class SequenceChecker {
        private long ts = 0L;
        private int total = 0;
        void verify(Take<Entry<?>> take) {
            try {
                verify(take.get());
            } catch (InterruptedException e) {
                fail("Interrupted");
            }
        }
        void verify(Entry<?> m) {
            long updTs = m.getBusinessTs();
            if(updTs < ts) {
                fail("Messages out-of-sequence");
            }
            ts = updTs;
            total++;
        }
        int getTotal() {
            return total;
        }
    }
    
}
