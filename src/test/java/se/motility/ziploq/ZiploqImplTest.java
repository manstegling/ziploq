package se.motility.ziploq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static se.motility.ziploq.SyncTestUtils.COMPARATOR;
import static se.motility.ziploq.SyncTestUtils.TS_1;
import static se.motility.ziploq.SyncTestUtils.ZERO;
import static se.motility.ziploq.SyncTestUtils.consume;
import static se.motility.ziploq.SyncTestUtils.failOnException;
import static se.motility.ziploq.SyncTestUtils.verify;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_1;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_2;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_3;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_4;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_5;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_6;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import se.motility.ziploq.SyncTestUtils.AsyncTestThread;
import se.motility.ziploq.SyncTestUtils.MsgObject;
import se.motility.ziploq.SyncTestUtils.TestEntry;
import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.SynchronizedConsumer;
import se.motility.ziploq.api.Ziploq;
import se.motility.ziploq.api.ZiploqFactory;

public class ZiploqImplTest {

    private static class SequenceChecker {
        private long ts = 0L;
        private int total = 0;
        void verify(Take<Entry<MsgObject>> take) {
            try {
                verify(take.get());
            } catch (InterruptedException e) {
                fail("Interrupted");
            }
        }
        void verify(Entry<MsgObject> m) {
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
    
    @FunctionalInterface
    private static interface Take<T> {
        T get() throws InterruptedException;
    }
    
    private static void takeAndVerify(Ziploq<MsgObject> ziploq,
            List<TestEntry> expected) throws InterruptedException {
        for(TestEntry e : expected) {
            verify(e, ziploq.take());
        }
        assertNull(ziploq.poll());
    }
    
    private static AsyncTestThread createTakeAndVerifyThread(
            Ziploq<MsgObject> ziploq, List<TestEntry> expected) {
        return new AsyncTestThread(() -> takeAndVerify(ziploq, expected));
    }
    
    //  A very crude approach to make sure the test thread has come to a blocking state.
    //  Simply sleeps and awakes 100 times (to mess with OS thread scheduling)
    //  and then checks that the test task is still running
    //  TODO:
    //      * Replace this with separate tests testing blocking functionality
    //      * For the purpose of coordinating calls, other methods should be used
    //
    private static void verifyBlocking(AsyncTestThread testThread) {
        for(int i=0;i<100;i++) {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {
                fail("Thread interrupted!");
            }
        }
        if(!testThread.isRunning()) {
            fail("Task completed. Expected thread to block!");
        }
    }
    
    private void joinTestThread(AsyncTestThread t1) {
        failOnException(() -> t1.test(2_000L));
    }
    
    private void addToQueue(SynchronizedConsumer<MsgObject> consumer, int messages) {
        long time = 100L;
        for (int i = 0; i < messages; i++) {
            if (i % 100 == 0) {
                time += 100;
            }
            time++;
            consume(consumer, OBJECT_1, time, ZERO);
        }
        consumer.complete();
    }
    
    private void addToQueueUnordered(SynchronizedConsumer<MsgObject> consumer, int messages) {
        long time = 100L;
        for (int i = 0; i < messages; i++) {
            if (i % 100 == 0) {
                time += 100;
            }
            time++;
            consume(consumer, OBJECT_1, time + (i % 2 == 0 ? 2 : -2), ZERO);
        }
        consumer.complete();
    }

    @Test(timeout=10_000)
    public void testCleanInit() {
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        assertNull(ziploq.poll());
        //Add similar test for take() 
    }
    
    @Test(timeout=1_000)
    public void testBlockOnTake() {
//        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        //TODO: run ziploq.take() in a separate thread and throw an AssertionError if
        //      the call completed before thread is interrupted
    }
    
    /**
     * Test that blocking mechanism for take() is working correctly
     */
    @Test(timeout=10_000)
    public void blockTakeUntilRelease() {
        long delay = 5L;
        
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer = ziploq.registerUnordered(
                delay, 5, BackPressureStrategy.DROP, Optional.empty());
        
        //E1: (TS1, 0)
        TestEntry e1 = consume(consumer, OBJECT_1, TS_1, ZERO);
        
        //E2: (TS1+delay-1, 0)
        consume(consumer, OBJECT_1, TS_1 + delay - 1, ZERO);
        assertNull(ziploq.poll());

        AsyncTestThread t = createTakeAndVerifyThread(ziploq, Collections.singletonList(e1));
        verifyBlocking(t);
      
        //E3: (TS1+delay, 0) (E1 is released)
        consume(consumer, OBJECT_1, TS_1 + delay, ZERO);

        joinTestThread(t);
    }
    

    
    /**
     * Consumer thread blocks on waiting for sorted messages to become available
     * Producer thread blocks on waiting for capacity to release sorted messages
     * Will cause dead-lock if signaling is not handled correctly
     */
    @Test(timeout=10_000)
    public void perMessageSignalling() {
        long delay = 5L;
        
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.of(COMPARATOR));
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerUnordered(
                delay, 2, BackPressureStrategy.BLOCK, Optional.empty());
        
        List<TestEntry> expected = new ArrayList<>();
        
        //Overfill consumer2 with non-ready messages
        expected.add(consume(consumer2, OBJECT_1, TS_1,     ZERO)); //#1
        expected.add(consume(consumer2, OBJECT_1, TS_1 + 1, ZERO)); //#2
        expected.add(consume(consumer2, OBJECT_1, TS_1 + 2, ZERO)); //#3
        assertNull(ziploq.poll());

        AsyncTestThread t = createTakeAndVerifyThread(ziploq, expected);
        verifyBlocking(t);
        
        //E4: Release all previous consumer2 messages (blocks until message is taken from ziploq)
        consume(consumer2, OBJECT_1, TS_1 + 2 + delay, ZERO); //#4

        joinTestThread(t);
    }
    
    /**
     * Producer thread blocks on waiting for capacity to release sorted messages
     * Consumer thread blocks on waiting for sorted messages to become available
     * Will cause dead-lock if signaling is not handled correctly
     */
    @Test(timeout=20_000)
    public void perMessageSignallingProducerFirst() {
        long delay = 1L;
        
        int capacity = 4;
        int messages = 10_000;
        
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.of(COMPARATOR));
        SynchronizedConsumer<MsgObject> consumer = ziploq.registerUnordered(
                delay, capacity, BackPressureStrategy.BLOCK, Optional.empty());
        
        AsyncTestThread t = new AsyncTestThread(() -> addToQueue(consumer, messages));
        verifyBlocking(t);
        
        SequenceChecker checker = new SequenceChecker();
        for (int i=0;i<messages-1;i++) {
            checker.verify(ziploq::take);
        }
        
        joinTestThread(t);
    }
    
    /**
     * Test that the comparator provided to synchronizer works correctly
     */
    
    @SuppressWarnings("unused")
    @Test
    public void secondarySort() {
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.of(COMPARATOR));
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerOrdered(2, BackPressureStrategy.BLOCK);
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerOrdered(2, BackPressureStrategy.BLOCK);
        
        TestEntry e1 = consume(consumer1, OBJECT_2, TS_1,     ZERO); //#2
        TestEntry e2 = consume(consumer1, OBJECT_1, TS_1 + 1, ZERO); //#3
        
        TestEntry e3 = consume(consumer2, OBJECT_1, TS_1,     ZERO); //#1
        TestEntry e4 = consume(consumer2, OBJECT_2, TS_1 + 1, ZERO); //#4
        
        verify(e3, ziploq.poll());
        verify(e1, ziploq.poll());
        verify(e2, ziploq.poll());
        assertNull(ziploq.poll());
    }
    
    /**
     * Test that Ordered queues signal correctly
     */
    @Test(timeout=10_000)
    public void orderedSignalling() {
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.of(COMPARATOR));
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerOrdered(2, BackPressureStrategy.BLOCK);
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerOrdered(2, BackPressureStrategy.BLOCK);
        TestEntry e1 = consume(consumer1, OBJECT_1, TS_1, ZERO);
        assertNull(ziploq.poll());
        
        AsyncTestThread t = createTakeAndVerifyThread(ziploq, Collections.singletonList(e1));
        verifyBlocking(t);
        
        consume(consumer2, OBJECT_2, TS_1, ZERO);
        joinTestThread(t);
    }
    
    /**
     * Put a lot of stress on the producer-consumer interaction by setting low buffers
     * Producer thread blocks on waiting for capacity to release sorted messages
     * Consumer thread blocks on waiting for sorted messages to become available
     * Will cause dead-lock if signaling is not handled correctly
     */
    @Test(timeout=20_000)
    public void orderedSignallingProducerFirst() {

        int messages = 10_000;
        
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.of(COMPARATOR));
        SynchronizedConsumer<MsgObject> consumer = ziploq.registerOrdered(2, BackPressureStrategy.BLOCK);
        
        AsyncTestThread t = new AsyncTestThread(() -> addToQueue(consumer, messages));
        verifyBlocking(t);
        
        SequenceChecker checker = new SequenceChecker();
        for (int i=0;i<messages-1;i++) {
            checker.verify(ziploq::take);
        }
        
        joinTestThread(t);
    }
    
    /**
     * Test that system delay mechanism is working correctly
     */
    @SuppressWarnings("unused")
    @Test
    public void systemTs() {
        long delay = 5L;
        
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(delay, Optional.of(COMPARATOR));
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerUnordered(
                delay, 5, BackPressureStrategy.BLOCK, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerUnordered(
                delay, 5, BackPressureStrategy.BLOCK, Optional.empty());
        
        //Add two messages to c2; none is ready
        TestEntry e1 = consume(consumer2, OBJECT_4, TS_1, ZERO);
        TestEntry e2 = consume(consumer2, OBJECT_5, TS_1, ZERO + delay);
        
        //Add three messages to c1, first two messages are ready
        TestEntry e3 = consume(consumer1, OBJECT_1, TS_1, ZERO);
        TestEntry e4 = consume(consumer1, OBJECT_2, TS_1, ZERO);
        consumer1.updateSystemTime(ZERO + delay);                            //signal that we are still alive
        TestEntry e5 = consume(consumer1, OBJECT_3, TS_1, ZERO + delay + 1); //release in c1
        assertNull(ziploq.poll());                                           //still awaiting c2
        
        TestEntry e6 = consume(consumer2, OBJECT_6, TS_1, ZERO + delay + 1); //release in c2
        
        verify(e3, ziploq.poll());
        verify(e4, ziploq.poll());
        verify(e1, ziploq.poll());
        assertNull(ziploq.poll());        
    }
    
    /**
     * Test that system delay mechanism is working correctly when one producer is slow
     */
    @Test(timeout=10_000)
    public void systemTsSlowProducer() {
        long delay = 5L;
        
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(delay, Optional.of(COMPARATOR));
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerUnordered(
                delay, 5, BackPressureStrategy.BLOCK, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerUnordered(
                delay, 5, BackPressureStrategy.BLOCK, Optional.empty());
        
        //Fill c1
        TestEntry e1 = consume(consumer1, OBJECT_1, TS_1 + 1*delay, ZERO + 1*delay);
        consume(consumer1, OBJECT_1, TS_1 + 2*delay, ZERO + 2*delay);
        consume(consumer1, OBJECT_1, TS_1 + 3*delay, ZERO + 3*delay);
        consume(consumer1, OBJECT_1, TS_1 + 4*delay, ZERO + 4*delay);
        consume(consumer1, OBJECT_1, TS_1 + 5*delay, ZERO + 5*delay);
        assertNull(ziploq.poll());
        
        //c2 producing messages slower than real time; none ready
        List<TestEntry> expected = new ArrayList<>();
        expected.add(consume(consumer2, OBJECT_1, TS_1, ZERO));
        expected.add(consume(consumer2, OBJECT_1, TS_1, ZERO + delay));
        expected.add(e1);
        assertNull(ziploq.poll()); //still awaiting c2
        
        AsyncTestThread t = createTakeAndVerifyThread(ziploq, expected);
        verifyBlocking(t);
        
        //release in c2, slower than real-time
        consume(consumer2, OBJECT_2, TS_1 + delay, ZERO + 3*delay);
        
        //Signal that we have recovered after being silent > system delays
        consumer2.updateSystemTime(ZERO + 3*delay);

        joinTestThread(t);
    }
    
    
    //TODO: structure 100k msg stress tests for all consumer types
    
    /**
     * Lock-free data structures being used overestimate size which could cause
     * issues in signaling (potential deadlock) if not handled correctly. Some empirical
     * analysis showed that such a deadlock would occur within 1k-10k messages when using
     * 1 thread. Running for 100k messages should hence 'guarantee' such scenarios will
     * be forced to occur. 
     */
    @Test(timeout=20_000)
    public void blockingOrderedDeadlock() {
        long delay = 1000L;
        int messages = 100_000;
        
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(delay, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer = ziploq.registerOrdered(1000, BackPressureStrategy.BLOCK);
        
        AsyncTestThread t = new AsyncTestThread(() -> addToQueue(consumer, messages));
        verifyBlocking(t);
        
        SequenceChecker checker = new SequenceChecker();
        for (int i = 0; i < messages; i++) {
            checker.verify(ziploq::take);
        }
        
        joinTestThread(t);
    }
    
    @Test(timeout=10_000)
    public void completeTwoOrdered() {
        long delay = 1000L;
        int messages = 10;
        
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(delay, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerOrdered(5, BackPressureStrategy.BLOCK);
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerOrdered(5, BackPressureStrategy.BLOCK);
        
        AsyncTestThread t1 = new AsyncTestThread(() -> addToQueue(consumer1, messages));
        AsyncTestThread t2 = new AsyncTestThread(() -> addToQueue(consumer2, messages));
        
        SequenceChecker checker = new SequenceChecker();
        for (int i = 0; i < 2*messages; i++) {
            checker.verify(ziploq::take);
        }
        
        joinTestThread(t1);
        joinTestThread(t2);
    }
    
    @Test(timeout=10_000)
    public void completeMixed() {
        long delay = 1000L;
        int messages = 10;
        
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(delay, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerOrdered(5, BackPressureStrategy.BLOCK);
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerOrdered(5, BackPressureStrategy.BLOCK);
        SynchronizedConsumer<MsgObject> consumer3 = ziploq.registerUnordered(
                10, 5, BackPressureStrategy.BLOCK, Optional.empty());

        
        AsyncTestThread t1 = new AsyncTestThread(() -> addToQueue(consumer1, messages));
        AsyncTestThread t2 = new AsyncTestThread(() -> addToQueue(consumer2, messages));
        AsyncTestThread t3 = new AsyncTestThread(() -> addToQueueUnordered(consumer3, messages));
        
        SequenceChecker checker = new SequenceChecker();
        for (int i = 0; i < 3 * messages + 1; i++) {
            checker.verify(ziploq::take);
        }
        
        joinTestThread(t1);
        joinTestThread(t2);
        joinTestThread(t3);
    }

    @Test(timeout=10_000)
    public void streamFromThree() {
        long delay = 1000L;
        int messages = 1000;
        
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(delay, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerOrdered(5, BackPressureStrategy.BLOCK);
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerOrdered(5, BackPressureStrategy.BLOCK);
        SynchronizedConsumer<MsgObject> consumer3 = ziploq.registerUnordered(
                10, 5, BackPressureStrategy.BLOCK, Optional.empty());

        
        AsyncTestThread t1 = new AsyncTestThread(() -> addToQueue(consumer1, messages));
        AsyncTestThread t2 = new AsyncTestThread(() -> addToQueue(consumer2, messages));
        AsyncTestThread t3 = new AsyncTestThread(() -> addToQueueUnordered(consumer3, messages));
        
        SequenceChecker checker = new SequenceChecker();
        ziploq.stream().forEach(checker::verify);
        
        assertEquals(3*messages, checker.getTotal());
        
        joinTestThread(t1);
        joinTestThread(t2);
        joinTestThread(t3);
    }
    
    @Test
    public void recoveryMsgOnly() {
        long delay = 1000L;

        Ziploq<MsgObject> ziploq = ZiploqFactory.create(delay, Optional.of(COMPARATOR));
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerOrdered(5, BackPressureStrategy.BLOCK);
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerOrdered(5, BackPressureStrategy.BLOCK);
        
        // c1
        TestEntry e1 = consume(consumer1, OBJECT_1, TS_1,     ZERO);
        TestEntry e2 = consume(consumer1, OBJECT_1, TS_1 + 4, ZERO + delay);
        consume(consumer1, OBJECT_1, TS_1 + 5, ZERO + 1*delay + 1);
        consume(consumer1, OBJECT_1, TS_1 + 6, ZERO + 2*delay + 1);
        
        // c2
        TestEntry e4 = consume(consumer2, OBJECT_2, TS_1,     ZERO);
        TestEntry e5 = consume(consumer2, OBJECT_2, TS_1 + 1, ZERO + delay + 1); //marks the start of new recovery period
        
        verify(e1, ziploq.poll());
        verify(e4, ziploq.poll());
        verify(e5, ziploq.poll());
        assertNull(ziploq.poll());
        
        TestEntry e6 = consume(consumer2, OBJECT_3, TS_1 + 3, ZERO + 2*delay + 1); //ends c2 recovery period

        verify(e6, ziploq.poll());
        verify(e2, ziploq.poll()); //emitted based on system timestamp
        assertNull(ziploq.poll());
    }
    
    @Test
    public void recoveryUpdSysTime() {
        long delay = 1000L;

        Ziploq<MsgObject> ziploq = ZiploqFactory.create(delay, Optional.of(COMPARATOR));
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerOrdered(5, BackPressureStrategy.BLOCK);
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerOrdered(5, BackPressureStrategy.BLOCK);
        
        // c2; already "in the future"
        consumer2.updateSystemTime(ZERO + 3*delay);
        
        // c1
        TestEntry e1 = consume(consumer1, OBJECT_1, TS_1, ZERO);
        consume(consumer1, OBJECT_1, TS_1 + 5, ZERO + delay + 1); //marks the start of new recovery period
        assertNull(ziploq.poll());
        
        consumer1.updateSystemTime(ZERO + delay + 2); //recovery complete
        
        verify(e1, ziploq.poll()); //emitted based on system time
        assertNull(ziploq.poll());
    }
    
    @Test
    public void recoveryExampleUpdSysTime() {
        long delay = 1000L;

        Ziploq<MsgObject> ziploq = ZiploqFactory.create(delay, Optional.of(COMPARATOR));
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerOrdered(100, BackPressureStrategy.BLOCK);
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerOrdered(100, BackPressureStrategy.BLOCK);
        
        // ordinary progress of c1 and c2
        TestEntry e1 = consume(consumer1, OBJECT_1, TS_1,      ZERO);               //c1
        TestEntry e2 = consume(consumer2, OBJECT_1, TS_1 + 1,  ZERO);               //c2
        TestEntry e3 = consume(consumer1, OBJECT_1, TS_1 + 5,  ZERO + delay/2);     //c1
        TestEntry e4 = consume(consumer2, OBJECT_1, TS_1 + 6,  ZERO + delay/2);     //c2
        TestEntry e5 = consume(consumer1, OBJECT_1, TS_1 + 10, ZERO + delay);       //c1
        TestEntry e6 = consume(consumer2, OBJECT_1, TS_1 + 11, ZERO + delay);       //c2
        TestEntry e7 = consume(consumer1, OBJECT_1, TS_1 + 19, ZERO + 3*delay/2);   //c1
        
        // have consumer catch up
        verify(e1, ziploq.poll());
        verify(e2, ziploq.poll());
        verify(e3, ziploq.poll());
        verify(e4, ziploq.poll());
        verify(e5, ziploq.poll());
        verify(e6, ziploq.poll());
        
        // now assume c2 is experiencing issues
        TestEntry e8 = consume(consumer1, OBJECT_1, TS_1 + 21, ZERO + 2*delay);     //c1
        consume(consumer1, OBJECT_1, TS_1 + 26, ZERO + 5*delay/2);                  //c1
        consume(consumer1, OBJECT_1, TS_1 + 31, ZERO + 3*delay);                    //c1
        consume(consumer1, OBJECT_1, TS_1 + 36, ZERO + 7*delay/2);                  //c1
        
        assertNull(ziploq.poll()); //c2 is empty
        
        // at system time 'ZERO + 3.5*delay' the c1 is back and recovering (but system time is not accepted yet)
        TestEntry e12= consume(consumer2, OBJECT_1, TS_1 + 14, ZERO + 7*delay/2);   //c2 - start of recovery
        verify(e12, ziploq.poll());
        
        // if it wasn't for recovery mechanism at this point e7-e8 would be emitted based on system timestamp
        assertNull(ziploq.poll());
        
        //c2 is catching up, submitting more old messages at this new system time
        //messages are therefore only emitted from ziploq based on business time
        TestEntry e13= consume(consumer2, OBJECT_1, TS_1 + 15, ZERO + 7*delay/2);   //c2
        verify(e13, ziploq.poll());
        assertNull(ziploq.poll());
        TestEntry e14= consume(consumer2, OBJECT_1, TS_1 + 16, ZERO + 7*delay/2);   //c2
        verify(e14, ziploq.poll());
        assertNull(ziploq.poll());
        
        //now c2 has caught up and message processing is normal
        consumer2.updateSystemTime(ZERO + 4*delay);
        
        // e7-e8 are finally emitted based on system time
        verify(e7, ziploq.poll());
        verify(e8, ziploq.poll());
        assertNull(ziploq.poll());
    }
    
    @Test
    public void recoveryExampleMsgsOnly() {
        long delay = 1000L;

        Ziploq<MsgObject> ziploq = ZiploqFactory.create(delay, Optional.of(COMPARATOR));
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerOrdered(100, BackPressureStrategy.BLOCK);
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerOrdered(100, BackPressureStrategy.BLOCK);
        
        // ordinary progress of c1 and c2
        TestEntry e1 = consume(consumer1, OBJECT_1, TS_1,      ZERO);               //c1
        TestEntry e2 = consume(consumer2, OBJECT_1, TS_1 + 1,  ZERO);               //c2
        TestEntry e3 = consume(consumer1, OBJECT_1, TS_1 + 5,  ZERO + delay/2);     //c1
        TestEntry e4 = consume(consumer2, OBJECT_1, TS_1 + 6,  ZERO + delay/2);     //c2
        TestEntry e5 = consume(consumer1, OBJECT_1, TS_1 + 10, ZERO + delay);       //c1
        TestEntry e6 = consume(consumer2, OBJECT_1, TS_1 + 11, ZERO + delay);       //c2
        TestEntry e7 = consume(consumer1, OBJECT_1, TS_1 + 19, ZERO + 3*delay/2);   //c1
        
        // have consumer catch up
        verify(e1, ziploq.poll());
        verify(e2, ziploq.poll());
        verify(e3, ziploq.poll());
        verify(e4, ziploq.poll());
        verify(e5, ziploq.poll());
        verify(e6, ziploq.poll());
        
        // now assume c2 is experiencing issues
        TestEntry e8 = consume(consumer1, OBJECT_1, TS_1 + 21, ZERO + 2*delay);     //c1
        consume(consumer1, OBJECT_1, TS_1 + 26, ZERO + 5*delay/2);                  //c1
        consume(consumer1, OBJECT_1, TS_1 + 31, ZERO + 3*delay);                    //c1
        consume(consumer1, OBJECT_1, TS_1 + 36, ZERO + 7*delay/2);                  //c1
        
        assertNull(ziploq.poll()); //c2 is empty
        
        // at system time 'ZERO + 3.5*delay' the c1 is back and recovering (but system time is not accepted yet)
        TestEntry e12= consume(consumer2, OBJECT_1, TS_1 + 14, ZERO + 7*delay/2);   //c2 - start of recovery
        verify(e12, ziploq.poll());
        
        // if it wasn't for recovery mechanism at this point e7-e8 would be emitted based on system timestamp
        assertNull(ziploq.poll());
        
        //c2 is catching up, submitting more old messages at this new system time
        //messages are therefore only emitted from ziploq based on business time
        TestEntry e13= consume(consumer2, OBJECT_1, TS_1 + 15, ZERO + 7*delay/2);   //c2
        verify(e13, ziploq.poll());
        assertNull(ziploq.poll());
        TestEntry e14= consume(consumer2, OBJECT_1, TS_1 + 16, ZERO + 7*delay/2);   //c2
        verify(e14, ziploq.poll());
        assertNull(ziploq.poll());
        
        //now c2 has caught up and proceeds as normal [artificial test to show recovery completes for messages too]
        TestEntry e15= consume(consumer2, OBJECT_1, TS_1 + 17, ZERO + 4*delay);     //c2
        verify(e15, ziploq.poll());
        assertNull(ziploq.poll());
        TestEntry e16= consume(consumer2, OBJECT_1, TS_1 + 18, ZERO + 9*delay/2);   //c2 - end of recovery
        verify(e16, ziploq.poll());
        
        // e7-e8 are finally emitted based on system time
        verify(e7, ziploq.poll());
        verify(e8, ziploq.poll());
        assertNull(ziploq.poll());
    }
    
}
