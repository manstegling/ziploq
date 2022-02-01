package se.motility.ziploq;

import static org.junit.Assert.*;
import static se.motility.ziploq.SyncTestUtils.*;
import static se.motility.ziploq.SyncTestUtils.MsgObject.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.stream.IntStream;

import org.junit.Test;

import se.motility.ziploq.SyncTestUtils.MsgObject;
import se.motility.ziploq.SyncTestUtils.TestEntry;
import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.FlowConsumer;
import se.motility.ziploq.api.ZipFlow;
import se.motility.ziploq.api.ZiploqFactory;

public class BlockingUnorderedQueueTest extends AbstractUnorderedQueueTest {

    @Override
    BackPressureStrategy getStrategy() {
        return BackPressureStrategy.BLOCK;
    }
    
    @Test(timeout=5000)
    public void compositeTest() throws InterruptedException {
        /**
         * This test works because capacity is a power of 2.
         * The SpscArrayQueue implementation currently used
         * rounds any non-power of 2 capacity to nearest
         * bigger power of 2.
         */
        
        int capacity = 8;
        int bDelay = 10;
        ZipFlow<MsgObject> ziploq = ZiploqFactory.create(100L, null);
        FlowConsumer<MsgObject> consumer = ziploq.registerUnordered(bDelay, capacity, getStrategy(), TEST_SOURCE, null);
        
        Phaser ours = new Phaser();
        Phaser remote = new Phaser();
        ours.register();
        List<TestEntry> result = Collections.synchronizedList(new ArrayList<>());
        Thread t = new Thread(() -> produce(consumer, capacity, bDelay, ours, remote, result));
        t.start();

        remote.awaitAdvance(0);
        assertFalse(result.isEmpty());
        
        List<TestEntry> r = new ArrayList<>(result);
        result.clear();
        for (TestEntry e : r) {
            verify(e, ziploq.take()); //blocks
        }
        ours.arriveAndDeregister();
        
        remote.awaitAdvance(1);
        assertFalse(result.isEmpty());
        for (TestEntry e : result) {
            verify(e, ziploq.poll());
        }
        assertNull(ziploq.poll());
        failOnException(t::join);
    }
    
    private void produce(FlowConsumer<MsgObject> consumer, int capacity, int businessDelay, Phaser remote, Phaser ours, List<TestEntry> result) {
        ours.register();
        //Fill queue up to capacity
        IntStream.range(0, capacity-1)
                 .mapToObj(i -> consume(consumer, OBJECT_1, i, ZERO))
                 .forEach(result::add);

        ours.arrive();
        TestEntry last = consume(consumer, OBJECT_1, capacity - 2 + businessDelay, ZERO); //release previous 'capacity-1' messages
        remote.awaitAdvance(0); //await 'result' has been cleaned
        result.add(last);
        
        consume(consumer, OBJECT_1, capacity + 2 * businessDelay - 1, ZERO); //release last message
        //Advance to show last message has been released
        ours.arriveAndDeregister();
    }
    
}
