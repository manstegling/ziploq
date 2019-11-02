package se.motility.ziploq;

import static org.junit.Assert.*;
import static se.motility.ziploq.SyncTestUtils.*;
import static se.motility.ziploq.SyncTestUtils.MsgObject.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Phaser;
import java.util.stream.IntStream;

import org.junit.Test;

import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.FlowConsumer;
import se.motility.ziploq.api.ZipFlow;
import se.motility.ziploq.api.ZiploqFactory;

public class BlockingOrderedQueueTest extends AbstractOrderedQueueTest {

    @Override
    BackPressureStrategy getStrategy() {
        return BackPressureStrategy.BLOCK;
    }
    
    @Test(timeout=5000)
    public void blockWhenFull() {
        /**
         * This test works because capacity is a power of 2.
         * The SpscArrayQueue implementation currently used
         * rounds any non-power of 2 capacity to nearest
         * bigger power of 2.
         */
        
        int capacity = 4;
        
        ZipFlow<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        FlowConsumer<MsgObject> consumer = ziploq.registerOrdered(capacity, getStrategy(), TEST_SOURCE);
        
        Phaser phaser = new Phaser();

        List<TestEntry> result = Collections.synchronizedList(new ArrayList<>());
        Thread t = new Thread(() -> produce(consumer, capacity, phaser, result));
        t.start();
        
        phaser.awaitAdvance(0);
        assertFalse(result.isEmpty());
        
        List<TestEntry> r = new ArrayList<>(result);
        result.clear();
        r.forEach(e -> verify(e, ziploq.poll()));
        
        phaser.awaitAdvance(1);
        
        result.forEach(e -> verify(e, ziploq.poll()));
        assertNull(ziploq.poll());
        failOnException(t::join);
    }
    
    private void produce(FlowConsumer<MsgObject> consumer, int capacity, Phaser phaser, List<TestEntry> result) {
        phaser.register();
        //Fill queue up to capacity
        IntStream.range(0, capacity)
                 .mapToObj(i -> consume(consumer, OBJECT_1, i, ZERO))
                 .forEach(result::add);

        //Advance to show queue was filled correctly
        phaser.arrive();
        result.add(consume(consumer, OBJECT_1, capacity, ZERO));
        
        //Advance to show blocking call has returned and result can be used
        phaser.arriveAndDeregister();
    }

}
