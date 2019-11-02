package se.motility.ziploq;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.Test;
import se.motility.ziploq.SyncTestUtils.MsgObject;
import se.motility.ziploq.SyncTestUtils.TestEntry;
import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.FlowConsumer;
import se.motility.ziploq.api.ZipFlow;
import se.motility.ziploq.api.ZiploqFactory;

import static org.junit.Assert.*;
import static se.motility.ziploq.SyncTestUtils.COMPARATOR;
import static se.motility.ziploq.SyncTestUtils.TS_1;
import static se.motility.ziploq.SyncTestUtils.ZERO;
import static se.motility.ziploq.SyncTestUtils.consume;
import static se.motility.ziploq.SyncTestUtils.verify;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_1;

public class UnboundedOrderedQueueTest extends AbstractOrderedQueueTest {

    @Override
    BackPressureStrategy getStrategy() {
        return BackPressureStrategy.UNBOUNDED;
    }
    
    @Test
    public void addBeyondCapacity() throws InterruptedException {
        int capacity = 4;
        ZipFlow<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.of(COMPARATOR));
        FlowConsumer<MsgObject> consumer = ziploq.registerOrdered(capacity, getStrategy(), TEST_SOURCE);
        
        List<TestEntry> expected = new ArrayList<>();
        long ts = TS_1;
        expected.add(consume(consumer, OBJECT_1, ts++, ZERO));
        expected.add(consume(consumer, OBJECT_1, ts++, ZERO));
        expected.add(consume(consumer, OBJECT_1, ts++, ZERO));
        expected.add(consume(consumer, OBJECT_1, ts++, ZERO));
        boolean hasReached = false;
        for(int i = 0; i < 127; i++) {
            TestEntry e = consume(consumer, OBJECT_1, ts++, ZERO);
            expected.add(e);
            if (!e.isAccepted()) {
                hasReached = true;
                break;
            }
        }
        assertTrue(hasReached);
        
        //Insert more after entries are no longer "accepted"
        expected.add(consume(consumer, OBJECT_1, ts++, ZERO));
        expected.add(consume(consumer, OBJECT_1, ts++, ZERO));
        expected.add(consume(consumer, OBJECT_1, ts++, ZERO));
        
        //Verify that all events in fact have been inserted
        for(TestEntry e : expected) {
            verify(e, ziploq.take());
        }
        assertNull(ziploq.poll());
    }

}
