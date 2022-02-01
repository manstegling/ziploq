package se.motility.ziploq;

import static org.junit.Assert.*;
import static se.motility.ziploq.SyncTestUtils.*;
import static se.motility.ziploq.SyncTestUtils.MsgObject.*;

import org.junit.Test;

import se.motility.ziploq.SyncTestUtils.MsgObject;
import se.motility.ziploq.SyncTestUtils.TestEntry;
import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.FlowConsumer;
import se.motility.ziploq.api.ZipFlow;
import se.motility.ziploq.api.ZiploqFactory;

public class DroppingUnorderedQueueTest extends AbstractUnorderedQueueTest {

    @Override
    BackPressureStrategy getStrategy() {
        return BackPressureStrategy.DROP;
    }
    
    @Test
    public void acceptedMessages() {
        long delay = 5L;
        
        ZipFlow<MsgObject> ziploq = ZiploqFactory.create(delay, COMPARATOR);
        FlowConsumer<MsgObject> consumer = ziploq.registerUnordered(delay, 2, getStrategy(), TEST_SOURCE, COMPARATOR);
        
        assertNull(ziploq.poll());
        
        TestEntry e1 = consume(consumer, OBJECT_1, TS_1,           ZERO);
        assertTrue(e1.isAccepted());
        assertNull(ziploq.poll());   
        
        TestEntry e2 = consume(consumer, OBJECT_2, TS_1,           ZERO);
        assertTrue(e2.isAccepted());
        assertNull(ziploq.poll());   
        
        TestEntry e3 = consume(consumer, OBJECT_3, TS_1 + delay,   ZERO); //accepted since no ready messages yet, promotes e1 and e2
        assertTrue(e3.isAccepted());
        
        TestEntry e4 = consume(consumer, OBJECT_4, TS_1 + 2*delay, ZERO); //not accepted but promotes e3
        assertFalse(e4.isAccepted());
        
        verify(e1, ziploq.poll());
        verify(e2, ziploq.poll());
        verify(e3, ziploq.poll());
        assertNull(ziploq.poll());        
        
    }
    
}
