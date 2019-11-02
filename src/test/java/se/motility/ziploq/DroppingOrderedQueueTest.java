package se.motility.ziploq;

import static org.junit.Assert.*;
import static se.motility.ziploq.SyncTestUtils.*;
import static se.motility.ziploq.SyncTestUtils.MsgObject.*;

import java.util.Optional;

import org.junit.Test;

import se.motility.ziploq.SyncTestUtils.MsgObject;
import se.motility.ziploq.SyncTestUtils.TestEntry;
import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.FlowConsumer;
import se.motility.ziploq.api.ZipFlow;
import se.motility.ziploq.api.ZiploqFactory;

public class DroppingOrderedQueueTest extends AbstractOrderedQueueTest {

    @Override
    BackPressureStrategy getStrategy() {
        return BackPressureStrategy.DROP;
    }
    
    @Test
    public void acceptedMessages() {
        int capacity = 4; //4 is minimum capacity of underlying queue
        ZipFlow<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.of(COMPARATOR));
        FlowConsumer<MsgObject> consumer = ziploq.registerOrdered(capacity, getStrategy(), TEST_SOURCE); 
        
        assertNull(ziploq.poll());
        
        TestEntry e1 = consume(consumer, OBJECT_1, TS_1, ZERO);        
        assertTrue(e1.isAccepted());
        
        TestEntry e2 = consume(consumer, OBJECT_2, TS_1, ZERO);
        assertTrue(e2.isAccepted());
        
        TestEntry e3 = consume(consumer, OBJECT_3, TS_1, ZERO);        
        assertTrue(e3.isAccepted());
        
        TestEntry e4 = consume(consumer, OBJECT_4, TS_1, ZERO);
        assertTrue(e4.isAccepted());
        
        TestEntry e5 = consume(consumer, OBJECT_5, TS_1, ZERO); //not accepted
        assertFalse(e5.isAccepted());
        
        TestEntry e6 = consume(consumer, OBJECT_6, TS_1, ZERO); //not accepted
        assertFalse(e6.isAccepted());
        
        verify(e1, ziploq.poll());
        verify(e2, ziploq.poll());
        verify(e3, ziploq.poll());
        verify(e4, ziploq.poll());
        assertNull(ziploq.poll());        
        
        TestEntry e7 = consume(consumer, OBJECT_7, TS_1, ZERO);
        assertTrue(e7.isAccepted());
        
        verify(e7, ziploq.poll());
        assertNull(ziploq.poll());
    }

}
