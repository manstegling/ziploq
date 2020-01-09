package se.motility.ziploq;

import static org.junit.Assert.*;
import static se.motility.ziploq.SyncTestUtils.*;
import static se.motility.ziploq.SyncTestUtils.MsgObject.*;

import java.util.Optional;

import org.junit.Test;

import se.motility.ziploq.SyncTestUtils.MsgObject;
import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.FlowConsumer;
import se.motility.ziploq.api.ZipFlow;
import se.motility.ziploq.api.ZiploqFactory;

@SuppressWarnings("unused")
public abstract class AbstractUnorderedQueueTest {
    
    static final String TEST_SOURCE = "SOURCE";
    abstract BackPressureStrategy getStrategy();

    @Test
    public void delayedRelease() {
        
        long delay = 5L;
        
        ZipFlow<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        FlowConsumer<MsgObject> consumer = ziploq.registerUnordered(delay, 5, getStrategy(), TEST_SOURCE, Optional.empty());
        
        //E1: (TS1, 0)
        TestEntry e1 = consume(consumer, OBJECT_1, TS_1,             ZERO); //#1
        assertNull(ziploq.poll()); 
        
        //E2: (TS1+delay-1, 0)
        TestEntry e2 = consume(consumer, OBJECT_1, TS_1 + delay - 1, ZERO); //#2
        assertNull(ziploq.poll()); 
        
        //E3: (TS1+delay, 0) (E1 is released)
        TestEntry e3 = consume(consumer, OBJECT_1, TS_1 + delay,     ZERO); //#3
        
        verify(e1, ziploq.poll());
        assertNull(ziploq.poll());
        
        //E4: (TS1+delay+1,0)
        TestEntry e4 = consume(consumer, OBJECT_1, TS_1 + delay + 1, ZERO); //#4
        assertNull(ziploq.poll());
    }
    
    @Test
    public void delayAndSort() {
        
        long delay = 5L;
        
        ZipFlow<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        FlowConsumer<MsgObject> consumer = ziploq.registerUnordered(delay, 5, getStrategy(), TEST_SOURCE, Optional.empty());
        
        //E1: (TS1+3, 0)
        TestEntry e1 = consume(consumer, OBJECT_1, TS_1 + 3,         ZERO); //#4
        assertNull(ziploq.poll());
        
        //E2: (TS1, 0)
        TestEntry e2 = consume(consumer, OBJECT_1, TS_1,             ZERO); //#1
        assertNull(ziploq.poll());
        
        //E3: (TS1+2, 0)
        TestEntry e3 = consume(consumer, OBJECT_1, TS_1 + 2,         ZERO); //#3
        assertNull(ziploq.poll());
        
        //E4: (TS1+1,0)
        TestEntry e4 = consume(consumer, OBJECT_1, TS_1 + 1,         ZERO); //#2
        assertNull(ziploq.poll());
        
        //E5: (TS1+3+delay,0) (releases all prior messages)
        TestEntry e5 = consume(consumer, OBJECT_1, TS_1 + 3 + delay, ZERO); //#5
        
        verify(e2, ziploq.poll());
        verify(e4, ziploq.poll());
        verify(e3, ziploq.poll());
        verify(e1, ziploq.poll());
        assertNull(ziploq.poll());
    }
    
    @Test
    public void secondarySort() {
        long delay = 5L;
        
        ZipFlow<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        FlowConsumer<MsgObject> consumer = ziploq.registerUnordered(delay, 5, getStrategy(), TEST_SOURCE, Optional.of(COMPARATOR));
        
        //E1: (TS1, 0, O2)
        TestEntry e1 = consume(consumer, OBJECT_2, TS_1,             ZERO); //#2
        assertNull(ziploq.poll());
        
        //E2: (TS1, 0, O1)
        TestEntry e2 = consume(consumer, OBJECT_1, TS_1,             ZERO); //#1
        assertNull(ziploq.poll());
        
        //E3: (TS1+1, 0, O1)
        TestEntry e3 = consume(consumer, OBJECT_1, TS_1 + 1,         ZERO); //#3
        assertNull(ziploq.poll());
        
        //E4: (TS1+1, 0, O2)
        TestEntry e4 = consume(consumer, OBJECT_2, TS_1 + 1,         ZERO); //#4
        assertNull(ziploq.poll());
        
        //E5: (TS1+1+delay, 0) (releases all prior messages)
        TestEntry e5 = consume(consumer, OBJECT_1, TS_1 + 1 + delay, ZERO); //#5
        
        verify(e2, ziploq.poll());
        verify(e1, ziploq.poll());
        verify(e3, ziploq.poll());
        verify(e4, ziploq.poll());
        assertNull(ziploq.poll());
    }
    
    
    @Test
    public void systemTs() {
        long bDelay = 5L;
        long sDelay = 10L;
        
        ZipFlow<MsgObject> ziploq = ZiploqFactory.create(sDelay, Optional.empty());
        FlowConsumer<MsgObject> consumer = ziploq.registerUnordered(bDelay, 5, getStrategy(), TEST_SOURCE, Optional.of(COMPARATOR));
        
        //E1: (TS1, TS1, O1)
        TestEntry e1 = consume(consumer, OBJECT_1, TS_1, TS_1);              //#1
        assertNull(ziploq.poll());
        
        //E1: (TS1, TS1, O2)
        TestEntry e2 = consume(consumer, OBJECT_2, TS_1, TS_1);              //#2
        assertNull(ziploq.poll());
        
        //E1: (TS1, TS1+delay-1, O3)
        TestEntry e3 = consume(consumer, OBJECT_3, TS_1, TS_1 + sDelay);     //#3
        assertNull(ziploq.poll());
        
        //E1: (TS1, TS1+delay, O4)
        TestEntry e4 = consume(consumer, OBJECT_4, TS_1, TS_1 + sDelay + 1); //#4

        verify(e1, ziploq.poll());
        verify(e2, ziploq.poll());
        assertNull(ziploq.poll());
        
        //E1: (TS1, TS1+delay+1, O1)
        TestEntry e5 = consume(consumer, OBJECT_1, TS_1, TS_1 + sDelay + 2); //#5
        assertNull(ziploq.poll());
        
    }
    
    @Test(expected = IllegalStateException.class)
    public void onEventAfterComplete() {
        ZipFlow<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        FlowConsumer<MsgObject> consumer = ziploq.registerUnordered(10L, 5, getStrategy(), TEST_SOURCE, Optional.of(COMPARATOR));
        consumer.complete();
        consume(consumer, OBJECT_1, ZERO, ZERO);
        fail();
    }
    
    @Test(expected = IllegalStateException.class)
    public void updateSystemTimeAfterComplete() {
        ZipFlow<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        FlowConsumer<MsgObject> consumer = ziploq.registerUnordered(10L, 5, getStrategy(), TEST_SOURCE, Optional.of(COMPARATOR));
        consumer.complete();
        consumer.updateSystemTime(TS_1);
        fail();
    }
    
}
