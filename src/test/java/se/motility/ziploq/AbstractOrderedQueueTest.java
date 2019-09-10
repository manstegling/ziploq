package se.motility.ziploq;

import static org.junit.Assert.*;
import static se.motility.ziploq.SyncTestUtils.*;
import static se.motility.ziploq.SyncTestUtils.MsgObject.*;

import java.util.Optional;

import org.junit.Test;

import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.SynchronizedConsumer;
import se.motility.ziploq.api.Ziploq;
import se.motility.ziploq.api.ZiploqFactory;

public abstract class AbstractOrderedQueueTest {

    static final String TEST_SOURCE = "SOURCE";
    
    @Test
    public void produceConsumeProduceConsume() {
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer = ziploq.registerOrdered(5, getStrategy(), TEST_SOURCE);
        
        assertNull(ziploq.poll());
        
        //E1: (TS1, 0)
        TestEntry e1 = consume(consumer, OBJECT_1, TS_1,     ZERO);
        
        verify(e1, ziploq.poll());
        assertNull(ziploq.poll());
        
        //E2: (TS1, 0)
        TestEntry e2 = consume(consumer, OBJECT_2, TS_1,     ZERO);
        
        verify(e2, ziploq.poll());
        assertNull(ziploq.poll());
        
        //E3: (TS1+1, 0)
        TestEntry e3 = consume(consumer, OBJECT_1, TS_1 + 1, ZERO);
        
        verify(e3, ziploq.poll());
        assertNull(ziploq.poll());
    }
    
    @SuppressWarnings("unused")
    @Test
    public void syncTwoConsumers() {
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerOrdered(5, getStrategy(), TEST_SOURCE);
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerOrdered(5, getStrategy(), TEST_SOURCE);
        
        //E1: (TS1+1, 0, c1)
        TestEntry e1 = consume(consumer1, OBJECT_1, TS_1 + 1, ZERO); //#2
        assertNull(ziploq.poll());  
        
        //E2: (TS1, 0, c2)
        TestEntry e2 = consume(consumer2, OBJECT_2, TS_1,     ZERO); //#1
        
        verify(e2, ziploq.poll()); //E2
        assertNull(ziploq.poll());         
    }
    
    @SuppressWarnings("unused")
    @Test
    public void syncThreeConsumers() {
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer1 = ziploq.registerOrdered(5, getStrategy(), TEST_SOURCE);
        SynchronizedConsumer<MsgObject> consumer2 = ziploq.registerOrdered(5, getStrategy(), TEST_SOURCE);
        SynchronizedConsumer<MsgObject> consumer3 = ziploq.registerOrdered(5, getStrategy(), TEST_SOURCE);
        
        //E1: (TS1+10, 0, c1)
        TestEntry e1 = consume(consumer1, OBJECT_1, TS_1 + 10, ZERO); //#5
        
        //E2: (TS1, 0, c2)
        TestEntry e2 = consume(consumer2, OBJECT_1, TS_1,      ZERO); //#1
        
        //E3: (TS1+1, 0, c2)
        TestEntry e3 = consume(consumer2, OBJECT_1, TS_1 + 1,  ZERO); //#2
        
        assertNull(ziploq.poll());
        
        //E4: (TS1+2, 0, c3)
        TestEntry e4 = consume(consumer3, OBJECT_1, TS_1 + 2,  ZERO); //#3
        
        verify(e2, ziploq.poll());
        verify(e3, ziploq.poll());
        assertNull(ziploq.poll());
        
        //E5: (TS1+3,0,c2)
        TestEntry e5 = consume(consumer2, OBJECT_1, TS_1 + 3,  ZERO); //#4
        
        verify(e4, ziploq.poll());
        assertNull(ziploq.poll());
        
    }
    
    @Test
    public void produceFullCapacityAndConsume() {
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer = ziploq.registerOrdered(5, getStrategy(), TEST_SOURCE);
        //E1: (TS1, 0)
        TestEntry e1 = consume(consumer, OBJECT_1, TS_1,   ZERO);
        //E2: (TS1, 0)
        TestEntry e2 = consume(consumer, OBJECT_2, TS_1,   ZERO);
        //E3: (TS1+1, 0)
        TestEntry e3 = consume(consumer, OBJECT_1, TS_1+1, ZERO);
        //E4: (TS1+1, 0)
        TestEntry e4 = consume(consumer, OBJECT_2, TS_1+1, ZERO);
        //E5: (TS1+10, 0)
        TestEntry e5 = consume(consumer, OBJECT_1, TS_1+10, ZERO);
        
        verify(e1, ziploq.poll());
        verify(e2, ziploq.poll());
        verify(e3, ziploq.poll());
        verify(e4, ziploq.poll());
        verify(e5, ziploq.poll());
        assertNull(ziploq.poll());
    }
    
    @Test(expected = IllegalStateException.class)
    public void onEventAfterComplete() {
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer = ziploq.registerOrdered(5, getStrategy(), TEST_SOURCE);
        consumer.complete();
        consume(consumer, OBJECT_1, ZERO, ZERO);
        fail();
    }
    
    @Test(expected = IllegalStateException.class)
    public void updateSystemTimeAfterComplete() {
        Ziploq<MsgObject> ziploq = ZiploqFactory.create(100L, Optional.empty());
        SynchronizedConsumer<MsgObject> consumer = ziploq.registerOrdered(5, getStrategy(), TEST_SOURCE);
        consumer.complete();
        consumer.updateSystemTime(TS_1);
        fail();
    }
    
    
    abstract BackPressureStrategy getStrategy();
    
}
