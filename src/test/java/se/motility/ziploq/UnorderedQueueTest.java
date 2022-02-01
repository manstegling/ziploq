package se.motility.ziploq;

import org.junit.Test;
import se.motility.ziploq.SyncTestUtils.TestEntry;
import se.motility.ziploq.impl.SpscSyncQueue;
import se.motility.ziploq.impl.SpscSyncQueueFactory;
import se.motility.ziploq.impl.SpscSyncQueueFactory.CapacityType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static se.motility.ziploq.SyncTestUtils.*;
import static se.motility.ziploq.SyncTestUtils.MsgObject.*;

public class UnorderedQueueTest {
    
    @Test(expected = IllegalArgumentException.class)
    public void negativeBusinessdelay() {
        SpscSyncQueue<Object> q = SpscSyncQueueFactory.createUnordered(-1L, 0L, 10, CapacityType.BOUNDED, null);
        assertNotNull(q);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void businessDelayHigherThanSystemDelay() {
        SpscSyncQueue<Object> q = SpscSyncQueueFactory.createUnordered(1L, 0L, 10, CapacityType.BOUNDED, null);
        assertNotNull(q);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void nonPositiveCapacity() {
        SpscSyncQueue<Object> q = SpscSyncQueueFactory.createUnordered(1L, 1L, 0, CapacityType.BOUNDED, null);
        assertNotNull(q);
    }

    @Test
    public void peek() {
        SpscSyncQueue<MsgObject> q = SpscSyncQueueFactory.createUnordered(0L, 10L, 1, CapacityType.UNBOUNDED, null);
        TestEntry e1 = new TestEntry(OBJECT_1, 0L, 0L, true);
        TestEntry e2 = new TestEntry(OBJECT_2, 1L, 0L, true);
        assertTrue(q.offer(e1));
        assertTrue(q.offer(e2));
        assertEquals(1, q.readySize());
        assertEquals(2, q.size());
        assertEquals(e1, q.peek());
        assertEquals(1, q.readySize());
        assertEquals(2, q.size());
    }
    
    @Test
    public void boundedCapacity() {
        int capacity = 10;
        SpscSyncQueue<MsgObject> q = SpscSyncQueueFactory.createUnordered(0L, 10L, capacity, CapacityType.BOUNDED, null);
        assertEquals(capacity, q.remainingCapacity());
        
        //Insert 'capacity + 1' messages (all are accepted due to "softCapacity" nature of unordered queues)
        for (int i = 0; i < capacity + 1 ; i++) {
            assertTrue(q.offer(new TestEntry(OBJECT_1, i, 0L, true)));
            assertEquals(i, q.readySize());                    // 0, 1,..., capacity            (ready messages)
            assertEquals(i + 1, q.size());                     // 1, 2,..., capacity+1          (total messages)
            assertEquals(capacity - i, q.remainingCapacity()); // capacity-0, capacity-1,..., 0 (non-ready messages do not count)
        }
        
        // At this point the queue is indeed full ('capacity' ready messages + 1 staged)
        // The next message will be rejected, but have the side-effect of promoting the final staged message
        
        // Confirm state
        assertEquals(0, q.remainingCapacity());
        assertEquals(capacity, q.readySize());
        assertEquals(capacity + 1, q.size());
        
        // Insert message
        assertFalse(q.offer(new TestEntry(OBJECT_1, capacity + 1, 0L, true))); // Rejected
        assertEquals(0, q.remainingCapacity());
        assertEquals(capacity + 1, q.readySize()); // Ready size is now 'capacity + 1' since the final staged message has been promoted
        assertEquals(capacity + 1, q.size());
    }
    
    @Test
    public void unboundedCapacity() {
        int capacity = 127; //<-- Magic number matching the granularity of capacity checks in unbounded queues (128 when including 1 staged)
        SpscSyncQueue<MsgObject> q = SpscSyncQueueFactory.createUnordered(0L, 10L, capacity, CapacityType.UNBOUNDED, null);
        assertEquals(capacity, q.remainingCapacity());
        
        //Insert 'capacity + 1' messages (all are accepted due to "softCapacity" nature of unordered queues)
        for (int i = 0; i < capacity + 1 ; i++) {
            assertTrue(q.offer(new TestEntry(OBJECT_1, i, 0L, true)));
            assertEquals(i, q.readySize());                    // 0, 1,..., capacity            (ready messages)
            assertEquals(i + 1, q.size());                     // 1, 2,..., capacity+1          (total messages)
            assertEquals(capacity - i, q.remainingCapacity()); // capacity-0, capacity-1,..., 0 (non-ready messages do not count)
        }
        
        // At this point the queue is indeed full ('capacity' ready messages + 1 staged)
        // The next message will be rejected, but have the side-effect of promoting the final staged message
        
        // Confirm state
        assertEquals(0, q.remainingCapacity());
        assertEquals(capacity, q.readySize());
        assertEquals(capacity + 1, q.size());
        
        // Insert message
        assertFalse(q.offer(new TestEntry(OBJECT_1, capacity + 1, 0L, true))); // Rejection signal (but message is not dropped!)
        assertEquals(0, q.remainingCapacity());
        assertEquals(capacity + 1, q.readySize());
        assertEquals(capacity + 2, q.size());      // The last message is actually _accepted_ even though returning 'false'!
    }
    
}
