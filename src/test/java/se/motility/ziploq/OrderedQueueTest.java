package se.motility.ziploq;

import org.junit.Test;
import se.motility.ziploq.SyncTestUtils.MsgObject;
import se.motility.ziploq.SyncTestUtils.TestEntry;
import se.motility.ziploq.impl.SyncQueue;
import se.motility.ziploq.impl.SyncQueueFactory;
import se.motility.ziploq.impl.SyncQueueFactory.CapacityType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_1;

public class OrderedQueueTest {

    @Test(expected = IllegalArgumentException.class)
    public void zeroCapacity() {
        SyncQueue<Object> q = SyncQueueFactory.createOrdered(0, CapacityType.BOUNDED);
        assertNotNull(q);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void nullCapacityType() {
        SyncQueue<Object> q = SyncQueueFactory.createOrdered(1, null);
        assertNotNull(q);
    }
    
    @Test
    public void peek() {
        SyncQueue<MsgObject> q = SyncQueueFactory.createOrdered(1, CapacityType.BOUNDED);
        TestEntry e1 = new TestEntry(OBJECT_1, 0L, 0L, true);
        assertTrue(q.offer(e1));
        assertEquals(1, q.readySize());
        assertEquals(1, q.size());
        assertEquals(e1, q.peek());
        assertEquals(1, q.readySize());
        assertEquals(1, q.size());
    }
    
    @Test
    public void boundedCapacity() {
        int capacity = 16; // Must be power of 2
        SyncQueue<MsgObject> q = SyncQueueFactory.createOrdered(capacity, CapacityType.BOUNDED);
        assertEquals(capacity, q.remainingCapacity());
        
        //Insert 'capacity' messages to fill the queue
        for (int i = 0; i < capacity; i++) {
            assertTrue(q.offer(new TestEntry(OBJECT_1, i, 0L, true)));
            int added = i + 1;
            assertEquals(added, q.readySize());                    // 1, 2,..., capacity            (ready messages)
            assertEquals(added, q.size());                         // 1, 2,..., capacity+1          (total messages)
            assertEquals(capacity - added, q.remainingCapacity()); // capacity-1, capacity-2,..., 0 (non-ready messages do not count)
        }
        
        // Confirm state
        assertEquals(0, q.remainingCapacity());
        assertEquals(capacity, q.readySize());
        assertEquals(capacity, q.size());
        
        // Insert message
        assertFalse(q.offer(new TestEntry(OBJECT_1, capacity + 1, 0L, true))); // Rejected
        assertEquals(0, q.remainingCapacity());
        assertEquals(capacity, q.readySize());
        assertEquals(capacity, q.size());
    }
    
    @Test
    public void unboundedCapacity() {
        int capacity = 128; //<-- Magic number matching the granularity of capacity checks in unbounded queues
        SyncQueue<MsgObject> q = SyncQueueFactory.createOrdered(capacity, CapacityType.UNBOUNDED);
        assertEquals(capacity, q.remainingCapacity()); // Power of 2
        
        //Insert 'capacity' messages to fill the queue
        for (int i = 0; i < capacity; i++) {
            assertTrue(q.offer(new TestEntry(OBJECT_1, i, 0L, true)));
            int added = i + 1;
            assertEquals(added, q.readySize());                    // 1, 2,..., capacity            (ready messages)
            assertEquals(added, q.size());                         // 1, 2,..., capacity+1          (total messages)
            assertEquals(capacity - added, q.remainingCapacity()); // capacity-1, capacity-2,..., 0 (non-ready messages do not count)
        }
        
        // Confirm state
        assertEquals(0, q.remainingCapacity());
        assertEquals(capacity, q.readySize());
        assertEquals(capacity, q.size());
        
        // Insert message
        assertFalse(q.offer(new TestEntry(OBJECT_1, capacity + 1, 0L, true))); // Rejected
        assertEquals(0, q.remainingCapacity());
        assertEquals(capacity + 1, q.readySize());  // The last message is actually _accepted_ even though returning 'false'!
        assertEquals(capacity + 1, q.size());       // The last message is actually _accepted_ even though returning 'false'!
    }
}
