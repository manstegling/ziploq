package se.motility.ziploq;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Test;
import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.FlowSource;
import se.motility.ziploq.api.ZiploqFactory;
import se.motility.ziploq.api.Zipq;

import static org.junit.Assert.assertNull;
import static se.motility.ziploq.SyncTestUtils.COMPARATOR;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_1;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_2;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_3;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_4;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_5;
import static se.motility.ziploq.SyncTestUtils.MsgObject.OBJECT_6;
import static se.motility.ziploq.SyncTestUtils.TS_1;
import static se.motility.ziploq.SyncTestUtils.ZERO;
import static se.motility.ziploq.SyncTestUtils.consume;
import static se.motility.ziploq.SyncTestUtils.verify;

public class ManagedZiploqTest {

    private static final String TEST_SOURCE = "SOURCE";

    /**
     * Test that system delay mechanism is working correctly
     */
    @Test(timeout = 3_000L)
    public void systemTs() throws InterruptedException {
        long delay = 5L;

        Queue<SyncTestUtils.TestEntry> q1 = new ConcurrentLinkedQueue<>();
        TestSource source1 = new TestSource(q1, ZERO);

        Queue<SyncTestUtils.TestEntry> q2 = new ConcurrentLinkedQueue<>();
        TestSource source2 = new TestSource(q2, ZERO);

        Zipq<SyncTestUtils.MsgObject> ziploq = ZiploqFactory
                .managedZipFlowBuilder(delay, COMPARATOR)
                .setPoolSize(2)
                .registerUnordered(source1, delay, 5, TEST_SOURCE, null)
                .registerUnordered(source2, delay, 5, TEST_SOURCE, null)
                .create();

        //Add two messages to c2; none is ready
        SyncTestUtils.TestEntry e1 = consume(q2, OBJECT_4, TS_1, ZERO);
        SyncTestUtils.TestEntry e2 = consume(q2, OBJECT_5, TS_1, ZERO + delay);

        //Add three messages to c1, first two messages are ready
        SyncTestUtils.TestEntry e3 = consume(q1, OBJECT_1, TS_1, ZERO);
        SyncTestUtils.TestEntry e4 = consume(q1, OBJECT_2, TS_1, ZERO);

        source1.updateSystemTs(ZERO + delay);           //signal that we are still alive

        SyncTestUtils.TestEntry e5 = consume(q1, OBJECT_3, TS_1, ZERO + delay + 1); //release in c1
        assertNull(ziploq.poll());                                           //still awaiting c2

        SyncTestUtils.TestEntry e6 = consume(q2, OBJECT_6, TS_1, ZERO + delay + 1); //release in c2

        verify(e3, ziploq.take()); //take() to give worker threads time to complete their part
        verify(e4, ziploq.take());
        verify(e1, ziploq.take());
        assertNull(ziploq.poll());
    }

    private static class TestSource implements FlowSource<SyncTestUtils.MsgObject> {
        private final Queue<SyncTestUtils.TestEntry> queue;
        private long systemTs;

        TestSource(Queue<SyncTestUtils.TestEntry> queue, long startSystemTs) {
            this.queue = queue;
            this.systemTs = startSystemTs;
        }
        @Override
        public Entry<SyncTestUtils.MsgObject> emit() {
            Entry<SyncTestUtils.MsgObject> e = queue.poll();
            if (e != null) {
                systemTs = e.getSystemTs();
                return e;
            } else {
                return null;
            }
        }
        @Override
        public long getSystemTs() {
            return systemTs;
        }
        public void updateSystemTs(long systemTs) {
            this.systemTs = systemTs;
        }
    }

}
