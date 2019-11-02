package se.motility.ziploq;

import java.util.Optional;

import org.junit.Test;
import se.motility.ziploq.SyncTestUtils.AsyncTestThread;
import se.motility.ziploq.SyncTestUtils.MsgObject;
import se.motility.ziploq.SyncTestUtils.SequenceChecker;
import se.motility.ziploq.api.AdvancedZiploq;
import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.FlowConsumer;
import se.motility.ziploq.api.ZipFlow;
import se.motility.ziploq.api.ZiploqFactory;

import static org.junit.Assert.assertEquals;
import static se.motility.ziploq.SyncTestUtils.*;

public class AdvancedZiploqTest {

    private static final String TEST_SOURCE = "SOURCE";
    
    @Test(timeout=10_000)
    public void advancedStreamFromThree() {
        long delay = 1000L;
        int messages = 1000;
        
        ZipFlow<MsgObject> ziploq = ZiploqFactory.create(delay, Optional.empty());
        FlowConsumer<MsgObject> consumer1 = ziploq.registerOrdered(5, BackPressureStrategy.BLOCK, TEST_SOURCE);
        FlowConsumer<MsgObject> consumer2 = ziploq.registerOrdered(5, BackPressureStrategy.BLOCK, TEST_SOURCE);
        FlowConsumer<MsgObject> consumer3 = ziploq.registerUnordered(
                10, 5, BackPressureStrategy.BLOCK, TEST_SOURCE, Optional.empty());

        
        AsyncTestThread t1 = new AsyncTestThread(() -> addToQueue(consumer1, messages));
        AsyncTestThread t2 = new AsyncTestThread(() -> addToQueue(consumer2, messages));
        AsyncTestThread t3 = new AsyncTestThread(() -> addToQueueUnordered(consumer3, messages));
        
        SequenceChecker checker = new SequenceChecker();
        AdvancedZiploq.streamWithWorker(ziploq, 5)
                      .forEach(checker::verify);
        
        assertEquals(3*messages, checker.getTotal());
        
        t1.join();
        t2.join();
        t3.join();
    }
    
}
