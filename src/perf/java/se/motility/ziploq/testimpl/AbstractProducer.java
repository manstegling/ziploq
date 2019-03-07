package se.motility.ziploq.testimpl;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.motility.ziploq.api.SynchronizedConsumer;
import se.motility.ziploq.testapi.Producer;

public abstract class AbstractProducer implements Producer {
    
    public static final long START_TIME = 1_540_000_000L;
    private static final AtomicInteger ID = new AtomicInteger();
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    public final Object object = new Object();
    public final Thread thread;
    public final Random random;
    
    public AbstractProducer(SynchronizedConsumer<Object> consumer,
            int messages, Runnable waitStrategy) {
        Runnable task = () -> produce(consumer, messages, waitStrategy);
        String threadName = getClass().getSimpleName() + "-" + ID.incrementAndGet();
        this.thread = new Thread(task, threadName);
        this.random = new Random(thread.getName().hashCode());
    }
        
    private void produce(SynchronizedConsumer<Object> consumer, int messages,
            Runnable waitStrategy) {
        long businessTime = START_TIME;
        long systemTime = START_TIME;
        for (int i=0; i<messages; i++) {
            businessTime = getNextBusinessTs(i, businessTime, systemTime);
            systemTime = getNextSystemTs(i, businessTime, systemTime);
            if (isEvent(i)) {
                while(!consumer.onEvent(object, businessTime, systemTime)) {
                    waitStrategy.run();
                }
            } else {
                consumer.updateSystemTime(systemTime);
            }
        }
        consumer.complete();
        log.info("Thread {} finished successfully.", thread.getName());
    }

    protected abstract long getNextBusinessTs(int messageNo, long businessTs, long systemTs);
    protected abstract long getNextSystemTs(int messageNo, long businessTs, long systemTs);
    protected abstract boolean isEvent(int messageNo);
    
    @Override
    public String toString() {
        return "Thread: " + thread.getName() + " ready to append messages.";
    }

    @Override
    public Thread getThread() {
        return thread;
    }
    
}
