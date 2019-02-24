package se.motility.ziploq.testimpl;

import se.motility.ziploq.api.SynchronizedConsumer;

public class UnorderedProducerWithDrift extends AbstractProducer {

    private static final int INCREMENT = 2;
    
    private final int maxDelay;
    private final int maxDrift;
    
    private boolean incrementBusinessTs;
    private long businessProgress;
    private long systemProgress;
    
    public UnorderedProducerWithDrift(SynchronizedConsumer<Object> consumer, int messages,
            int maxDrift, int maxDelay, Runnable waitStrategy) {
        super(consumer, messages, waitStrategy);
        this.maxDrift = maxDrift;
        this.maxDelay = maxDelay;
    }

    @Override
    protected long getNextBusinessTs(int messageNo, long businessTs, long systemTs) {
        //random walk with hard boundaries
        long delta = businessTs - systemTs;
        double threshold;
        if (delta <= -maxDrift + INCREMENT) {
            threshold = 0d;
        } else if (delta >= maxDrift - INCREMENT) {
            threshold = 1d;
        } else {
            threshold = 0.5;
        }
        incrementBusinessTs = random.nextDouble() > threshold;
        if (incrementBusinessTs) {
            businessProgress += 2;
        }
        int delayBound = (int) Math.min(Math.max(maxDrift + delta - 2,  0), maxDelay);
        return businessProgress - random.nextInt(delayBound);
    }

    @Override
    protected long getNextSystemTs(int messageNo, long businessTs, long systemTs) {
        if (!incrementBusinessTs) {
            systemProgress += 2;
        }
        return systemProgress;
    }

    @Override
    protected boolean isEvent(int messageNo) {
        return true;
    }

}
