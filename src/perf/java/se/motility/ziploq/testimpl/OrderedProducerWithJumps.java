package se.motility.ziploq.testimpl;

import se.motility.ziploq.api.SynchronizedConsumer;

public class OrderedProducerWithJumps extends AbstractProducer {
    
    private final int jumpFreq;
    private final int jumpBound;
    
    public OrderedProducerWithJumps(SynchronizedConsumer<Object> consumer,
            int messages, int jumpFreq, int jumpBound, Runnable waitStrategy) {
        super(consumer, messages, waitStrategy);
        this.jumpFreq = jumpFreq;
        this.jumpBound = jumpBound;
    }

    @Override
    protected long getNextBusinessTs(int messageNo, long businessTs, long systemTs) {
        if(messageNo % jumpFreq == 0) {
            return businessTs + random.nextInt(jumpBound) + 1;
        } else {
            return businessTs + 1;
        }
    }

    @Override
    protected long getNextSystemTs(int messageNo, long businessTs, long systemTs) {
        return businessTs;
    }

    @Override
    protected boolean isEvent(int messageNo) {
        return true;
    }
       
}
