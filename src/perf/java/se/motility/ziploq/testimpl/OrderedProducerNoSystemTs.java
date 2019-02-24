package se.motility.ziploq.testimpl;

import se.motility.ziploq.api.SynchronizedConsumer;

public class OrderedProducerNoSystemTs extends OrderedProducer {

    public OrderedProducerNoSystemTs(SynchronizedConsumer<Object> consumer,
            int messages, double messagesPerMilli, Runnable waitStrategy) {
        super(consumer, messages, messagesPerMilli, waitStrategy);
    }

    @Override
    protected long getNextSystemTs(int messageNo, long businessTs, long systemTs) {
        return Long.MAX_VALUE;
    }
    
}
