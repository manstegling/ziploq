package se.motility.ziploq.testimpl;

import se.motility.ziploq.api.FlowConsumer;

public class OrderedProducer extends AbstractProducer {

    private final double messagesPerMilli;
    
    public OrderedProducer(FlowConsumer<Object> consumer,
            int messages, double messagesPerMilli, Runnable waitStrategy) {
        super(consumer, messages, waitStrategy);
        this.messagesPerMilli = messagesPerMilli;
    }

    @Override
    protected long getNextBusinessTs(int messageNo, long businessTs, long systemTs) {
        return START_TIME + (int) (messageNo / messagesPerMilli);
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
