# Ziploq


An ingeniously simple device for merging and sequencing data from any number of data sources. It doesn't matter whether the input data is ordered or not, the output will always be ordered!

### The pitch

`Ziploq` is a lightweight message-synchronization device with ultra-high performance. It let's you quickly join data from multiple sources in a reactive way, without having to go for bulky universal solutions such as `Project Reactor` or `Akka Streams`. With this tool, it's possible to merge and sequence up to 20 million msgs/s for ordered input and 10 million msgs/s for unordered input -- on a regular desktop computer!

For your convenience, `Ziploq` also provides strategies for handling backpressure on both Producer and Consumer side.

#### Normal use case (illustration)

![Ziploq; merging input sources](https://raw.githubusercontent.com/manstegling/ziploq/master/images/ziploq.png)

### Using Ziploq

Create a new `Ziploq` instance using the factory

```java
Ziploq<MyMsg> ziploq = ZiploqFactory.create(msgComparator);
```

Register a new _ordered_ input source

```java
SynchronizedConsumer<MyMsg> consumer = ziploq.registerOrdered(capacity, backPressureStrategy); 
```

or an _unordered_ input source

```java
SynchronizedConsumer<MyMsg> consumer = ziploq.registerUnordered(businessDelay, capacity, backPressureStrategy, comparator)); 
```

Feed messages into the `Ziploq` machinery by calling `consumer.onEvent(msg, businessTs)` for each incoming message. When no more messages are available, call `complete()`.

The merged output can be obtained as a regular Java `Stream` that you can use to set up your reactive data processing pipe

```java
ziploq.stream()
      //map, filter, and so on....
      .forEach(result -> doSomethingUseful(result));
```

The stream will emit messages when available, or otherwise block. When `complete()` has been called for all input consumers and all messages have been processed, the `Stream` will terminate.

It is also possible to retrieve the ordered output by using old-school methods `take()` and `poll()`.

### ZipFlow keeps data flowing (even when there's no input)

If you're developing a real-time application that has to keep pushing messages even when some input data sources are not producing any--`ZipFlow` is the way to go. To create an instance, do exactly as before but also provide a `systemDelay` parameter:

```java
ZipFlow<MyMsg> zipflow = ZiploqFactory.create(systemDelay, msgComparator);
```

When registering input data sources with `ZipFlow` you'll get a turbo-charged flavor of the `SynchronizedConsumer` called a `FlowConsumer`. These consumers use a vector clock consisting of both business time and system time, where the latter is used for heartbeating functionality. This allows messages to be dispatched through the synchronizer even at times when some input sources are silent, as long as they progress their system time.

To publish updates for the system clock at times when no new messages are available, simply call `consumer.updateSystemTime(systemTs)`.

### The vector clock explained

##### Unordered data

Sorting of unordered data is carried out [on-line](https://en.wikipedia.org/wiki/Online_algorithm). You only need to know how much delayed messages can be (at most) in terms of the _business clock_ compared to previously processed messages from the same source. Setting the `businessDelay` to, for example, 100ms means we never expect to see a message with business timestamp _T-101_ coming in after a message from the same source with business timestamp _T_ has already been processed.

The _business clock_ is ideally based on epoch millisecond timestamps from a global business clock. However, business time is completely decoupled from system time and timestamps are handled internally as `long` values so any integer, such as a global sequence number, can be used. Messages are always sequenced first with respect to the business clock. In high-frequency applications, secondary sorting criteria can easily be configured to provide sub-millisecond (total) ordering.

##### Real-time applications

`ZipFlow` has been designed with real-time processing in mind. Therefore, a secondary clock has been introduced, which is called the _system clock_. Making each producer continuously publish their system time allows for advancing the message sequence even at times when input sources are silent. This mechanism can be considered a modern form of heartbeating; in which all sources drive event time together. The heartbeating frequency is governed by the `systemDelay` parameter; during normal operation it is expected that system time is progressed in steps not larger than the configured `systemDelay`.

When dealing with real-time processing, the system clock should be driven by wall-clock time. In this way, it's just a matter of configuration when it comes to how much real-time delay is acceptable within your application.

In many real-world applications the data is coming from external data sources. In case a data connection goes down, the producer should stop publishing system time. Instead, simply wait until the connection has been re-established and then start publishing messages again. From a vector clock perspective this might involve a jump in system (wall-clock) time, but no immediate jump in business time. `Ziploq` will automatically detect such a jump in the producer's system clock and initiate a recovery grace period for that producer (in system time) to complete its recovery and, thus, guarantee output sequencing is not impacted.

### Background and rationale

There's no way to merge multiple ordered message streams built into the Java language. This is a significant gap impacting many message-passing and data crunching applications. Even if there were a simple method for merging streams in an efficient way while maintaining order, in many real-world scenarios we don't event have access to ordered input! Due to multicast protocols, weird upstream architecture or data transmission latencies, the input data reaching us often comes out-of-order in some way or another.

### Logging

This library uses SLF4J for logging, so please make sure you've got your logger configured to handle this. Log entries will be written if any input sources break the ordering contract resulting in out-of-sequence message flows.

By default, only a non-decreasing business timestamp sequence is enforced. If you'd want the framework to also check that the input data is compliant with the configured _Comparator_ (if any) set the system property `ziploq.log.comparator_compliant=true`.

The framework also warns when the blocking action to wait for output has not returned within a certain time-frame. If this happens, information about the sources that are silent--causing the block--will be provided. The default timeout is 120000 milliseconds (2 minutes) and can be adjusted with the system property `ziploq.log.wait_timeout`.


### Other stuff

If there's a known systematic time discrepancy between producers, this discrepancy must be added to the `systemDelay` parameter. For example, if producer `B` most of the time provide a system timestamp 500ms later than producer `A` for messages having the same business timestamp, do add 500ms to your desired value for the system time delay parameter to offset this.

The classes `OrderedSyncQueue` and `UnorderedSyncQueue` are useful on their own for Single-Producer Single-Consumer (SPSC) scenarios. The latter comes with built-in on-line sorting for queued messages based on a vector clock. They're both lock-free concurrent SPSC queues built on top of data structures from the lightning-fast [JCTools](https://github.com/JCTools/JCTools) library. Create instances of these queues directly using the associated `SpscSyncQueueFactory`.

_Note:_ Illustrations describing how the vector clock and sorting mechanism work are in the making.

### Code example

Creating a `Ziploq` instance, registering two ordered input data sources and then operating on the merged, ordered output is done in the following way

```java
public static void main(String[] args) {
    //Create ziploq
    Comparator<MyMsg> tiebreaker = Comparator.comparing(MyMsg::getMessage);
    Ziploq<MyMsg> ziploq = ZiploqFactory.create(Optional.of(tiebreaker));
    
    //Register input data sources
    Producer producerA = new Producer("Producer-A",
        ziploq.registerOrdered(100, BackPressureStrategy.BLOCK, "Source-A"));
    Producer producerB = new Producer("Producer-B",
        ziploq.registerOrdered(100, BackPressureStrategy.BLOCK, "Source-B"));
    
    //Start data producers
    producerA.start();
    producerB.start();
    
    //Retrieve an ordered output stream
    ziploq.stream()
          .map(Entry::getMessage)
          .map(MyMsg::getMessage)
          .forEach(System.out::println);
}
```

In the simplest case, consider `Producer` and `MyMsg` classes as per below

```java
    static class Producer extends Thread {
        private final SynchronizedConsumer<MyMsg> consumer;
        private final String id;
        public Producer(String id, SynchronizedConsumer<MyMsg> consumer) {
            this.id = id;
            this.consumer = consumer;
        }
        @Override
        public void run() {
            IntStream.range(0, 1001)
                     .mapToObj(i -> new MyMsg(id+"-"+i, i))
                     .forEach(msg -> consumer.onEvent(msg, msg.getTimestamp()));
            consumer.complete();
        }
    }
    
    static class MyMsg {
        private final String message;
        private final long timestamp;
        public MyMsg(String message, long timestamp) {
            this.message = message;
            this.timestamp = timestamp;
        }
        public String getMessage() {
            return message;
        }
        public long getTimestamp() {
            return timestamp;
        }
    }
```

Running this program will output messages from 0 to 1000. The `tiebreaker` comparator guarantees that "Adaptor-A" messages always come before "Adaptor-B" messages when timestamps are equal.

```
Adaptor-A-0
Adaptor-B-0
Adaptor-A-1
Adaptor-B-1
...
Adaptor-A-999
Adaptor-B-999
Adaptor-A-1000
Adaptor-B-1000
```


### Leave feedback

Feel free to reach out if you have any questions or queries! For issues, please use Github's issue tracking functionality.

 ----

Måns Tegling, 2019
