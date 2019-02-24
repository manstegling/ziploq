# Ziploq


An ingeniously simple device for merging and sequencing data from any number of data sources. It doesn't matter whether the input data is ordered or not, the output will always be ordered!

### The pitch

`Ziploq` is a lightweight message-synchronization device with ultra-high performance. It let's you quickly join data from multiple sources in a reactive way, without having to go for bulky universal solutions such as `Project Reactor` or `Akka Streams`. With this tool, it's possible to merge and sequence up to 20 million msgs/s for ordered input and 10 million msgs/s for unordered input -- on a regular desktop computer!

For your convenience, `Ziploq` also provides strategies for handling backpressure on both Producer and Consumer side.

### Using Ziploq

Create a new `Ziploq` instance using the factory

```java
Ziploq<MyMsg> ziploq = ZiploqFactory.create(systemDelay, msgComparator);
```

Register a new _ordered_ input source

```java
SynchronizedConsumer<MyMsg> consumer = ziploq.registerOrdered(capacity, backPressureStrategy); 
```

or an _unordered_ input source

```java
SynchronizedConsumer<MyMsg> consumer = ziploq.registerUnordered(businessDelay, capacity, backPressureStrategy, comparator)); 
```

Feed messages into the `Ziploq` machinery by calling `consumer.onEvent(msg, businessTs, systemTs)` for each incoming message. To publish updates to the system clock at times when no new messages are available, call `updateSystemTime(systemTs)`. When no more messages are available, call `complete()`.

The merged output can be obtained as a regular Java `Stream` that you can use to set up your reactive data processing pipe

```java
ziploq.stream()
      //map, filter, and so on....
      .forEach(result -> doSomethingUseful(result));
```

The stream will emit messages when available, or otherwise block. When `complete()` has been called for all input consumers and all messages have been processed, the `Stream` will terminate.

It is also possible to retrieve the ordered output by using old-school methods `take()` and `poll()`.

### Understanding the vector clock

Sorting of unordered data is carried out [on-line](https://en.wikipedia.org/wiki/Online_algorithm). You only need to know how much delayed messages can be (at most) in terms of the _business clock_ compared to previously processed messages from the same source. Setting the `businessDelay` to, for example, 100ms means we never expect to see a message with business timestamp `T-101` coming in after a message from the same source with business timestamp `T` has already been processed.

The _business clock_ is ideally based on epoch millisecond timestamps from a global business clock. However, timestamps are handled internally as `long` values and any integer, such as a global sequence number, can be used. Messages are always sequenced first with respect to the business clock. In high-frequency applications, secondary sorting criteria can easily be configured to provide sub-millisecond (total) ordering.

`Ziploq` has been designed with real-time processing in mind. Therefore, a secondary clock has been introduced, which is called the _system clock_.  Making each producer continuously publish their system time allows for advancing the message sequence even at times when input sources are silent. This mechanism can be considered a modern form of heartbeating; in which all sources drive event time together.

When dealing with real-time processing, the system clock should be driven by wall-clock time. In this way, it's just a matter of configuration when it comes to how much real-time delay is acceptable within your application.


### Background and rationale

There's no way to merge multiple ordered message streams built into the Java language. This is a significant gap impacting many message-passing and data crunching applications. Even if there were a simple method for merging streams in an efficient way while maintaining order, in many real-world scenarios we don't event have access to ordered input! Due to multicast protocols, weird upstream architecture or data transmission latencies, the input data reaching us often comes out-of-order in some way or another.

To address these issues I've created `Ziploq`. With this device it becomes dead simple to merge and sequence message streams from any number of sources, even if the input data is unordered.

### Other stuff

The classes `OrderedSyncQueue` and `UnorderedSyncQueue` are useful on their own for Single-Producer Single-Consumer (SPSC) scenarios. The latter comes with built-in on-line sorting for queued messages based on a vector clock. They're both lock-free concurrent SPSC queues built on top of data structures from the lightning-fast [JCTools](https://github.com/JCTools/JCTools) library. Create instances of these queues using the associated `SpscSyncQueueFactory`.

_Note:_ Illustrations describing how the vector clock and sorting mechanism work are in the making.

### Leave feedback

Feel free to reach out if you have any questions or queries! For issues, please use Github's issue tracking functionality.

 ----

Måns Tegling, 2019
