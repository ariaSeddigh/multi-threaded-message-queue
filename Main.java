import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.time.Instant;

// Class for Message
class Message {
    private String id;
    private String content;
    private long timestamp; // For TTL
    private long ttl; // Time to live in milliseconds
    private int priority; // Priority level (higher number = higher priority)
    private long estimatedTime; // Estimated execution time in ms

    public Message(String id, String content, long ttl, int priority, long estimatedTime) {
        this.id = id;
        this.content = content;
        this.timestamp = Instant.now().toEpochMilli();
        this.ttl = ttl;
        this.priority = priority;
        this.estimatedTime = estimatedTime;
    }

    public String getId() {
        return id;
    }

    public String getContent() {
        return content;
    }

    public boolean isExpired() {
        return ttl > 0 && (Instant.now().toEpochMilli() - timestamp) > ttl;
    }

    public int getPriority() {
        return priority;
    }

    public long getEstimatedTime() {
        return estimatedTime;
    }

    @Override
    public String toString() {
        return "Message{id='" + id + "', content='" + content + "', priority=" + priority + ", estimatedTime=" + estimatedTime + "ms}";
    }
}

// Class for Topic (Thread-safe queue)
class Topic {
    private final Queue<Message> queue;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();
    private final int capacity; // 0 for unbounded
    private final boolean bounded;
    final boolean retentionPolicy;
    private final boolean fanOut;

    public Topic(int capacity, boolean retentionPolicy, boolean fanOut) {
        this.capacity = capacity;
        this.bounded = capacity > 0;
        this.retentionPolicy = retentionPolicy;
        this.fanOut = fanOut;
        this.queue = new PriorityQueue<>((a, b) -> b.getPriority() - a.getPriority()); // Higher priority first
    }

    public void enqueue(Message message) throws InterruptedException {
        lock.lock();
        try {
            if (bounded) {
                while (queue.size() >= capacity) {
                    notFull.await();
                }
            }
            queue.add(message);
            notEmpty.signal(); // Signal one waiting consumer
        } finally {
            lock.unlock();
        }
    }

    public Message dequeue() throws InterruptedException {
        lock.lock();
        try {
            while (queue.isEmpty()) {
                notEmpty.await();
            }
            Message message = queue.poll();
            if (bounded) {
                notFull.signal();
            }
            return message;
        } finally {
            lock.unlock();
        }
    }

    public void cleanExpired() {
        lock.lock();
        try {
            queue.removeIf(Message::isExpired);
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public List<Message> getMessages() {
        lock.lock();
        try {
            return new ArrayList<>(queue);
        } finally {
            lock.unlock();
        }
    }
}

// Producer Runnable
class Producer implements Runnable {
    private final Topic topic;
    private final int numMessages;
    private final Random random = new Random();

    public Producer(Topic topic, int numMessages) {
        this.topic = topic;
        this.numMessages = numMessages;
    }

    @Override
    public void run() {
        for (int i = 0; i < numMessages; i++) {
            String id = UUID.randomUUID().toString();
            String content = "Message " + i;
            long ttl = random.nextBoolean() ? 5000 : 0; // Random TTL for some messages
            int priority = random.nextInt(10); // Priority 0-9
            long estimatedTime = random.nextInt(1000) + 100; // 100-1100 ms
            Message message = new Message(id, content, ttl, priority, estimatedTime);
            try {
                topic.enqueue(message);
                System.out.println("Produced: " + message);
                Thread.sleep(500); // Simulate delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

// Consumer Runnable
class Consumer implements Runnable {
    private final Topic topic;

    public Consumer(Topic topic) {
        this.topic = topic;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Message message = topic.dequeue();
                if (message.isExpired()) {
                    System.out.println("Discarded expired: " + message);
                    continue;
                }
                System.out.println("Consumed: " + message);

                Thread.sleep(message.getEstimatedTime());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}

// Garbage Collector for retention policy
class GarbageCollector implements Runnable {
    private final List<Topic> topics;
    private final long interval;

    public GarbageCollector(List<Topic> topics, long interval) {
        this.topics = topics;
        this.interval = interval;
    }

    @Override
    public void run() {
        while (true) {
            for (Topic topic : topics) {
                if (topic.retentionPolicy) {
                    topic.cleanExpired();
                }
            }
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

// Dashboard to display status
class Dashboard {
    private final List<Topic> topics;
    private final List<Thread> consumers;

    public Dashboard(List<Topic> topics, List<Thread> consumers) {
        this.topics = topics;
        this.consumers = consumers;
    }

    public void display() {
        System.out.println("Dashboard:");
        for (int i = 0; i < topics.size(); i++) {
            Topic topic = topics.get(i);
            System.out.println("Topic " + i + ": Queue size = " + topic.size());
            List<Message> messages = topic.getMessages();
            long totalEstimated = 0;
            int priorityCount = 0;
            for (Message msg : messages) {
                if (msg.getPriority() > 0) {
                    totalEstimated += msg.getEstimatedTime();
                    priorityCount++;
                }
            }
            double avgEstimated = priorityCount > 0 ? (double) totalEstimated / priorityCount : 0;
            System.out.println("Average estimated time for priority tasks: " + avgEstimated + " ms");
        }
        System.out.println("Active consumers: " + consumers.stream().filter(Thread::isAlive).count());
        System.out.println("--------------------");
    }
}

public class Main {
    public static void main(String[] args) throws InterruptedException {

        boolean retentionPolicy = true; // Enable retention policy
        boolean fanOut = true; // Enable fan-out (multiple consumers per topic)

        int capacity = 5; // Capacity if bounded

        // Create topics
        List<Topic> topics = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            topics.add(new Topic(capacity, retentionPolicy, fanOut));
        }

        // Create producers
        List<Thread> producers = new ArrayList<>();
        for (Topic topic : topics) {
            producers.add(new Thread(new Producer(topic, 20))); // 20 messages per producer
        }

        // Create consumers (fan-out: multiple per topic if enabled)
        List<Thread> consumers = new ArrayList<>();
        int numConsumersPerTopic = fanOut ? 2 : 1;
        for (Topic topic : topics) {
            for (int i = 0; i < numConsumersPerTopic; i++) {
                consumers.add(new Thread(new Consumer(topic)));
            }
        }

        // Garbage collector if retention policy enabled
        Thread gc = null;
        if (retentionPolicy) {
            gc = new Thread(new GarbageCollector(topics, 1000)); // Check every 1 second
            gc.setDaemon(true);
            gc.start();
        }

        // Start threads
        producers.forEach(Thread::start);
        consumers.forEach(Thread::start);

        // Dashboard thread
        Dashboard dashboard = new Dashboard(topics, consumers);
        Thread dashboardThread = new Thread(() -> {
            while (true) {
                dashboard.display();
                try {
                    Thread.sleep(2000); // Update every 2 seconds
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        dashboardThread.setDaemon(true);
        dashboardThread.start();

        // Wait for producers to finish
        for (Thread producer : producers) {
            producer.join();
        }

        // consumers run for a while, then interrupt
        Thread.sleep(10000); // running time
        consumers.forEach(Thread::interrupt);

        if (gc != null) {
            gc.interrupt();
        }
    }
}