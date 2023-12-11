import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

public class Controller extends Thread {
    private StreamGenerator streamGenerator;
    private HybridJoinThread hybridJoinThread;
    private BlockingQueue<List<tuple>> streamQueue;
    private volatile boolean running = true;

    public Controller(StreamGenerator streamGenerator, HybridJoinThread hybridJoinThread,
                      BlockingQueue<List<tuple>> streamQueue) {
        this.streamGenerator = streamGenerator;
        this.hybridJoinThread = hybridJoinThread;
        this.streamQueue = streamQueue;
    }

    @Override
    public void run() {
        while (running && !Thread.currentThread().isInterrupted()) {
            adjustStreamingSpeed();
            // Thread.sleep(500); // Adjust the sleep time as per your requirement the number of seconds to wait before adjusting the speed is 1 second
        }
    }

    public void stopController() {
        running = false;
    }

    private void adjustStreamingSpeed() {
        
        int queueSize = streamQueue.size();
        if (queueSize > 15000) {
            streamGenerator.decreasePace();
            
        } else if (queueSize < 500) {
            streamGenerator.increasePace();

        }
    }


}
