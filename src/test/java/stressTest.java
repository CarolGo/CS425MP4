import cs425.mp3.Config;
import cs425.mp3.FileOperation;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class stressTest {

    static Random random = new Random();

    @Test
    void stress1() throws Exception {
        FileOperation f = new FileOperation(null);
        ExecutorService e = Executors.newFixedThreadPool(5000);
        for (int i = 0; i < 5000; i++) {
            e.submit(() -> {
                try {
                    Thread.sleep(random.nextInt(1000));
                    Socket s = new Socket("127.0.0.1", Config.TCP_PORT);
                    s.getOutputStream().write("Hello you!".getBytes());
                    s.getOutputStream().close();
                    System.err.println("Data wrote");
                    s.close();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            });
        }
        e.shutdown();
        e.awaitTermination(100, TimeUnit.SECONDS);
        System.err.println("Finished");
        f.stopServer();
    }

}
