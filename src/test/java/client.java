import cs425.mp3.FileOperation;

import java.net.InetSocketAddress;
import java.net.Socket;

public class client {
    public static void main(String... args) throws Exception {
        client();
    }

    static void client() throws Exception {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", 30231));
        FileOperation.sendFileViaSocket("F:\\raw.en.tgz", socket);
        socket.close();
    }
}
