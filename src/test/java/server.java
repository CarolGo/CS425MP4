import cs425.mp3.FileOperation;

import java.net.ServerSocket;

public class server {
    public static void main(String... args) throws Exception {
        server();
    }

    static void server() throws Exception {
        ServerSocket serverSocket = new ServerSocket(30231);
        FileOperation.readFileViaSocket("F:\\raw.en.tgz.dup", serverSocket.accept());
        serverSocket.close();
    }
}
