import cs425.mp3.Config;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class sendRecvTest {

    public static void main(String... args) throws Exception {
        ServerSocket sc = new ServerSocket(Config.TCP_FILE_TRANS_PORT);
        new Thread(() -> {
            try {
                Socket socket = sc.accept();
                saveFileViaSocket("tttt", socket);
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        Socket s = new Socket("127.0.0.1", Config.TCP_FILE_TRANS_PORT);
        sendFileViaSocket("a", s);
        s.close();
    }

    private static void sendFileViaSocket(String originalFilePath, Socket socket) throws IOException {
        socket.setSoTimeout(120_000); // 120s timeout
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(originalFilePath))) {
            bufferedReadWrite(in, socket.getOutputStream(), Config.NETWORK_BUFFER_SIZE);
        }
        System.err.println("Sent");
    }

    /**
     * Receive a file via socket, do nothing with socket
     *
     * @param newFileName File name (UUID) of the file
     * @param socket      A socket produced by ServerSocket.accept()
     */
    private static void saveFileViaSocket(String newFileName, Socket socket) throws IOException {
        File dest = new File(Config.STORAGE_PATH, newFileName);
        socket.setSoTimeout(120_000); // 120s timeout
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(dest))) {
            bufferedReadWrite(socket.getInputStream(), bos, Config.NETWORK_BUFFER_SIZE);
        }
        System.err.println("Recv");
    }

    private static void bufferedReadWrite(InputStream in, OutputStream out, int bSize) throws IOException {
        byte[] buf = new byte[bSize];
        int len;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        out.flush();
    }
}
