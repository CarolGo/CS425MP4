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
                saveFileViaSocket(socket);
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        Socket s = new Socket("127.0.0.1", Config.TCP_FILE_TRANS_PORT);
        sendFileViaSocket("a", s, "SDFSA");
        s.close();
    }

    private static boolean sendFileViaSocket(String originalFilePath, Socket socket, String sdfsName) {
        try {
            socket.setSoTimeout(120_000); // 120s timeout
            File toSend = new File(originalFilePath);
            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(toSend))) {
                DataOutputStream dOut = new DataOutputStream(socket.getOutputStream());
                dOut.writeUTF(sdfsName);
                dOut.writeLong(toSend.length());
                bufferedReadWrite(in, dOut, Config.NETWORK_BUFFER_SIZE);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        System.err.println("Sent");
        return true;
    }

    /**
     * Receive a file via socket, do nothing with socket
     *
     * @param socket A socket produced by ServerSocket.accept()
     */
    private static boolean saveFileViaSocket(Socket socket) {
        try {
            socket.setSoTimeout(120_000); // 120s timeout
            DataInputStream dIn = new DataInputStream(socket.getInputStream());
            String sdfsName = dIn.readUTF();
            long fileSize = dIn.readLong();
            System.err.println(sdfsName);
            System.err.println(fileSize);
            File dest = new File(Config.STORAGE_PATH, sdfsName);
            try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(dest))) {
                bufferedReadWrite(dIn, bos, Config.NETWORK_BUFFER_SIZE);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        System.err.println("Recv");
        return true;
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
