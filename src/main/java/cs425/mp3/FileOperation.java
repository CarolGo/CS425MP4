package cs425.mp3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class FileOperation {
    private static final Logger logger = LoggerFactory.getLogger(FileOperation.class);
    private static final int bufSize = Config.FILE_BUFFER_SIZE;

    private ConcurrentHashMap<String, Set<String>> localFileMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Set<String>> sdfsFileMap = new ConcurrentHashMap<>();

    public FileOperation() {

    }

    /**
     * Just send the file via socket, do nothing with socket
     */
    public static void sendFileViaSocket(String filePath, Socket socket) throws IOException {
        socket.setSoTimeout(120_000); // 120s timeout
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(filePath));
        BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());

        byte[] buf = new byte[bufSize];
        int len;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }

        logger.info("Finished sending file");
        in.close();
        out.flush();
    }

    public static void readFileViaSocket(String targetPath, Socket socket) throws IOException {
        socket.setSoTimeout(120_000); // 120s timeout
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(targetPath));
        InputStream in = socket.getInputStream();

        byte[] buf = new byte[bufSize];
        int len;
        while ((len = in.read(buf)) > 0) {
            bos.write(buf, 0, len);
        }

        logger.info("Finished receiving file");
        bos.close();
    }

}
