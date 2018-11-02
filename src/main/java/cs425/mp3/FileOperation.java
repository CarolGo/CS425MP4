package cs425.mp3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * All operations regarding distributed FS
 */
public final class FileOperation {
    private final Logger logger = LoggerFactory.getLogger(FileOperation.class);

    // Runtime variable
    private final Node node;
    private final ExecutorService processThread;
    private final ExecutorService singleMainThread;
    private final String serverHostname;
    private final ServerSocket serverSocket;
    private boolean isFileServerRunning;

    // File meta data
    private ConcurrentHashMap<UUID, FileObject> localFileMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<UUID, FileObject> sdfsFileMap = new ConcurrentHashMap<>();

    public FileOperation(Node n) throws IOException {
        this.node = n;
        this.serverHostname = InetAddress.getLocalHost().getCanonicalHostName();
        this.serverSocket = new ServerSocket(Config.TCP_PORT);
        this.processThread = Executors.newFixedThreadPool(Config.NUM_CORES * 2);
        this.singleMainThread = Executors.newSingleThreadExecutor();
        this.isFileServerRunning = true;
        this.singleMainThread.submit(() -> {
            Thread.currentThread().setName("FS-main");
            logger.info("File server started listening on <{}>...", this.serverHostname);
            while (this.isFileServerRunning) {
                try {
                    this.processThread.submit(this.mainFileServer(this.serverSocket.accept()));
                } catch (IOException e) {
                    logger.error("Server socket failed", e);
                }
            }
            logger.info("File server stopped listening...");
        });
    }

    public void stopServer() {
        this.isFileServerRunning = false;
        this.processThread.shutdown();
        this.singleMainThread.shutdown();
    }

    public void put(String localFileName, String sdfsFileName) {

    }

    public void get(String sdfsFileName, String localFileName) {

    }

    public void delete(String sdfsFileName) {

    }

    public void listFileLocations(String sdfsFileName) {

    }

    public void listFileLocal() {

    }

    public void getVersions(String sdfsFileName, String numVersions, String localFileName) {
        int numOfLatestVersions;
        try {
            numOfLatestVersions = Integer.valueOf(numVersions);
        } catch (NumberFormatException n) {
            logger.error("Version number input error");
            numOfLatestVersions = 1;
        }
    }

    /**
     * Copy file to a path
     */
    private void localCopyFileToStorage(String originalPath, String newFileName) throws IOException {
        File dest = new File(Config.STORAGE_PATH, newFileName);
        File src = new File(originalPath);
        try (BufferedInputStream is = new BufferedInputStream(new FileInputStream(src))) {
            try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(dest))) {
                bufferedReadWrite(is, os, 8192);
            }
        }
    }

    /**
     * Just send the file via socket, do nothing with socket
     *
     * @param originalFilePath File path for the file you want to send
     * @param socket           A socket connects to remote host
     */
    private void sendFileViaSocket(String originalFilePath, Socket socket) throws IOException {
        socket.setSoTimeout(120_000); // 120s timeout
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(originalFilePath))) {
            bufferedReadWrite(in, socket.getOutputStream(), Config.NETWORK_BUFFER_SIZE);
            logger.info("Finished sending file");
        }
    }

    /**
     * Receive a file via socket, do nothing with socket
     *
     * @param newFileName File name (UUID) of the file
     * @param socket      A socket produced by ServerSocket.accept()
     */
    private void readFileViaSocket(String newFileName, Socket socket) throws IOException {
        File dest = new File(Config.STORAGE_PATH, newFileName);
        socket.setSoTimeout(120_000); // 120s timeout
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(dest))) {
            bufferedReadWrite(socket.getInputStream(), bos, Config.NETWORK_BUFFER_SIZE);
            logger.info("Finished receiving file");
        }
    }

    private void bufferedReadWrite(InputStream in, OutputStream out, int bSize) throws IOException {
        byte[] buf = new byte[bSize];
        int len;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        out.flush();
    }

    /**
     * Define operations for the file server
     */
    private Runnable mainFileServer(Socket clientSocket) {
        return () -> {
            Thread.currentThread().setName("FS-process");
            logger.info("Connection from client <{}>", clientSocket.getRemoteSocketAddress());
            // Logic start

            // Logic ends
            try {
                clientSocket.close();
                logger.info("Closed connection from client: <{}>", clientSocket.getRemoteSocketAddress());
            } catch (IOException e) {
                logger.error("Close socket failed", e);
            }
        };
    }

}
