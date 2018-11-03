package cs425.mp3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
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
    private final ExecutorService processFileRecvThread;
    private final ExecutorService singleMainRecvThread;
    private final String serverHostname;
    private final ServerSocket serverSocket;
    private final ServerSocket fileReceiveSocket;
    private boolean isFileServerRunning;

    // File meta data
    private ConcurrentHashMap<String, FileObject> localFileMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, FileObject> sdfsFileMap = new ConcurrentHashMap<>();

    // Cached for writing file
    private FileObject fileObjectCache;

    public FileOperation(Node n) throws IOException {
        this.node = n;
        this.serverHostname = InetAddress.getLocalHost().getCanonicalHostName();
        this.serverSocket = new ServerSocket(Config.TCP_PORT);
        this.fileReceiveSocket = new ServerSocket(Config.TCP_FILE_TRANS_PORT);
        this.processThread = Executors.newFixedThreadPool(Config.NUM_CORES * 2);
        this.processFileRecvThread = Executors.newFixedThreadPool(Config.NUM_CORES * 2);
        this.singleMainThread = Executors.newSingleThreadExecutor();
        this.singleMainRecvThread = Executors.newSingleThreadExecutor();
        this.isFileServerRunning = true;
        this.singleMainThread.submit(() -> {
            Thread.currentThread().setName("FS-main");
            logger.info("File server started listening on <{}>...", this.serverHostname);
            while (this.isFileServerRunning) {
                try {
                    if (this.serverSocket.isClosed()) continue;
                    this.processThread.submit(this.mainFileServer(this.serverSocket.accept()));
                } catch (IOException e) {
                    logger.error("Server socket failed", e);
                }
            }
        });
        this.singleMainRecvThread.submit(() -> {
            Thread.currentThread().setName("FS-recv-main");
            logger.info("File receive server started listening on <{}>...", this.serverHostname);
            while (this.isFileServerRunning) {
                try {
                    if (this.fileReceiveSocket.isClosed()) continue;
                    this.processFileRecvThread.submit(this.mainFileRecvServer(this.fileReceiveSocket.accept()));
                } catch (IOException e) {
                    logger.error("Server socket failed", e);
                }
            }
        });
    }

    public void stopServer() {
        this.isFileServerRunning = false;
        this.processThread.shutdown();
        this.processFileRecvThread.shutdown();
        this.singleMainThread.shutdown();
        this.singleMainRecvThread.shutdown();
        try {
            this.serverSocket.close();
            this.fileReceiveSocket.close();
            logger.info("File server stopped listening...");
        } catch (IOException e) {
            logger.error("Server socket failed to close", e);
        }
    }

    public void put(String localFileName, String sdfsFileName) {
        String leader = this.node.getLeader();
        if (leader.isEmpty()) {
            logger.error("Leader empty, can not put");
            return;
        }
        FileCommandResult queryResault = query(sdfsFileName);
        if (queryResault != null && queryResault.getVersion() >= 0) {
            int newVersion = queryResault.getVersion() + 1;
            FileCommand cmd = new FileCommand("put", leader, sdfsFileName, newVersion);
            try {
                Socket s = connectToServer(leader, Config.TCP_PORT);
                FileCommandResult res = sendFileCommandViaSocket(cmd, s);
                if (res.isHasError()) {
                    logger.info("master put error");
                } else {
                    localCopyFileToStorage(localFileName, sdfsFileName);
                    logger.info("local replication finished");
                    for (String host : res.getReplicaNodes()) {
                        Socket replicaSocket = connectToServer(host, Config.TCP_FILE_TRANS_PORT);
                        sendFileViaSocket(localFileName, replicaSocket);
                        logger.info("put replica of {} at {}", sdfsFileName, host);
                        replicaSocket.close();
                    }
                    logger.info("put finished");
                }
            } catch (IOException e) {
                logger.debug("Failed to establish connection", e);

            }
        } else {
            logger.info("Failure on query in put operation");
        }

    }

    public void get(String sdfsFileName, String localFileName) {

    }

    public void delete(String sdfsFileName) {

    }

    public void listFileLocations(String sdfsFileName) {

    }

    public void listFileLocal() {
        for (String file : this.localFileMap.keySet()) {
            FileObject fo = this.localFileMap.get(file);
            int version = fo.getVersion();
            logger.info("local file includes: {}+{}", version, fo.getPath());
        }
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
        logger.debug("Copy file from <{}> to <{}>", originalPath, dest.getAbsolutePath());
        File src = new File(originalPath);
        try (BufferedInputStream is = new BufferedInputStream(new FileInputStream(src))) {
            try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(dest))) {
                bufferedReadWrite(is, os, 8192);
            }
        }
    }

    /**
     * connection to host
     */
    private Socket connectToServer(String host, int port) throws IOException {
        Socket s = new Socket();
        // Potential higher performance with SO_KA
        s.setKeepAlive(true);
        s.connect(new InetSocketAddress(host, port), Config.CONNECT_TIMEOUT_SECOND * 1000);
        s.setSoTimeout(Config.RW_TIMEOUT_SECOND * 1000);
        logger.info("Connected to server {}", host);
        return s;
    }

    /**
     * Just send the file command via socket, do nothing with socket
     *
     * @param fc     File path for the file you want to send
     * @param socket A socket connects to remote host
     */
    private FileCommandResult sendFileCommandViaSocket(FileCommand fc, Socket socket) throws IOException {
        FileCommandResult res = null;
        try {
            // Output goes first or the input will block forever
            ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));

            out.writeObject(fc);
            out.flush();
            logger.info("file command sent at '{}'.", fc.getTimestamp());

            // Some blocking here for sure
            res = FileCommandResult.parseFromStream(in);
            // Communication finished, notice the sequence
            in.close();
            out.close();
            socket.close();

        } catch (ClassNotFoundException e) {
            logger.error("Client received malformed data!");
        }
        return res;

    }

    private void sendFileCommandResultViaSocket(ObjectOutputStream out, FileCommandResult fcs) {
        try {
            out.writeObject(fcs);
            out.flush();
            logger.info("file command result sent at '{}'.", fcs.getTimestamp());
        } catch (IOException e) {
            logger.debug("Failed to establish connection", e);
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
    private void saveFileViaSocket(String newFileName, Socket socket) throws IOException {
        File dest = new File(Config.STORAGE_PATH, newFileName);
        socket.setSoTimeout(120_000); // 120s timeout
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(dest))) {
            bufferedReadWrite(socket.getInputStream(), bos, Config.NETWORK_BUFFER_SIZE);
            logger.info("Finished receiving file <{}>", newFileName);
        }
    }

    /**
     * @param sdfsFileName SDFS file name
     * @return verision number 0 if not exist, -1 if failure, otherwise latest version number in master node
     */
    private FileCommandResult query(String sdfsFileName) {
        String leader = this.node.getLeader();
        if (!leader.isEmpty()) {
            FileCommand cmd = new FileCommand("query", leader, sdfsFileName, 0);
            try {
                Socket s = connectToServer(leader, Config.TCP_PORT);
                FileCommandResult res = sendFileCommandViaSocket(cmd, s);
                if (!res.isHasError()) {
                    return res;
                }
            } catch (IOException e) {
                logger.debug("Failed to establish connection", e);
                return null;
            }
        }
        logger.info("leader not elected");
        return null;
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
     * Receive a file via socket
     */
    private Runnable mainFileRecvServer(Socket socket) {
        return () -> {
            Thread.currentThread().setName("FS-recv-process");
            try {
                //TODO: get from meta data info
                saveFileViaSocket("test", socket);
                socket.close();
                this.localFileMap.put("test", new FileObject("test", 1));
            } catch (IOException e) {
                logger.error("Receive file failed", e);
            }
        };
    }

    /**
     * Define operations for the file server
     */
    private Runnable mainFileServer(Socket clientSocket) {
        return () -> {
            Thread.currentThread().setName("FS-process");
            logger.info("Connection from client <{}>", clientSocket.getRemoteSocketAddress());
            // Logic start
            FileCommand cmd = null;
            try {
                // Output goes first or the input will block forever
                ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(clientSocket.getInputStream()));
                cmd = FileCommand.parseFromStream(in);
                if (cmd == null) {
                    logger.error("FileCommand is null");
                    return;
                }
                logger.info("file command received from <{}>, type <{}>", clientSocket.getInetAddress().getHostName(), cmd.getType());
                switch (cmd.getType()) {
                    case "query":
                        queryHandler(out, cmd.getFileName());
                        break;
                    case "put":
                        putHandler(out, cmd, clientSocket.getInetAddress().getHostName());
                        break;
                    default:
                        logger.error("Command type error");
                        break;
                }
            } catch (ClassNotFoundException e) {
                logger.error("Client received malformed data!");
            } catch (IOException e) {
                logger.error("Server socket failed", e);
            }
            // Logic ends
            try {
                clientSocket.close();
                logger.info("Closed connection from client: <{}>", clientSocket.getRemoteSocketAddress());
            } catch (IOException e) {
                logger.error("Close socket failed", e);
            }
        };
    }

    private void putHandler(ObjectOutputStream out, FileCommand cmd, String clientHostname) {
        int version = cmd.getVersionNum();
        String fileName = cmd.getFileName();
        //store new file
        if (version == 1) {
            ArrayList<String> hosts = new ArrayList<>(Arrays.asList(this.node.getNodesArray()));
            Collections.shuffle(hosts);
            hosts.remove(clientHostname);
            if (hosts.size() >= 3) {
                Set<String> replicaNodes = new HashSet<>();
                replicaNodes.add(hosts.get(1));
                replicaNodes.add(hosts.get(2));
                replicaNodes.add(hosts.get(0));
                logger.debug("Selected replica nodes: {}", String.join(", ", replicaNodes));
                //set sdfs meta information
                FileObject newFile = new FileObject(fileName, version);
                newFile.setReplicaLocations(replicaNodes);
                this.sdfsFileMap.put(fileName, newFile);
                //send back fcs
                FileCommandResult fcs = new FileCommandResult(replicaNodes, version);
                sendFileCommandResultViaSocket(out, fcs);
            } else {
                logger.info("put handler fail to get node list");
                FileCommandResult fcs = new FileCommandResult(null, 0);
                fcs.setHasError(false);
                sendFileCommandResultViaSocket(out, fcs);
            }
        }
        //update new version
        else {
            FileObject oldFile = this.sdfsFileMap.get(fileName);
            oldFile.setVersion(version);
            Set<String> replicaNodes = oldFile.getReplicaLocations();
            FileCommandResult fcs = new FileCommandResult(replicaNodes, version);
            sendFileCommandResultViaSocket(out, fcs);
        }
    }

    private void queryHandler(ObjectOutputStream out, String fileName) {
        int version = 0;
        Set<String> replicaLocations = null;
        for (String file : this.sdfsFileMap.keySet()) {
            if (file.equals(fileName) && this.sdfsFileMap.get(fileName).getVersion() > version) {
                version = this.sdfsFileMap.get(fileName).getVersion();
                replicaLocations = this.sdfsFileMap.get(fileName).getReplicaLocations();
            }
        }
        FileCommandResult fcs = new FileCommandResult(replicaLocations, version);
        sendFileCommandResultViaSocket(out, fcs);
    }

}
