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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * All operations regarding distributed FS
 */
public final class FileOperation {
    private final Logger logger = LoggerFactory.getLogger(FileOperation.class);

    private final AtomicBoolean hasReceivedSuccess = new AtomicBoolean(false);

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
        initialMainThreadsJob();
    }

    private void initialMainThreadsJob() {
        this.singleMainThread.submit(() -> {
            Thread.currentThread().setName("FS-main");
            logger.info("File server started listening on <{}>...", this.serverHostname);
            while (this.isFileServerRunning) {
                try {
                    if (this.serverSocket.isClosed()) continue;
                    this.processThread.submit(this.mainFileServer(this.serverSocket.accept()));
                } catch (IOException e) {
                    logger.error("Main socket failed", e);
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
                    logger.error("File socket failed", e);
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
            FileObject fo = new FileObject(newVersion);
            FileCommand cmd = new FileCommand("put", leader, sdfsFileName, newVersion);
            FileCommandResult res = null;
            try {
                Socket s = connectToServer(leader, Config.TCP_PORT);
                res = sendFileCommandViaSocket(cmd, s);
                if (res.isHasError()) {
                    logger.info("master put error");
                    return;
                }
                localCopyFileToStorage(new File(localFileName), fo.getUUID(), true);
                this.localFileMap.put(sdfsFileName, fo);
                logger.info("local replication finished");
            } catch (IOException e) {
                logger.debug("Failed to put", e);
            }
            if (res == null) {
                logger.error("FileCommandResult is null");
                return;
            }
            for (String host : res.getReplicaNodes()) {
                //TODO: Multi-thread send?
                Socket replicaSocket;
                try {
                    replicaSocket = connectToServer(host, Config.TCP_FILE_TRANS_PORT);
                    File toSend = new File(Config.STORAGE_PATH, fo.getUUID());
                    sendFileViaSocket(toSend, replicaSocket, sdfsFileName, newVersion, "put");
                } catch (IOException e) {
                    logger.info("Failed put replica of {} at {}", sdfsFileName, host);
                    logger.error("Reason for failure: ", e);
                    continue;
                }
                try {
                    replicaSocket.close();
                    logger.info("Success put replica of {} at {}", sdfsFileName, host);
                } catch (IOException e) {
                    logger.error("Replica socket close failed", e);
                }
            }
        } else {
            logger.info("Failure on query in put operation");
        }
    }

    public void get(String sdfsFileName, String localFileName) {
        //try the local file first
        if (this.localFileMap.get(sdfsFileName) != null) {
            logger.info("File <{}> found in local machine", sdfsFileName);
            try {
                File localPath = new File(Config.STORAGE_PATH, this.localFileMap.get(sdfsFileName).getUUID());
                localCopyFileToStorage(localPath, localFileName, false);
                logger.info("Local file <{}> got!!!", sdfsFileName);
            } catch (IOException e) {
                logger.debug("fail for local get");
            }
            return;
        }
        // Not in local, get from Master
        String leader = this.node.getLeader();
        if (leader.isEmpty()) {
            logger.error("Leader empty, can not get");
            return;
        }
        FileCommandResult queryResault = query(sdfsFileName);
        if (queryResault != null && queryResault.getVersion() >= 0) {
            for (String host : queryResault.getReplicaNodes()) {
                try {
                    Socket getSocket = connectToServer(host, Config.TCP_PORT);
                    FileCommandResult getResult = sendFileCommandViaSocket(new FileCommand("get", host, sdfsFileName, 0), getSocket);
                    if (getResult.isHasError()) {
                        logger.info("get error with <{}>", host);
                    } else {
                        long totalSleepingTime = 0;
                        long timeout = 10_000;
                        //Todo:receive the file and save
                        while (!this.hasReceivedSuccess.get() || totalSleepingTime < Config.FILE_RECV_TIMEOUT_MILLSECOND) {
                            try {
                                Thread.sleep(timeout);
                                totalSleepingTime += timeout;
                            } catch (InterruptedException e) {
                                logger.error("WTF", e);
                            }
                        }
                        if (this.hasReceivedSuccess.get()) {
                            logger.info("File <{}> got from <{}>!!!", sdfsFileName, host);
                            this.hasReceivedSuccess.set(false);
                            break;
                        } else {
                            logger.info("File <{}> get failed", sdfsFileName);
                            continue;
                        }
                    }
                } catch (IOException e) {
                    logger.debug("Failed to establish connection with <{}>", host, e);
                }

            }
        } else {
            logger.info("Failure on query in put operation");
        }
    }

    public void delete(String sdfsFileName) {
        String leader = this.node.getLeader();
        if (leader.isEmpty()) {
            logger.error("Leader empty, can not delete");
            return;
        }
        //leader delete
        if (this.node.getHostName().equals(leader)) {
            FileObject file = this.sdfsFileMap.get(sdfsFileName);
            if (file == null) {
                logger.info("delete finished");
            } else {
                this.sdfsFileMap.remove(sdfsFileName);
                for (String host : file.getReplicaLocations()) {
                    try {
                        Socket deleteSocket = connectToServer(host, Config.TCP_PORT);
                        FileCommandResult res = sendFileCommandViaSocket(new FileCommand("delete", host, sdfsFileName, 0), deleteSocket);
                        if (res.isHasError()) {
                            logger.debug("Fail to ask node <{}> to delete", host);
                        }
                    } catch (IOException e) {
                        logger.debug("Failed to establish connection with <{}>", host, e);
                    }

                }
                logger.info("delete finished");
            }
        } else {   //member delete
            try {
                Socket deleteSocket = connectToServer(leader, Config.TCP_PORT);
                FileCommandResult result = sendFileCommandViaSocket(new FileCommand("delete", leader, sdfsFileName, 0), deleteSocket);
                if (result.isHasError()) {
                    logger.debug("Master delete fail");
                } else {
                    logger.info("delete finished!!!");
                }
            } catch (IOException e) {
                logger.debug("Failed to establish connection with <{}>", leader, e);
            }
        }
    }

    public void listFileLocations(String sdfsFileName) {
        if (!this.node.getLeader().equals(this.node.getHostName())) {
            askBackup();
        }
        FileObject file = this.sdfsFileMap.get(sdfsFileName);
        if (file == null) {
            logger.info("<{}> not stored", sdfsFileName);
        } else {
            logger.info(String.join(",", file.getReplicaLocations()));
        }
    }

    private void askBackup() {
        String leader = this.node.getLeader();
        if (leader.isEmpty()) {
            logger.error("Leader empty, can not list file in SDFS");
        } else {
            try {
                Socket s = connectToServer(leader, Config.TCP_PORT);
                FileCommandResult result = sendFileCommandViaSocket(new FileCommand("backup", leader, "", 0), s);
                if (result.isHasError()) {
                    logger.debug("error when requesting backup");
                } else {
                    this.sdfsFileMap = result.getBackup();
                    logger.info("backup from <{}> finished", leader);
                }
            } catch (IOException e) {
                logger.debug("Fail to establish coonection with <{}>", leader, e);
            }
        }
    }

    public void listFileLocal() {
        logger.info("local file includes");
        for (String file : this.localFileMap.keySet()) {
            FileObject fo = this.localFileMap.get(file);
            int version = fo.getVersion();
            logger.info("name: <{}>     verison: <{}>", file, version);
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
    private void localCopyFileToStorage(File originalPath, String newFileName, Boolean isPut) throws IOException {
        File dest;
        if (isPut) {
            dest = new File(Config.STORAGE_PATH, newFileName);
        } else {
            dest = new File(Config.GET_PATH, newFileName);
        }
        logger.debug("Copy file from <{}> to <{}>", originalPath.getAbsolutePath(), dest.getAbsolutePath());
        try (BufferedInputStream is = new BufferedInputStream(new FileInputStream(originalPath))) {
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
     * Just send the file via socket, no closing socket
     *
     * @param toSendFile File path for the file you want to send
     * @param socket     A socket connects to remote host
     * @param sdfsName   SDFS name to send to client
     * @param intention  Send for get or put
     */
    private void sendFileViaSocket(File toSendFile, Socket socket, String sdfsName, int version, String intention) throws IOException {
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(toSendFile))) {
            logger.debug("[{}] Sending <{}>({}b) version <{}> to <{}>", intention, toSendFile.getAbsolutePath(), toSendFile.length(), version, socket.getRemoteSocketAddress());
            DataOutputStream dOut = new DataOutputStream(socket.getOutputStream());
            dOut.writeUTF(intention.toLowerCase());
            dOut.writeUTF(sdfsName);
            dOut.writeInt(version);
            dOut.writeLong(toSendFile.length());
            bufferedReadWrite(in, dOut, Config.NETWORK_BUFFER_SIZE);
        }
    }

    /**
     * Receive a file via InputStream, do nothing with stream
     */
    private void saveFileViaSocketInput(InputStream in, File dest) throws IOException {
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(dest))) {
            bufferedReadWrite(in, bos, Config.NETWORK_BUFFER_SIZE);
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
            String intention = "";
            try {
                DataInputStream dIn = new DataInputStream(socket.getInputStream());
                intention = dIn.readUTF();
                String sdfsName = dIn.readUTF();
                int fileVersion = dIn.readInt();
                long fileSize = dIn.readLong();
                logger.debug("[{}] Receiving file <{}>({}b) version <{}> from <{}>", intention, sdfsName, fileSize, fileVersion, socket.getRemoteSocketAddress());
                File dest;
                switch (intention) {
                    case "put":
                        FileObject fo = new FileObject(fileVersion);
                        dest = new File(Config.STORAGE_PATH, fo.getUUID());
                        saveFileViaSocketInput(dIn, dest);
                        // Only update list when save file is successful
                        this.localFileMap.put(sdfsName, fo);
                        break;
                    case "get":
                        dest = new File(Config.GET_PATH, sdfsName);
                        saveFileViaSocketInput(dIn, dest);
                        this.hasReceivedSuccess.set(true);
                        break;
                    default:
                        throw new IOException("Unknown intention");
                }
                logger.debug("[{}] Got file <{}>({}b) version <{}> from <{}>", intention, sdfsName, fileSize, fileVersion, socket.getRemoteSocketAddress());
            } catch (IOException e) {
                logger.error("Receive file failed", e);
                if (intention.equals("get")) this.hasReceivedSuccess.set(false);
            }
            try {
                socket.close();
            } catch (IOException e) {
                logger.error("Closing socket failed");
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
            try {
                // Output goes first or the input will block forever
                ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(clientSocket.getInputStream()));
                FileCommand cmd = FileCommand.parseFromStream(in);
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
                    case "get":
                        getHandler(out, cmd, clientSocket.getInetAddress().getHostName());
                        break;
                    case "delete":
                        if (this.node.getLeader().equals(this.node.getHostName())) {
                            masterDeleteHandler(out, cmd, clientSocket.getInetAddress().getHostName());
                        } else {
                            memberDeleteHandler(out, cmd, clientSocket.getInetAddress().getHostName());
                        }
                        break;
                    case "backup":
                        backupHandler(out, clientSocket.getInetAddress().getHostName());
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

    private void backupHandler(ObjectOutputStream out, String requesetHost) {
        if (this.node.getLeader().equals(this.node.getHostName())) {
            FileCommandResult result = new FileCommandResult(null, 0);
            result.setBackup(this.sdfsFileMap);
            sendFileCommandResultViaSocket(out, result);
            logger.info("backup send to <{}>", requesetHost);
        } else {
            logger.debug("Wrong backup request received by <{}>", this.node.getHostName());
        }
    }

    private void masterDeleteHandler(ObjectOutputStream out, FileCommand cmd, String requestHost) {
        String fileName = cmd.getFileName();
        FileObject deleteTarget = this.sdfsFileMap.get(fileName);
        FileCommandResult result = new FileCommandResult(null, 0);
        ;
        //check if sdfs has this file
        if (deleteTarget == null) {
            sendFileCommandResultViaSocket(out, result);
        } else {
            Set<String> replicaNodes = deleteTarget.getReplicaLocations();
            this.sdfsFileMap.remove(fileName);
            //ask all members to delete the file
            for (String host : replicaNodes) {
                if (host.equals(this.node.getHostName())) {
                    this.localFileMap.remove(fileName);
                } else {
                    try {
                        Socket s = connectToServer(host, Config.TCP_PORT);
                        FileCommandResult memberResult = sendFileCommandViaSocket(new FileCommand("delete", host, fileName, 0), s);
                        if (memberResult.isHasError()) {
                            logger.debug("Fail to ask node <{}> to delete", host);
                            result.setHasError(true);
                        }

                    } catch (IOException e) {
                        logger.debug("Fail to establish connection with <{}>", host, e);
                        result.setHasError(true);
                    }
                }
            }
            logger.info("master delete done");
            sendFileCommandResultViaSocket(out, result);
        }

    }

    private void memberDeleteHandler(ObjectOutputStream out, FileCommand cmd, String requestHost) {
        String fileName = cmd.getFileName();
        if (this.localFileMap.get(fileName) != null) {
            this.localFileMap.remove(fileName);
            sendFileCommandResultViaSocket(out, new FileCommandResult(null, 0));
            logger.info("delete <{}> requested by master <{}>", fileName, requestHost);
        } else {
            sendFileCommandResultViaSocket(out, new FileCommandResult(null, 0));
        }
    }


    private void getHandler(ObjectOutputStream out, FileCommand cmd, String requestHost) {
        String sdfsFileName = cmd.getFileName();
        FileCommandResult result = new FileCommandResult(null, 0);
        FileObject file = this.localFileMap.get(sdfsFileName);
        if (file == null) {
            result.setHasError(true);
            logger.info("cannot find file <{}> at <{}>", sdfsFileName, this.node.getHostName());
            sendFileCommandResultViaSocket(out, result);
            return;
        }
        result.setReplicaNodes(file.getReplicaLocations());
        result.setVersion(file.getVersion());
        sendFileCommandResultViaSocket(out, result);
        try {
            Socket transSocket = connectToServer(requestHost, Config.TCP_FILE_TRANS_PORT);
            File toSend = new File(Config.STORAGE_PATH, file.getUUID());
            sendFileViaSocket(toSend, transSocket, sdfsFileName, result.getVersion(), "get");
            transSocket.close();
            logger.info("Requested file <{}> version <{}> sent back", sdfsFileName, result.getVersion());
        } catch (IOException e) {
            logger.debug("Fail to send file", e);
        }
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
                logger.warn("Selected replica nodes: {}", String.join(", ", replicaNodes));
                //set sdfs meta information
                FileObject newFile = new FileObject(version);
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
