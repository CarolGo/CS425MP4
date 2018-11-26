package cs425.mp3;

import cs425.Config;
import cs425.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Instant;

/**
 * All operations regarding distributed FS
 */
public final class FileOperation {
    private final Logger logger = LoggerFactory.getLogger(FileOperation.class);

    private final AtomicBoolean hasReceivedSuccess = new AtomicBoolean(false);

    // Runtime variable
    public final Node node;
    private final ExecutorService dataBackupThread;
    private final ScheduledExecutorService metaBackupThread;
    private final ExecutorService processThread;
    private final ExecutorService singleMainThread;
    private final ExecutorService processFileRecvThread;
    private final ExecutorService singleMainRecvThread;
    private final String serverHostname;
    private final ServerSocket serverSocket;
    private final ServerSocket fileReceiveSocket;
    private boolean isFileServerRunning;
    private LocalDateTime lastBackupTime = LocalDateTime.now();

    // File meta data
    private ConcurrentHashMap<String, List<FileObject>> localFileMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<FileObject>> sdfsFileMap = new ConcurrentHashMap<>();

    // Failure cached queue
    private ConcurrentHashMap<String, String> leaderFailureHandledSet = new ConcurrentHashMap<>();

    public FileOperation(Node n) throws IOException {
        this.node = n;
        this.serverHostname = InetAddress.getLocalHost().getCanonicalHostName();
        this.serverSocket = new ServerSocket(Config.TCP_PORT);
        this.fileReceiveSocket = new ServerSocket(Config.TCP_FILE_TRANS_PORT);
        this.processThread = Executors.newFixedThreadPool(Config.NUM_CORES * 2);
        this.processFileRecvThread = Executors.newFixedThreadPool(Config.NUM_CORES * 2);
        this.metaBackupThread = Executors.newScheduledThreadPool(1);
        this.dataBackupThread = Executors.newSingleThreadExecutor();
        this.singleMainThread = Executors.newSingleThreadExecutor();
        this.singleMainRecvThread = Executors.newSingleThreadExecutor();
        this.isFileServerRunning = true;
        initialMainThreadsJob();
    }

    private void initialMainThreadsJob() {
        this.dataBackupThread.submit(() -> {
            while (true) {
                if (this.node.crashedNode.size() == 0 && this.leaderFailureHandledSet.isEmpty()) {
                    Util.noExceptionSleep(500);
                    continue;
                }
                //sleep 2s waiting for possible leader change
                Util.noExceptionSleep(2000);
                while (!this.node.isLeaderCrashed.get()) {
                    HashMap<String, String> copy = new HashMap<>(this.node.crashedNode);
                    this.node.crashedNode.clear();
                    if (this.node.getHostName().equals(this.node.getLeader())) {
                        logger.info("Leader <{}> dealt with crash info", this.node.getLeader());
                        copy.forEach((failedNodeHostname, timestamp) -> {
                            if (this.leaderFailureHandledSet.containsKey(failedNodeHostname)) {
                                // Already handled before
                                logger.info("Failure already handled: <{}>", failedNodeHostname);
                                return;
                            }
                            this.leaderFailureHandledSet.put(failedNodeHostname, "");
                        });
                    } else {
                        logger.warn("Member dealt with crash info");
                        copy.forEach((host, timestamp) -> {
                            FileCommand f = new FileCommand("crash", this.node.getLeader(), host, -1);
                            try {
                                Socket s = Util.connectToServer(this.node.getLeader(), Config.TCP_PORT);
                                FileCommandResult result = sendFileCommandViaSocket(f, s);
                                logger.info("2");
                                if (result.isHasError()) {
                                    logger.error("Fail send crash msg");
                                    return;
                                }
                            } catch (IOException e) {
                                logger.error("Fail send crash msg to leader", e);
                            }
                        });
                    }
                    if (this.node.getLeader().equals(this.node.getHostName())) {
                        //sleep 2s for master getting all the failure information
                        Util.noExceptionSleep(2000);
                        if (this.leaderFailureHandledSet.isEmpty())
                            return;
                        copyAllFilesForFailureNodes();
                    }
                    break;
                }
            }

        });
        this.metaBackupThread.scheduleAtFixedRate(() -> {
            //Logic here for backup sdfsFileMap
            String leader = this.node.getLeader();
            if (!leader.equals(this.node.getHostName())) return;

            ArrayList<String> hosts = new ArrayList<>(Arrays.asList(this.node.getNodesArray()));
            Collections.shuffle(hosts);
            hosts.remove(this.node.getHostName());
            for (int i = 0; i < 3; i++) {
                try {
                    Socket backupSocket = Util.connectToServer(hosts.get(i), Config.TCP_PORT);
                    FileCommand backupFileCommand = new FileCommand("requestBackup", hosts.get(i), "", 0);
                    backupFileCommand.setBackup(this.sdfsFileMap);
                    FileCommandResult res = sendFileCommandViaSocket(backupFileCommand, backupSocket);
                    if (res.isHasError()) {
                        logger.debug("Fail to ask node <{}> to request backup", hosts.get(i));
                    }
                } catch (IOException e) {
                    logger.debug("Fail to establish connection with <{}>", hosts.get(i));
                }
            }

        }, 10, Config.BACKUP_PERIOD, TimeUnit.SECONDS);
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

    private void copyAllFilesForFailureNodes() {
        logger.info("Leader <{}> starts failure recovery", this.node.getLeader());
        //before the leader handler the error, it must get the lastest backup
        if (this.node.isLeaderChanged.get()) {
            logger.info("New leader starts gather backup");
            this.node.isLeaderChanged.set(false);
            for (String host : this.node.getMemberList().keySet()) {
                try {
                    Socket s = Util.connectToServer(host, Config.TCP_PORT);
                    FileCommandResult res = sendFileCommandViaSocket(new FileCommand("backup", host, "", 0), s);
                    if (res.isHasError()) {
                        logger.info("Fail to ask <{}> for backup", host);
                        continue;
                    }
                    LocalDateTime backupTime = res.getTimestamp();
                    if (backupTime.isAfter(this.lastBackupTime)) {
                        this.sdfsFileMap = res.getBackup();
                        logger.info("Latest backup from <{}>", host);
                    }
                } catch (IOException e) {
                    logger.debug("Fail to establish connection with <{}>", host, e);
                    continue;
                }
            }
        }
        Util.noExceptionSleep(2000);
        logger.info("Start handle failure nodes of length <{}>", this.leaderFailureHandledSet.size());
        this.sdfsFileMap.forEach((fileName, fileObjects) -> {
            for (FileObject fo : fileObjects) {
                Set<String> needHandleNodes = new HashSet<>();
                Set<String> repNodes = fo.getReplicaLocations();
                for (String failure : this.leaderFailureHandledSet.keySet()) {
                    if (repNodes.contains(failure)) {
                        needHandleNodes.add(failure);
                        repNodes.remove(failure);
                    }
                }
                if (needHandleNodes.isEmpty()) continue;
                logger.debug("Number of replica candidate:<{}>", String.join(", ", repNodes));
                String targetNode = repNodes.toArray(new String[0])[0];
                if (targetNode == null) {
                    logger.error("TargetNode is NULL");
                } else {
                    logger.info("TargetNode is {}", targetNode);
                }
                ArrayList<String> allAliveHost = new ArrayList<>(Arrays.asList(this.node.getNodesArray()));
                Collections.shuffle(allAliveHost);
                int sampleSize = needHandleNodes.size();
                int i = 0;
                for (String host : allAliveHost) {
                    if (i >= sampleSize) break;
                    if (repNodes.contains(host)) continue;
                    if (host.equals(this.node.getLeader())) {
                        FileCommand fc = new FileCommand("requestReplica", targetNode, fileName, fo.getVersion());
                        try {
                            Socket socket = Util.connectToServer(targetNode, Config.TCP_PORT);
                            FileCommandResult result = sendFileCommandViaSocket(fc, socket);
                            if (result.isHasError()) {
                                logger.error("Has err");
                                continue;
                            }
                            //no error, insert information into sdfs map
                            fo.getReplicaLocations().add(host);
                            i++;
                        } catch (IOException e) {
                            logger.error("crashHandler err", e);
                        }
                    } else {
                        try {
                            Socket socket = Util.connectToServer(host, Config.TCP_PORT);
                            FileCommand f = new FileCommand("getReplica", targetNode, fileName, fo.getVersion());
                            FileCommandResult res = sendFileCommandViaSocket(f, socket);
                            if (res.isHasError()) {
                                logger.error("RES fail");
                                continue;
                            }
                            //no error, insert information into sdfs map
                            fo.getReplicaLocations().add(host);
                            i++;
                        } catch (IOException e) {
                            logger.error("Crash handle fail", e);
                        }
                    }
                }
            }
        });
        this.leaderFailureHandledSet.clear();
        logger.info("Crash recovery Finished!!!");
    }

    public void stopServer() {
        this.isFileServerRunning = false;
        this.metaBackupThread.shutdown();
        this.dataBackupThread.shutdown();
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
        Instant start = Instant.now();
        String leader = this.node.getLeader();
        if (leader.isEmpty()) {
            logger.error("Leader empty, can not put");
            return;
        }
        FileCommandResult queryResault = query(sdfsFileName);
        if (queryResault != null && queryResault.getVersion() >= 0) {
            int newVersion = queryResault.getVersion() + 1;
            List<FileObject> newList;
            if (newVersion == 1) {
                newList = new ArrayList<>(10);
                this.localFileMap.put(sdfsFileName, newList);
            } else {
                newList = this.localFileMap.get(sdfsFileName);
            }
            FileObject fo = new FileObject(newVersion);
            FileCommand cmd = new FileCommand("put", leader, sdfsFileName, newVersion);
            FileCommandResult res = null;
            try {
                Socket s = Util.connectToServer(leader, Config.TCP_PORT);
                res = sendFileCommandViaSocket(cmd, s);
                if (res.isHasError()) {
                    logger.info("master put error");
                    return;
                }
                localCopyFileToStorage(new File(localFileName), fo.getUUID(), true);
                newList.add(fo);
                logger.info("local replication finished");
            } catch (IOException e) {
                logger.debug("Failed to put", e);
            }
            if (res == null) {
                logger.error("FileCommandResult is null");
                return;
            }
            for (String host : res.getReplicaNodes()) {
                if (host.equals(this.node.getHostName())) {
                    continue;
                }
                //TODO: Multi-thread send?
                Socket replicaSocket;
                try {
                    replicaSocket = Util.connectToServer(host, Config.TCP_FILE_TRANS_PORT);
                    File toSend = new File(Config.STORAGE_PATH, fo.getUUID());
                    sendFileViaSocket(toSend, replicaSocket, sdfsFileName, newVersion, "put", "");
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
        Instant end = Instant.now();
        logger.info("put takes <{}>", end.getEpochSecond() - start.getEpochSecond());
    }

    public void get(String sdfsFileName, String localFileName) {
        Instant start = Instant.now();
        // Not in local, get from Master
        String leader = this.node.getLeader();
        if (leader.isEmpty()) {
            logger.error("Leader empty, can not get");
            return;
        }
        FileCommandResult queryResault = query(sdfsFileName);
        if (queryResault != null && queryResault.getVersion() >= 0) {
            if(queryResault.getReplicaNodes() == null){
                logger.info("Requested file not stored");
            }
            for (String host : queryResault.getReplicaNodes()) {
                try {
                    Socket getSocket = Util.connectToServer(host, Config.TCP_PORT);
                    FileCommandResult getResult = sendFileCommandViaSocket(new FileCommand("get", localFileName, sdfsFileName, 0), getSocket);
                    if (getResult.isHasError()) {
                        logger.info("get error with <{}>", host);
                    } else {
                        long totalSleepingTime = 0;
                        long timeout = 100;
                        while (!this.hasReceivedSuccess.get() && totalSleepingTime < Config.FILE_RECV_TIMEOUT_MILLSECOND) {
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
                            logger.info("File <{}> get failed from <{}>", sdfsFileName, host);
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
        Instant end = Instant.now();
        logger.info("get takes <{}>", end.getEpochSecond() - start.getEpochSecond());
    }

    private FileObject getLatestLocalVersion(String sdfsFileName) {
        List<FileObject> foList = this.localFileMap.get(sdfsFileName);
        if (foList == null) return null;
        if (foList.size() == 0) return null;
        return foList.get(foList.size() - 1);
    }

    public void delete(String sdfsFileName) {
        String leader = this.node.getLeader();
        if (leader.isEmpty()) {
            logger.error("Leader empty, can not delete");
            return;
        }
        //leader delete
        if (this.node.getHostName().equals(leader)) {
            List<FileObject> fileList = this.sdfsFileMap.get(sdfsFileName);
            if (fileList == null) {
                logger.info("delete finished");
            } else {
                this.sdfsFileMap.remove(sdfsFileName);
                this.lastBackupTime = LocalDateTime.now();
                for (FileObject file : fileList) {
                    for (String host : file.getReplicaLocations()) {
                        try {
                            if (host.equals(this.node.getHostName())) {
                                this.localFileMap.remove(sdfsFileName);
                            } else {
                                Socket deleteSocket = Util.connectToServer(host, Config.TCP_PORT);
                                FileCommandResult res = sendFileCommandViaSocket(new FileCommand("delete", host, sdfsFileName, 0), deleteSocket);
                                if (res.isHasError()) {
                                    logger.debug("Fail to ask node <{}> to delete", host);
                                }
                            }
                        } catch (IOException e) {
                            logger.debug("Failed to establish connection with <{}>", host, e);
                        }
                    }
                }
                logger.info("delete finished!!!");
            }
        } else {   //member delete
            try {
                Socket deleteSocket = Util.connectToServer(leader, Config.TCP_PORT);
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
        List<FileObject> fileObjects = this.sdfsFileMap.get(sdfsFileName);
        if (fileObjects == null) {
            logger.info("<{}> not stored", sdfsFileName);
            return;
        }
        for (FileObject file : fileObjects) {
            logger.info("File version <{}> replica at: {}", file.getVersion(), String.join(", ", file.getReplicaLocations()));
        }
    }

    /**
     * Ask leader for a backup of sdfsFileMap
     */
    private void askBackup() {
        String leader = this.node.getLeader();
        if (leader.isEmpty()) {
            logger.error("Leader empty, can not list file in SDFS");
        } else {
            try {
                Socket s = Util.connectToServer(leader, Config.TCP_PORT);
                FileCommandResult result = sendFileCommandViaSocket(new FileCommand("backup", leader, "", 0), s);
                if (result.isHasError()) {
                    logger.debug("error when requesting backup");
                } else {
                    this.lastBackupTime = result.getTimestamp();
                    this.sdfsFileMap = result.getBackup();
                    //logger.info("backup from <{}> finished at <{}>", leader, this.lastBackupTime);
                }
            } catch (IOException e) {
                logger.debug("Fail to establish coonection with <{}>", leader, e);
            }
        }
    }

    public void listFileLocal() {
        logger.info("local file includes");
        for (String file : this.localFileMap.keySet()) {
            for (FileObject fo : this.localFileMap.get(file)) {
                int version = fo.getVersion();
                logger.info("name: <{}>     verison: <{}>", file, version);
            }
        }
    }

    public void getVersions(String sdfsFileName, String numVersions, String localFileName) {
        String leader = this.node.getLeader();
        if (leader.isEmpty()) {
            logger.error("Leader empty, can get versions in SDFS");
            return;
        }
        if (!this.node.getLeader().equals(this.node.getHostName())) {
            // Ask leader for all file locations, trick
            askBackup();
        }
        if (this.sdfsFileMap.size() == 0) {
            logger.error("No file ever stored in cluster");
            return;
        }
        HashMap<Integer, Set<String>> versionLocations = this.transformNumOfVersion(this.sdfsFileMap.get(sdfsFileName));
        int latestVersion = latestVersion(versionLocations);
        int numOfLatestVersions = Integer.valueOf(numVersions);
        if (latestVersion - numOfLatestVersions < 0) {
            logger.error("Too many version history requested. Only have <{}>, asked for <{}>", latestVersion, numOfLatestVersions);
            return;
        }
        for (int targetVer = latestVersion; targetVer > latestVersion - numOfLatestVersions; targetVer--) {
            Set<String> potentialNodes = versionLocations.get(targetVer);
            for (String node : potentialNodes) {
                logger.info("Getting version <{}> for <{}> from <{}>", targetVer, sdfsFileName, node);
                try {
                    Socket s = Util.connectToServer(node, Config.TCP_PORT);
                    // Send version i of x file to me
                    FileCommand f = new FileCommand("requestVersion", localFileName, sdfsFileName, targetVer);
                    FileCommandResult fcr = sendFileCommandViaSocket(f, s);
                    if (fcr.isHasError()) {
                        logger.error("Remote <{}> does not have version <{}> for <{}>", node, targetVer, sdfsFileName);
                    } else {
                        // Got one version, enough
                        logger.info("Success get version <{}> for <{}> from <{}>", targetVer, sdfsFileName, node);
                        break;
                    }
                } catch (IOException e) {
                    logger.debug("Fail to ask others for versions", e);
                }
            }
        }
    }

    private int latestVersion(HashMap<Integer, Set<String>> vL) {
        // Find max of version # because stream().max() not working
        int latestVersion = Integer.MIN_VALUE;
        for (Integer i : vL.keySet()) {
            if (i > latestVersion) {
                latestVersion = i;
            }
        }
        return latestVersion;
    }

    /**
     * Flatten the fileObjects to a {1:Hosts; 2:Hosts...}
     */
    private HashMap<Integer, Set<String>> transformNumOfVersion(List<FileObject> fileObjects) {
        HashMap<Integer, Set<String>> res = new HashMap<>();
        for (FileObject fo : fileObjects) {
            int ver = fo.getVersion();
            if (!res.containsKey(ver)) {
                res.put(ver, new HashSet<>());
            }
            Set<String> hosts = res.get(ver);
            hosts.addAll(fo.getReplicaLocations());
        }
        return res;
    }

    public void printAll(){
        if(this.node.getLeader().equals(this.node.getHostName())){
            logger.info("Last backup time: ", lastBackupTime.toString());
            this.sdfsFileMap.forEach((fileName, fileObjects) ->{
                logger.info("file Name: <{}>", fileName);
                for(FileObject fo: fileObjects){
                    logger.info("   version: <{}>", fo.getVersion());
                    logger.info("       replica: <{}>", String.join(", ",fo.getReplicaLocations()));
                }
            });
        }else{
            logger.info("Only the leader can print all");
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
        //logger.debug("Copy file from <{}> to <{}>", originalPath.getAbsolutePath(), dest.getAbsolutePath());
        try (BufferedInputStream is = new BufferedInputStream(new FileInputStream(originalPath))) {
            try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(dest))) {
                bufferedReadWrite(is, os, 8192);
            }
        }
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
            // logger.info("file command sent at '{}'.", fc.getTimestamp());

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
            //logger.info("file command result sent at '{}'.", fcs.getTimestamp());
        } catch (IOException e) {
            logger.debug("Failed to establish connection", e);
        }

    }

    /**
     * Just send the file via socket, no closing socket
     *
     * @param toSendFile        File path for the file you want to send
     * @param socket            A socket connects to remote host
     * @param sdfsName          SDFS name to send to client
     * @param intention         Send for get or put
     * @param fileNameForNonPut If not putting, then need to specify file name
     */
    private void sendFileViaSocket(File toSendFile, Socket socket, String sdfsName, int version, String intention, String fileNameForNonPut) throws IOException {
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(toSendFile))) {
            //logger.debug("[{}] Sending <{}>({}b) version <{}> to <{}>", intention, toSendFile.getAbsolutePath(), toSendFile.length(), version, socket.getRemoteSocketAddress());
            DataOutputStream dOut = new DataOutputStream(socket.getOutputStream());
            dOut.writeUTF(intention.toLowerCase());
            dOut.writeUTF(sdfsName);
            dOut.writeInt(version);
            dOut.writeLong(toSendFile.length());
            dOut.writeUTF(fileNameForNonPut);
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
                Socket s = Util.connectToServer(leader, Config.TCP_PORT);
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
            String remoteHn = socket.getInetAddress().getHostName();
            try {
                DataInputStream dIn = new DataInputStream(socket.getInputStream());
                intention = dIn.readUTF();
                String sdfsName = dIn.readUTF();
                int fileVersion = dIn.readInt();
                long fileSize = dIn.readLong();
                String extraInfo = dIn.readUTF();
                //logger.debug("[{}] Receiving file <{}>({}b) version <{}> from <{}>", intention, sdfsName, fileSize, fileVersion, remoteHn);
                File dest;
                switch (intention) {
                    case "put":
                        FileObject fo = new FileObject(fileVersion);
                        dest = new File(Config.STORAGE_PATH, fo.getUUID());
                        saveFileViaSocketInput(dIn, dest);
                        // Only update list when save file is successful
                        if (!this.localFileMap.containsKey(sdfsName)) {
                            this.localFileMap.put(sdfsName, new ArrayList<>(10));
                        }
                        this.localFileMap.get(sdfsName).add(fo);
                        break;
                    case "get":
                        dest = new File(Config.GET_PATH, extraInfo);
                        saveFileViaSocketInput(dIn, dest);
                        this.hasReceivedSuccess.set(true);
                        break;
                    case "version":
                        dest = new File(Config.GET_PATH, String.format("%s-version-%d", extraInfo, fileVersion));
                        saveFileViaSocketInput(dIn, dest);
                        break;
                    default:
                        throw new IOException("Unknown intention");
                }
                //logger.debug("[{}] Got file <{}>({}b) version <{}> from <{}>", intention, sdfsName, fileSize, fileVersion, remoteHn);
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
            //logger.info("Connection from client <{}>", clientSocket.getRemoteSocketAddress());
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
                //logger.info("file command received from <{}>, type <{}>", clientSocket.getInetAddress().getHostName(), cmd.getType());
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
                    case "requestVersion":
                        requestVersionHandler(out, cmd, clientSocket.getInetAddress().getHostName());
                        clientSocket.close();
                        logger.info("Z1");
                        return;
                    case "requestBackup":
                        saveBackupHandler(out, cmd);
                        break;
                    case "crash":
                        if (this.node.getLeader().equals(this.node.getHostName())) {
                            crashHandler(out, cmd);
                        }
                        break;
                    case "getReplica":
                        getReplicaHandle(out, cmd);
                        break;
                    case "requestReplica":
                        requestReplicaHandle(out, cmd, clientSocket.getInetAddress().getHostName());
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
                // logger.info("Closed connection from client: <{}>", clientSocket.getRemoteSocketAddress());
            } catch (IOException e) {
                logger.error("Close socket failed", e);
            }
        };
    }

    private void requestVersionHandler(ObjectOutputStream out, FileCommand cmd, String targetHostname) {
        int targetV = cmd.getVersionNum();
        String saveToName = cmd.getHostName();
        String whichFile = cmd.getFileName();
        Optional<FileObject> oFo = this.localFileMap.get(whichFile).stream().filter(fo -> fo.getVersion() == targetV).findFirst();
        FileCommandResult fcr = new FileCommandResult(null, -1);
        if (!oFo.isPresent()) {
            logger.error("No file <{}> with version <{}> here", whichFile, targetV);
            fcr.setHasError(true);
            sendFileCommandResultViaSocket(out, fcr);
            return;
        }
        //logger.info("Got matching file, sending to {}", targetHostname);
        sendFileCommandResultViaSocket(out, fcr);
        // Send this file back
        FileObject file = oFo.get();
        try {
            Socket transSocket = Util.connectToServer(targetHostname, Config.TCP_FILE_TRANS_PORT);
            File toSend = new File(Config.STORAGE_PATH, file.getUUID());
            sendFileViaSocket(toSend, transSocket, whichFile, targetV, "version", saveToName);
            transSocket.close();
            //logger.info("Requested file <{}> version <{}> sent back", whichFile, targetV);
        } catch (IOException e) {
            logger.debug("Fail to send file", e);
        }
    }

    private void saveBackupHandler(ObjectOutputStream out, FileCommand cmd) {
        this.sdfsFileMap = cmd.getBackup();
        this.lastBackupTime = cmd.getTimestamp();
        FileCommandResult result = new FileCommandResult(null, 0);
        sendFileCommandResultViaSocket(out, result);
    }

    private void backupHandler(ObjectOutputStream out, String requesetHost) {
        FileCommandResult result = new FileCommandResult(null, 0);
        result.setBackup(this.sdfsFileMap);
        result.setTimestamp(this.lastBackupTime);
        sendFileCommandResultViaSocket(out, result);
    }

    private void requestReplicaHandle(ObjectOutputStream out, FileCommand cmd, String requestHost) {
        int version = cmd.getVersionNum();
        String fileName = cmd.getFileName();
        List<FileObject> fileObjects = this.localFileMap.get(fileName);
        for (FileObject fo : fileObjects) {
            if (fo.getVersion() == version) {
                try {
                    // Send the requested file back to requester
                    Socket socket = Util.connectToServer(requestHost, Config.TCP_FILE_TRANS_PORT);
                    File toSend = new File(Config.STORAGE_PATH, fo.getUUID());
                    sendFileViaSocket(toSend, socket, fileName, version, "put", "");
                    socket.close();
                    sendFileCommandResultViaSocket(out, new FileCommandResult(null, 0));
                    break;
                } catch (IOException e) {
                    logger.error("requestReplicaHandle err", e);
                }
            }
        }
    }

    private void getReplicaHandle(ObjectOutputStream out, FileCommand cmd) {
        String targetNode = cmd.getHostName();
        String fileName = cmd.getFileName();
        int version = cmd.getVersionNum();
        FileCommand fc = new FileCommand("requestReplica", targetNode, fileName, version);
        try {
            Socket socket = Util.connectToServer(targetNode, Config.TCP_PORT);
            FileCommandResult result = sendFileCommandViaSocket(fc, socket);
            if (result.isHasError()) {
                logger.error("Has err");
            }
            sendFileCommandResultViaSocket(out, new FileCommandResult(null, 0));
        } catch (IOException e) {
            logger.error("Get rep handle err", e);
        }
    }

    private void crashHandler(ObjectOutputStream out, FileCommand cmd) {
        String failedNodeHostname = cmd.getFileName();
        if (this.leaderFailureHandledSet.containsKey(failedNodeHostname)) {
            // Already handled before
            //logger.info("Already handled: <{}>", failedNodeHostname);
            sendFileCommandResultViaSocket(out, new FileCommandResult(null, 0));
            return;
        }
        this.leaderFailureHandledSet.put(failedNodeHostname, "");
        sendFileCommandResultViaSocket(out, new FileCommandResult(null, 0));
    }

    private void masterDeleteHandler(ObjectOutputStream out, FileCommand cmd, String requestHost) {
        String fileName = cmd.getFileName();
        List<FileObject> fileObjects = this.sdfsFileMap.get(fileName);
        if (fileObjects == null) return;
        for (FileObject deleteTarget : fileObjects) {
            FileCommandResult result = new FileCommandResult(null, 0);
            //check if sdfs has this file
            if (deleteTarget == null) {
                sendFileCommandResultViaSocket(out, result);
            } else {
                Set<String> replicaNodes = deleteTarget.getReplicaLocations();
                this.sdfsFileMap.remove(fileName);
                this.lastBackupTime = LocalDateTime.now();
                //ask all members to delete the file
                for (String host : replicaNodes) {
                    if (host.equals(this.node.getHostName())) {
                        this.localFileMap.remove(fileName);
                    } else {
                        try {
                            Socket s = Util.connectToServer(host, Config.TCP_PORT);
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
    }

    private void memberDeleteHandler(ObjectOutputStream out, FileCommand cmd, String requestHost) {
        String fileName = cmd.getFileName();
        if (this.localFileMap.get(fileName) != null) {
            this.localFileMap.remove(fileName);
            sendFileCommandResultViaSocket(out, new FileCommandResult(null, 0));
            //logger.info("delete <{}> requested by master <{}>", fileName, requestHost);
        } else {
            sendFileCommandResultViaSocket(out, new FileCommandResult(null, 0));
        }
    }


    private void getHandler(ObjectOutputStream out, FileCommand cmd, String requestHost) {
        String sdfsFileName = cmd.getFileName();
        String saveToName = cmd.getHostName();
        FileCommandResult result = new FileCommandResult(null, 0);
        FileObject file = this.getLatestLocalVersion(sdfsFileName);
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
            Socket transSocket = Util.connectToServer(requestHost, Config.TCP_FILE_TRANS_PORT);
            File toSend = new File(Config.STORAGE_PATH, file.getUUID());
            sendFileViaSocket(toSend, transSocket, sdfsFileName, result.getVersion(), "get", saveToName);
            transSocket.close();
            //logger.info("Requested file <{}> version <{}> sent back", sdfsFileName, result.getVersion());
        } catch (IOException e) {
            logger.debug("Fail to send file", e);
        }
    }

    private void putHandler(ObjectOutputStream out, FileCommand cmd, String clientHostname) {
        int version = cmd.getVersionNum();
        String fileName = cmd.getFileName();
        if (version == 0) {
            //Not possible, but who knows
            logger.error("Version is 0, WTF");
        }
        if (version == 1) {
            // Add a list if version is 1
            this.sdfsFileMap.put(fileName, new ArrayList<>(10));
            this.lastBackupTime = LocalDateTime.now();
            logger.debug("Version is 1, add new");
        }
        List<FileObject> thisFileList = this.sdfsFileMap.get(fileName);
        if (thisFileList == null) logger.error("null");
        //store new file or old file with new version
        ArrayList<String> hosts = new ArrayList<>(Arrays.asList(this.node.getNodesArray()));
        Collections.shuffle(hosts);
        hosts.remove(clientHostname);
        if (hosts.size() >= 3) {
            Set<String> replicaNodes = new HashSet<>();
            replicaNodes.add(clientHostname);
            replicaNodes.add(hosts.get(1));
            replicaNodes.add(hosts.get(2));
            replicaNodes.add(hosts.get(0));
            //logger.warn("Selected replica nodes: {}", String.join(", ", replicaNodes));
            //set sdfs meta information
            FileObject newFile = new FileObject(version);
            newFile.setReplicaLocations(replicaNodes);
            thisFileList.add(newFile);
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

    private void queryHandler(ObjectOutputStream out, String fileName) {
        int version = 0;
        Set<String> replicaLocations = null;
        for (String file : this.sdfsFileMap.keySet()) {
            if (!file.equals(fileName)) continue;
            List<FileObject> fileObjects = this.sdfsFileMap.get(file);
            FileObject last = fileObjects.get(fileObjects.size() - 1);
            version = last.getVersion();
            replicaLocations = last.getReplicaLocations();
        }
        FileCommandResult fcs = new FileCommandResult(replicaLocations, version);
        sendFileCommandResultViaSocket(out, fcs);
    }


}
