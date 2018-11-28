package cs425.crane.node;

import cs425.Util;
import cs425.Config;
import cs425.crane.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cs425.mp3.FileOperation;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.*;
import java.util.Random;

/**
 * crane node
 */
public class CraneNode {
    private final Logger logger = LoggerFactory.getLogger(CraneNode.class);
    //FileOperation from mp3
    private final FileOperation fNode;

    //Variables for task assignment
    private final ServerSocket taskAssignmentSocket;
    private final ExecutorService taskAssignmentListenerThread;

    //Variables for crane system control
    private final ExecutorService processThread;
    private final boolean isCraneRunning;
    private final String serverHostname;
    private int nextPortToAssign = Config.TCP_TUPLE_TRANS_BASE_PORT;
    private HashMap<String, String> taskLocationMap;
    private HashMap<String, String> taskTypeMap;


    /**
     * initialize the CraneNode with a FileOperation node.
     *
     * @param fOper FileOperation node
     * @throws IOException
     */
    public CraneNode(FileOperation fOper) throws IOException {
        this.fNode = fOper;
        this.serverHostname = InetAddress.getLocalHost().getCanonicalHostName();
        this.processThread = Executors.newFixedThreadPool(Config.NUM_CORES * 2);
        this.taskAssignmentSocket = new ServerSocket(Config.TCP_TASK_ASSIGNMENT_PORT);
        this.taskAssignmentListenerThread = Executors.newSingleThreadExecutor();
        this.isCraneRunning = true;
        this.taskLocationMap = new HashMap<>();
        this.taskTypeMap = new HashMap<>();
        initialMainThreadsJob();
    }

    /**
     * initialize all crane system threads
     */
    private void initialMainThreadsJob() {
        this.taskAssignmentListenerThread.submit(() -> {
            Thread.currentThread().setName("crane-task-assignment-listener");
            logger.info("task assignment thread started listening on <{}>...", this.serverHostname);
            while (this.isCraneRunning) {
                try {
                    if (this.taskAssignmentSocket.isClosed()) continue;
                    this.processThread.submit(this.taskListener(this.taskAssignmentSocket.accept()));
                } catch (IOException e) {
                    logger.error("Task assignment socket failed", e);
                }
            }
        });
    }

    /**
     * start stream execution. First assign the tasks to different workers or ask master to do the assignment.
     *
     * @param topofile input topology txt file
     */
    public void execute(String topofile) {
        //if the current node is master, reader the topofile and assign the tasks
        if (this.fNode.node.getLeader().equals(this.fNode.node.getHostName())) {
            //first get the topofile to local machine from SDFS
            this.fNode.get(topofile, topofile);
            try (BufferedReader br = new BufferedReader(new FileReader(Config.GET_PATH + "/" + topofile))) {
                String line;
                //parse the file line by line - taskType # src # taskName # dest # object # numOfThreads
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(" # ");
                    String taskType = parts[0];
                    if (parts.length < 6) {
                        break;
                    }
                    String src = parts[1];
                    String taskName = parts[2];
                    String dest = parts[3];
                    String object = parts[4];
                    int numOfThreads = Integer.parseInt(parts[5]);
                    switch (taskType) {
                        case "spout":
                            assignTask("spout", src, taskName, dest, object, numOfThreads);
                            break;
                        case "bolt":
                            assignTask("bolt", src, taskName, dest, object, numOfThreads);
                            break;
                        case "sink":
                            assignTask("sink", src, taskName, dest, object, numOfThreads);
                            break;
                        default:
                            logger.error("Unknown task type");
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to open file", e);
            }
        } else { //ask master to assign the tasks
            try {
                TaskMessage msg = new TaskMessage("assignment", topofile, "", "", "", 0);
                AckMessage res = sendTaskMessageViaSocket(Util.connectToServer(this.fNode.node.getLeader(), Config.TCP_TASK_ASSIGNMENT_PORT), msg);
                if (!res.isFinished()) {
                    logger.error("Failed to ask master to assign tasks");
                }
            } catch (IOException e) {
                logger.error("Failed to connected to master", e);
            }
        }
    }

    /**
     * Listener thread that process the task assigned by master. Should create work for valid task received.
     *
     * @param s Socket that accept the master connection
     * @return Listener
     */
    private Runnable taskListener(Socket s) {
        return () -> {
            Thread.currentThread().setName("task-listener");
            try {
                // Output goes first or the input will block forever
                ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(s.getInputStream()));
                TaskMessage task = TaskMessage.parseFromStream(in);

                //for test
/*                logger.info(
                        "Task received.\n" +
                                "type: <{}>\n" +
                                "src: <{}>\n" +
                                "name: <{}>\n" +
                                "dest: <{}>\n" +
                                "obj: <{}>\n" +
                                "port: <{}>",
                        task.getType(), task.getSrc(), task.getName(), task.getDest(), task.getObject(), task.getPort());*/

                AckMessage ack = new AckMessage(UUID.randomUUID(), true);
                //master receive the assignment request, call the execute again to assign the tasks
                if (task.getType().equals("assignment")) {
                    execute(task.getSrc());
                    sendAckMessageViaStream(out, ack);
                } else if (task.getType().equals("spout") || task.getType().equals("bolt") || task.getType().equals("sink")) {
                    this.processThread.submit(this.worker(task, out));
                } else {
                    logger.error("Unknown task type received", task.getType());
                }
            } catch (Exception e) {
                logger.error("Failed to execute the assigned task", e);
            }
        };
    }

    /**
     * Main worker thread to execute the task. Should send ack message to master once topology is set up.
     *
     * @param task task message that contains all the information about the task
     * @param out  output stream to reply ack to master
     * @return worker
     */
    private Runnable worker(TaskMessage task, ObjectOutputStream out) {
        return () -> {
            Thread.currentThread().setName(task.getName());
            logger.info("Worker thread for <{}> starts", Thread.currentThread().getName());
            //get the object code from the sdfs.
            String[] sourceHostPlusPort;
            String sourceHost;
            int sourcePort;
            int thisPort = task.getPort();
            //sourceSocket is used for receiving stream from upper node, destSocket is used for sending stream for lower node.
            Socket sourceSocket, destSocket;
            ServerSocket destServerSocket;
            AckMessage ack = new AckMessage(UUID.randomUUID(), true);
            sendAckMessageViaStream(out, ack);
            switch (task.getType()) {
                case "spout":
                    //listen for dest node connection
                    while (true) {
                        try {
                            destServerSocket = new ServerSocket(thisPort);
                            if (destServerSocket.isClosed()) continue;
                            logger.info("<{}> listen at port <{}>", task.getName(),thisPort);
                            destSocket = destServerSocket.accept();
                            logger.info("Stream connection from <{}> to <{}> set up", task.getDest(), task.getName());
                            destSocket.close();
                            break;
                        } catch (IOException e) {
                            logger.error("<{}> at <{}> failed to listen on port <{}>", task.getName(), this.serverHostname, thisPort, e);
                        }
                    }
                    break;
                case "bolt":
                    //try to connected to source node
                    sourceHostPlusPort = task.getSrc().split("\\+");
                    sourceHost = sourceHostPlusPort[0];
                    sourcePort = Integer.parseInt(sourceHostPlusPort[1]);
                    try {
                        logger.info("<{}> trying to connect to <{}> at port <{}>", task.getName(),sourceHost, sourcePort);
                        sourceSocket = Util.connectToServer(sourceHost, sourcePort);
                        //listen for dest node connection
                        logger.info("bolt:here1");
                        while (true) {
                            try {
                                destServerSocket = new ServerSocket(thisPort);
                                if (destServerSocket.isClosed()) continue;
                                destSocket = destServerSocket.accept();
                                logger.info("Stream connection from <{}> to <{}> set up", task.getDest(), task.getName());
                                ack = new AckMessage(UUID.randomUUID(), true);
                                sendAckMessageViaStream(out, ack);
                                destSocket.close();
                                break;
                            } catch (IOException e) {
                                logger.error("<{}> at <{}> failed to listen on port <{}>", task.getName(), this.serverHostname, thisPort, e);
                            }
                        }
                    } catch (IOException e) {
                        logger.error("Worker <{}> failed to connected to <{}>", task.getName(), task.getSrc(), e);
                    }
                    break;
                case "sink":
                    //try to connected to source node
                    sourceHostPlusPort = task.getSrc().split("\\+");
                    sourceHost = sourceHostPlusPort[0];
                    sourcePort = Integer.parseInt(sourceHostPlusPort[1]);
                    try {
                        logger.info("<{}> trying to connect to <{}> at port <{}>", task.getName(),sourceHost, sourcePort);
                        sourceSocket = Util.connectToServer(sourceHost, sourcePort);
                        //listen for dest node connection
                        while (true) {
                            try {
                                logger.info("blocked here");
                                destServerSocket = new ServerSocket(thisPort);
                                if (destServerSocket.isClosed()) continue;
                                destSocket = destServerSocket.accept();
                                logger.info("Stream connection from <{}> to <{}> set up", task.getDest(), task.getName());
                                ack = new AckMessage(UUID.randomUUID(), true);
                                sendAckMessageViaStream(out, ack);
                                destSocket.close();
                                break;
                            } catch (IOException e) {
                                logger.error("<{}> at <{}> failed to listen on port <{}>", task.getName(), this.serverHostname, thisPort, e);
                            }
                        }
                    } catch (IOException e) {
                        logger.error("Worker <{}> failed to connected to <{}>", task.getName(), task.getSrc(), e);
                    }
                    break;
                default:
                    logger.error("Unknown task type received: <{}>", task.getType());
            }
        };
    }

    /**
     * Send the task assignment message via connected socket.
     *
     * @param s  already connected TCP socket
     * @param tm task assignment message
     */
    private AckMessage sendTaskMessageViaSocket(Socket s, TaskMessage tm) throws IOException {
        AckMessage res = null;
        try {
            // Output goes first or the input will block forever
            ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(s.getOutputStream()));
            ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(s.getInputStream()));

            out.writeObject(tm);
            out.flush();
            // Some blocking here for sure
            res = AckMessage.parseFromStream(in);
            // Communication finished, notice the sequence
            in.close();
            out.close();
            s.close();

        } catch (ClassNotFoundException e) {
            logger.error("Client received malformed data!");
        }
        return res;
    }

    /**
     * Send AckMessage into a output stream.
     *
     * @param out object output stream for sending AckMessage back
     * @param ack AckMessage to send back
     */
    private void sendAckMessageViaStream(ObjectOutputStream out, AckMessage ack) {
        try {
            out.writeObject(ack);
            out.flush();
            //logger.info("file command result sent at '{}'.", fcs.getTimestamp());
        } catch (IOException e) {
            logger.error("Failed to send ack", e);
        }

    }

    /**
     * Master assign task to random workers with all the necessary information.
     *
     * @param taskType     specify the task type - spout, bolt or sink
     * @param src          used by spout to specify what the input file name is in the SDFS
     * @param taskName     specify the task name
     * @param dest         For sink it is the file name to write the result, for bolt and spout it is node socket address to pass the processed stream.
     * @param object       task source object name in the SDFS
     * @param numOfThreads number of threads requested for this task
     */
    private void assignTask(String taskType, String src, String taskName, String dest, String object, int numOfThreads) throws IOException {
        for (int i = 0; i < numOfThreads; i++) {
            //pick random node to assign the task
            Set<String> hosts = this.fNode.node.getMemberList().keySet();
            int randomIndex = new Random().nextInt(hosts.size());
            int j = 0;
            for (String host : hosts) {
                if (j == randomIndex) {
                    TaskMessage msg;
                    //for simplicity just let each task listened on a distinct port
                    //Todo:add multiple thread feature
                    String hostPlusPort = host + "+" + Integer.toString(this.nextPortToAssign);
                    this.taskTypeMap.put(taskName, taskType);
                    logger.info("Task <{}> assigned to <{}>", taskName, hostPlusPort);
                    switch (taskType) {
                        case "spout":
                            this.taskLocationMap.put(taskName,hostPlusPort);
                            msg = new TaskMessage(taskType, src, taskName, dest, object, this.nextPortToAssign);
                            break;
                        case "bolt":
                            this.taskLocationMap.put(taskName, hostPlusPort);
                            msg = new TaskMessage(taskType, this.taskLocationMap.get(src), taskName, dest, object, this.nextPortToAssign);
                            break;
                        case "sink":
                            this.taskLocationMap.put(taskName, hostPlusPort);
                            msg = new TaskMessage(taskType, this.taskLocationMap.get(src), taskName, dest, object, this.nextPortToAssign);
                            break;
                        default:
                            logger.error("Unknown task type to assign", taskType);
                            return;
                    }
                    AckMessage res = sendTaskMessageViaSocket(Util.connectToServer(host, Config.TCP_TASK_ASSIGNMENT_PORT), msg);
                    if (!res.isFinished()) {
                        logger.error("Failed to assign task to node: <{}>", host);
                    }
                }
                j++;
            }
            this.nextPortToAssign += 1;
        }

    }

}
