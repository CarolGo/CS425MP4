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
    private HashMap<String, Integer> taskPortMap;


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
        this.taskPortMap = new HashMap<>();
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
                    this.processThread.submit(this.taskAssignmentExecutor(this.taskAssignmentSocket.accept()));
                } catch (IOException e) {
                    logger.error("Task assignment socket failed", e);
                }
            }
        });
    }

    /**
     * start stream execution. First assign the tasks to different workers or ask master to do the assignment.
     *
     * @param topofile
     */
    public void execute(String topofile) {
        //first get the topofile to local machine from SDFS
        this.fNode.get(topofile, topofile);
        try (BufferedReader br = new BufferedReader(new FileReader(Config.GET_PATH + "/" + topofile))) {
            String line;
            //parse the file line by line - taskType # src # taskName # dest # object # numOfThreads
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(" # ");
                String taskType = parts[0];
                if(parts.length < 6){
                    break;
                }
                String src = parts[1];
                String taskName = parts[2];
                String dest = parts[3];
                String object = parts[4];
                int numOfThreads = Integer.parseInt(parts[5]);
                //if the current node is master, reader the topofile and assign the tasks
                if (this.fNode.node.getLeader().equals(this.fNode.node.getHostName())) {
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


                } else { //ask master to assign the tasks
                    TaskMessage msg = new TaskMessage("assignment", topofile, "","", "");
                    AckMessage res = sendTaskMessageViaSocket(Util.connectToServer(this.fNode.node.getLeader(), Config.TCP_TASK_ASSIGNMENT_PORT), msg);
                    if (!res.isFinished()) {
                        logger.error("Failed to ask master to assign tasks");
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to open file", e);
        }
    }

    /**
     * Listener for task assignment from the master .
     *
     * @param s Socket that accept the master connection
     * @return Listener
     */
    private Runnable taskAssignmentExecutor(Socket s) {
        return () -> {
            Thread.currentThread().setName("task-assignment-executor");
            try {
                // Output goes first or the input will block forever
                ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(s.getInputStream()));
                TaskMessage task = TaskMessage.parseFromStream(in);

                //for test
                logger.info("Task received.\n" +
                        "type: <{}>\n" +
                        "src: <{}>\n" +
                        "name: <{}>\n" +
                        "dest: <{}>\n" +
                        "obj: <{}>", task.getType(), task.getSrc(), task.getName(), task.getDest(), task.getObject());
                AckMessage ack = new AckMessage(UUID.randomUUID(), true);

                switch (task.getType()) {
                    case "assignment": //master receive the assignment request, call the execute to assign the tasks
                        execute(task.getSrc());
                        break;
                    case "spout":
                        sendAckMessageViaStream(out, ack);
                        //Todo:submit the user input object code to new spout worker threads, set topology, write task assignment ack back to master after open(), then execute
                        break;
                    case "bolt":
                        sendAckMessageViaStream(out, ack);
                        //Todo:submit the user input object code to new bolt worker threads, set topology, write task assignment ack back to master after prepare(), then execute
                        break;
                    case "sink":
                        sendAckMessageViaStream(out, ack);
                        //Todo:submit the user input object code to new sink worker threads, set topology, write task assignment ack back to master after prepare(), then execute
                        break;
                    default:
                        logger.error("Unknown task type received", task.getType());
                }
            } catch (Exception e) {
                logger.error("Failed to execute the assigned task", e);
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
     * Master assign task to random workers.
     *
     * @param taskType     specify the task type - spout, bolt or sink
     * @param src          used by spout to specify what the input file name is in the SDFS
     * @param taskName     specify the task name
     * @param dest         used by sink or bolt to specify what the destination stream or file is
     * @param object       task source object name in the SDFS
     * @param numOfThreads number of threads requested for this task
     */
    //Todo: should master assign the ports for each task?
    private void assignTask(String taskType, String src, String taskName, String dest, String object, int numOfThreads) throws IOException {
        for (int i = 0; i < numOfThreads; i++) {
            //pick random node to assign the task
            Set<String> hosts = this.fNode.node.getMemberList().keySet();
            int randomIndex = new Random().nextInt(hosts.size());
            logger.info("random index: <{}>, size: <{}>", randomIndex, hosts.size());
            int j = 0;
            for (String host : hosts) {
                if (j == randomIndex) {
                    logger.info(host);
                    TaskMessage msg = new TaskMessage(taskType, src, taskName, dest, object);
                    AckMessage res = sendTaskMessageViaSocket(Util.connectToServer(host, Config.TCP_TASK_ASSIGNMENT_PORT), msg);
                    if (!res.isFinished()) {
                        logger.error("Failed to assign task to node: {<>}", host);
                    }
                }
                j++;
            }

        }

    }

}
