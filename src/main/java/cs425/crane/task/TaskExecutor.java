package cs425.crane.task;

import cs425.Util;
import cs425.crane.JavaClassLoader;
import cs425.crane.message.AckMessage;
import cs425.crane.message.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Class that execute a given Spout/Bolt/Sink job
 * Anchored ack per tuple is finished. Since TA said that we can restart the entire job upon failure,
 * anchor is really not used.
 */
public class TaskExecutor {
    private final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

    private final Socket inputSocket;
    private final Socket outputSocket;
    private final Object taskObject;
    private ObjectOutputStream TupleOutputStream;
    private ObjectInputStream TupleInputStream;
    private ObjectOutputStream AckMessageOutputStream;
    private ObjectInputStream AckMessageInputStream;

    private final String taskType;
    private final String taskName;
    private int timeOutPerTuple;
    private final ExecutorService ackHandler;
    private final ScheduledExecutorService replayer;
    private ConcurrentHashMap<UUID,Tuple> waitForAck;

    public TaskExecutor(Socket in, Socket out, String taskType, String taskName, String object){
        this.inputSocket = in;
        this.outputSocket = out;
        logger.info(object);
        this.taskObject = (new JavaClassLoader()).createInstance("cs425.crane.applications." + object);
        this.taskType = taskType;
        this.taskName = taskName;
        this.waitForAck = new ConcurrentHashMap<>();
        ackHandler = Executors.newSingleThreadExecutor();
        replayer = Executors.newScheduledThreadPool(1);
    }

    private void initialAllThreads(){
        //initial the ack listener thread
        this.ackHandler.submit(()->{
            Thread.currentThread().setName(this.taskName + ":ackHandler");
            try{
                this.AckMessageInputStream = new ObjectInputStream(new BufferedInputStream(this.outputSocket.getInputStream()));
                if(!this.taskType.equals("sink")){
                    while(true){
                        if(this.waitForAck.isEmpty()) continue;
                        AckMessage ack = readAckMessageFromStream(this.AckMessageInputStream);
                        logger.info("Ack get for <{}>", ack.getId());
                        if(ack.isFinished()) this.waitForAck.remove(ack.getId());
                    }
                }
            } catch(IOException e){
                logger.error("Failed to read ack message", e);
            }
        });
        //initial the replay thread
        this.replayer.scheduleAtFixedRate(()->{
            Thread.currentThread().setName(this.taskName + ":replayer");
            if(!this.taskType.equals("sink") && !this.waitForAck.isEmpty()){
                try{
                    LocalDateTime now;
                    for(Tuple t: this.waitForAck.values()){
                        now = LocalDateTime.now();
                        if(Util.localDateTimeToSecond(now) - Util.localDateTimeToSecond(t.getTimestamp()) >= this.timeOutPerTuple){
                            logger.info(t.getData().get(0).toString() + "replayed");
                            sendTupleToStream(this.TupleOutputStream, t);
                        }
                    }
                }catch(IOException e){
                    logger.error("Failed to replay",e);
                }
            }
        }, this.timeOutPerTuple, this.timeOutPerTuple, TimeUnit.SECONDS);
    }

    public void prepare(){
        this.timeOutPerTuple = 2;
        if(taskType.equals("spout")){
            Spout spout = (Spout)this.taskObject;
            spout.open();
        } else if(taskType.equals("bolt")){
            Bolt bolt = (Bolt)this.taskObject;
            bolt.prepare();
        } else if(taskType.equals("sink")){
            Sink sink = (Sink)this.taskObject;
            sink.prepare();
        } else{
            logger.error("Unknown task type");
        }
        //initialAllThreads();
    }

    public void execute(){
        if(this.taskType.equals("spout")){
            Spout spout = (Spout)this.taskObject;
            Tuple outputTuple;
            try {
                this.TupleOutputStream = new ObjectOutputStream(new BufferedOutputStream(this.outputSocket.getOutputStream()));
                while((outputTuple = spout.nextTuple()) != null){
                    sendTupleToStream(this.TupleOutputStream, outputTuple);
                    if(outputTuple.getId() == null){
                        this.cleanUp();
                        logger.info("<{}> finished", this.taskName);
                        return;
                    }
                    //this.waitForAck.put(outputTuple.getId(), outputTuple);
                }
                logger.info("null Tuple");
            } catch(IOException e){
                logger.debug("<{}> failed to handle Tuple", this.taskName);
            }
        } else if(this.taskType.equals("bolt")){
            Bolt bolt = (Bolt)this.taskObject;
            Tuple inputTuple, outputTuple;
            try{
                this.TupleOutputStream = new ObjectOutputStream(new BufferedOutputStream(this.outputSocket.getOutputStream()));
                this.AckMessageOutputStream = new ObjectOutputStream(new BufferedOutputStream(this.inputSocket.getOutputStream()));
                this.TupleInputStream = new ObjectInputStream(new BufferedInputStream(this.inputSocket.getInputStream()));
                while(true){
                    inputTuple = readTupleFromStream(this.TupleInputStream);
                    bolt.process(inputTuple);
                    while((outputTuple = bolt.nextTuple()) != null){
                        sendTupleToStream(this.TupleOutputStream, outputTuple);
                        if(outputTuple.getId() == null){
                            this.cleanUp();
                            logger.info("<{}> finished", this.taskName);
                            return;
                        }
                        //this.waitForAck.put(outputTuple.getId(), outputTuple);
                    }
                }
            } catch(IOException e){
                logger.debug("<{}> failed to handle Tuple", this.taskName);
            }
        } else if(this.taskType.equals("sink")){
            Sink sink = (Sink)this.taskObject;
            Tuple inputTuple;
            try{
                this.AckMessageOutputStream = new ObjectOutputStream(new BufferedOutputStream(this.inputSocket.getOutputStream()));
                this.TupleInputStream = new ObjectInputStream(new BufferedInputStream(this.inputSocket.getInputStream()));
                while(true){
                    inputTuple = readTupleFromStream(this.TupleInputStream);
                    if(inputTuple.getId() == null){
                        this.cleanUp();
                        logger.info("<{}> finished", this.taskName);
                        return;
                    }
                    sink.process(inputTuple);

                }
            } catch(IOException e){
                logger.debug("<{}> failed to handle Tuple", this.taskName);
            }
        }
    }

    public void cleanUp(){
        try{
            if(taskType.equals("spout")){
                Spout spout = (Spout)this.taskObject;
                spout.close();
            } else if(taskType.equals("bolt")){
                Bolt bolt = (Bolt)this.taskObject;
                bolt.cleanUp();
            } else if(taskType.equals("sink")){
                Sink sink = (Sink)this.taskObject;
                sink.cleanUp();
            } else{
                logger.error("Unknown task type");
            }
            this.ackHandler.shutdown();
            this.replayer.shutdown();
            if(this.inputSocket != null){
                this.inputSocket.close();
            }
            if(this.outputSocket != null){
                this.outputSocket.close();
            }
            logger.info("Task executor finished");
        } catch(IOException e){
            logger.error("Failed to close the task executor sockets");
        }
    }


    /**
     * Read a Tuple from the given ObjectInputStream.
     * @param in ObjectInputStream to read a Tuple
     * @return Tuple that is read
     * @throws IOException
     */
    private Tuple readTupleFromStream(ObjectInputStream in) throws IOException{
        try{
            Tuple t = Tuple.parseFromStream(in);
            return t;
        } catch (ClassNotFoundException e){
            logger.error("Malformed Tuple", e);
            return null;
        }
    }

    /**
     * Read an AckMessage from the given ObjectInputStream.
     * @param in ObjectInputStream to read an AcKMessage
     * @return  AckMessage that is read
     * @throws IOException
     */
    private AckMessage readAckMessageFromStream(ObjectInputStream in) throws IOException{
        try{
            AckMessage ack = AckMessage.parseFromStream(in);
            return ack;
        } catch (ClassNotFoundException e){
            logger.error("Malformed Tuple", e);
            return null;
        }
    }

    /**
     * Send a Tuple to ObjectOutputStream
     * @param out ObjectOutputStream to send a Tuple
     * @param t Tuple to send
     * @throws IOException
     */
    private void sendTupleToStream(ObjectOutputStream out, Tuple t) throws IOException{
        out.writeObject(t);
        out.flush();
    }

    /**
     * send an AckMessage to socket.
     * @param out ObjectOutputStream to send an AckMessage
     * @param ack AckMessage to send
     * @throws IOException
     */
    private void sendAckMessageToStream(ObjectOutputStream out, AckMessage ack) throws IOException{
        out.writeObject(ack);
        out.flush();
    }


    public Socket getInputSocket() {
        return inputSocket;
    }

    public Socket getOutputSocket() {
        return outputSocket;
    }

}
