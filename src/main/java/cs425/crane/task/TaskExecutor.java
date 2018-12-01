package cs425.crane.task;

import cs425.Util;
import cs425.crane.message.AckMessage;
import cs425.crane.message.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Class that execute a given Spout/Bolt/Sink job
 * Anchored ack per tuple is finished. Since TA said that we can restart the entire job upon failure,
 * anchor is really not used.
 */
public class TaskExecutor {
    private final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

    private Socket inputSocket;
    private Socket outputSocket;
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

    public TaskExecutor(Socket in, Socket out, String taskType, String taskName){
        this.inputSocket = in;
        this.outputSocket = out;
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
        initialAllThreads();
    }

    public void execute(){

        if(this.taskType.equals("spout")){
            Tuple out;
            try {
                this.TupleOutputStream = new ObjectOutputStream(new BufferedOutputStream(this.outputSocket.getOutputStream()));
                for (int i = 0; i < 10; i ++){
                    out = new Tuple(UUID.randomUUID(), new Integer(i));
                    this.sendTupleToStream(this.TupleOutputStream, out);
                    this.waitForAck.put(out.getId(), out);
                    logger.info("Tuple send out");
                    logger.info(out.getId().toString());
                }
            } catch(IOException e){
                logger.error("<{}> failed to handle Tuple", this.taskName, e);
            }
        } else if(this.taskType.equals("bolt")){
            Tuple in, out;
            int count = 0;
            try{
                this.TupleOutputStream = new ObjectOutputStream(new BufferedOutputStream(this.outputSocket.getOutputStream()));
                this.AckMessageOutputStream = new ObjectOutputStream(new BufferedOutputStream(this.inputSocket.getOutputStream()));
                this.TupleInputStream = new ObjectInputStream(new BufferedInputStream(this.inputSocket.getInputStream()));
                while(true){
                    in = this.readTupleFromStream(this.TupleInputStream);
                    count ++;
                    logger.info("Tuple get");
                    logger.info(in.getId().toString());
                    ArrayList<Object> data = in.getData();
                    out = new Tuple(in.getId(), data.get(0), new String("aaaaa"));
                    if(in.getData().get(0).equals(2) && count < 15) continue;
                    logger.info("Output Tuple created");
                    this.sendTupleToStream(this.TupleOutputStream, out);
                    this.waitForAck.put(out.getId(), out);
                    logger.info("Tuple send out");
                    //finish process the tuple send ackBack
                    AckMessage ack = new AckMessage(in.getId(), true);
                    sendAckMessageToStream(this.AckMessageOutputStream,ack);
                    logger.info("ack out");
                }
            } catch(IOException e){
                logger.error("<{}> failed to handle Tuple", this.taskName, e);
            }
        } else if(this.taskType.equals("sink")){
            Tuple in;
            try{
                this.AckMessageOutputStream = new ObjectOutputStream(new BufferedOutputStream(this.inputSocket.getOutputStream()));
                this.TupleInputStream = new ObjectInputStream(new BufferedInputStream(this.inputSocket.getInputStream()));
                while(true){
                    in = this.readTupleFromStream(this.TupleInputStream);
                    logger.info("Tuple get");
                    logger.info(in.getId().toString());
                    ArrayList<Object> data = in.getData();
                    logger.info(Integer.toString(data.size()));
                    logger.info(data.get(0).toString());
                    logger.info(data.get(1).toString());
                    //finish process the tuple send ackBack
                    AckMessage ack = new AckMessage(in.getId(), true);
                    sendAckMessageToStream(this.AckMessageOutputStream,ack);
                    logger.info("ack out");
                }
            } catch(IOException e){
                logger.error("<{}> failed to handle Tuple", this.taskName, e);
            }
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

    public void setInputSocket(Socket inputSocket) {
        this.inputSocket = inputSocket;
    }

    public Socket getOutputSocket() {
        return outputSocket;
    }

    public void setOutputSocket(Socket outputSocket) {
        this.outputSocket = outputSocket;
    }
}
