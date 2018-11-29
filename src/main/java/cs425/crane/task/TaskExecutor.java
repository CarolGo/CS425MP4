package cs425.crane.task;

import com.sun.corba.se.pept.encoding.InputObject;
import cs425.crane.message.AckMessage;
import cs425.crane.message.Tuple;
import cs425.crane.node.CraneNode;
import cs425.crane.task.Bolt;
import cs425.crane.task.Spout;
import cs425.crane.task.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Class that execute a given Spout/Bolt/Sink job
 */
public class TaskExecutor {
    private final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

    private Socket inputSocket;
    private Socket outputSocket;
    private final String taskType;
    private final String taskName;
    private int timeOutPerTuple;

    public TaskExecutor(Socket in, Socket out, String taskType, String taskName){
        this.inputSocket = in;
        this.outputSocket = out;
        this.taskType = taskType;
        this.taskName = taskName;
    }

    public void prepare(){

    }
    public void execute(){
        ObjectOutputStream outputStream;
        ObjectInputStream inputStream;
        if(this.taskType.equals("spout")){
            Tuple t;
            try {
                outputStream = new ObjectOutputStream(new BufferedOutputStream(this.outputSocket.getOutputStream()));
                for (int i = 0; i < 10; i ++){
                    t = new Tuple(UUID.randomUUID(), new Integer(i));
                    this.sendTupleToStream(outputStream, t);
                    logger.info("Tuple send out");
                    logger.info(t.getId().toString());
                }
            } catch(IOException e){
                logger.error("<{}> failed to handle Tuple", this.taskName, e);
            }
        } else if(this.taskType.equals("bolt")){
            Tuple in, out;
            try{
                outputStream = new ObjectOutputStream(new BufferedOutputStream(this.outputSocket.getOutputStream()));
                inputStream = new ObjectInputStream(new BufferedInputStream(this.inputSocket.getInputStream()));
                while(true){
                    logger.info("blocked here");
                    in = this.readTupleFromStream(inputStream);
                    logger.info("Tuple get");
                    logger.info(in.getId().toString());
                    ArrayList<Object> data = in.getData();
                    logger.info("Input data get");
                    out = new Tuple(in.getId(), data.get(0), new String("aaaaa"));
                    logger.info("Output Tuple created");
                    this.sendTupleToStream(outputStream, out);
                    logger.info("Tuple send out");
                }
            } catch(IOException e){
                logger.error("<{}> failed to handle Tuple", this.taskName, e);
            }
        } else if(this.taskType.equals("sink")){
            Tuple in;
            try{
                inputStream = new ObjectInputStream(new BufferedInputStream(this.inputSocket.getInputStream()));
                while(true){
                    in = this.readTupleFromStream(inputStream);
                    logger.info("Tuple get");
                    logger.info(in.getId().toString());
                    ArrayList<Object> data = in.getData();
                    logger.info(Integer.toString(data.size()));
                    logger.info(data.get(0).toString());
                    logger.info(data.get(1).toString());
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
