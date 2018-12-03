package cs425.crane.applications;

import cs425.crane.message.Tuple;
import cs425.crane.task.Spout;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;

public class ReadLineXYSpout implements Spout {

    private BufferedReader br;
    private final Logger logger = LoggerFactory.getLogger(ReadLineSpout.class);
    private final int numOfLines= 1000000;
    private int i;

    @Override
    public void open() {
        try{
            br = new BufferedReader(new FileReader("RandomQ.txt"));
            i = 0;
        } catch (FileNotFoundException e){
            logger.error("ReadLineAsinSpout failed to open the file", e);
        }
    }

    @Override
    public Tuple nextTuple(){
        try{
            i ++;
            //Util.noExceptionSleep(1);
            String line;
            if((line = br.readLine()) == null || i == numOfLines){
                logger.info("Read File finished");
                return new Tuple(null, "");
            }
            Tuple t = null;
            Integer x =Integer.parseInt(line.split(" ")[0]);
            Integer y =Integer.parseInt(line.split(" ")[1]);
            if(x > 0 && y > 0){
                t = new Tuple(UUID.randomUUID(), "First Quadrant");
            } else if(x < 0 && y > 0){
                t = new Tuple(UUID.randomUUID(), "Second Quadrant");
            } else if(x < 0 && y < 0){
                t = new Tuple(UUID.randomUUID(), "Third Quadrant");
            } else if(x>0 && y<0){
                t = new Tuple(UUID.randomUUID(),"Forth Quadrant");
            } else if (x==0 && y!=0) {
                t = new Tuple(UUID.randomUUID(), "Y");
            }else if(x!=0 && y==0) {
                t = new Tuple(UUID.randomUUID(), "X");
            }
            else{
                t = new Tuple(UUID.randomUUID(), "Origin");
            }
            if(i % 10000 == 0){
                logger.info(Integer.toString(i));
            }
            return t;
        } catch(IOException e){
            logger.error("ReadLineXYSpout failed to generate next Tuple", e);
            return null;
        }
    }

    @Override
    public void close(){
        try{
            logger.info("Spout is trying to close");
            br.close();
        } catch (IOException e){
            logger.error("ReadLineXYSpout failed to close", e);
        }
    }

    @Override
    public void ack(UUID id){

    }

    @Override
    public void fail(UUID id){

    }



}
