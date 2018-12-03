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

public class ReadLineAsinSpout implements Spout {

    private BufferedReader br;
    private final Logger logger = LoggerFactory.getLogger(ReadLineAsinSpout.class);
    private final int numOfLines= 200000;
    private int i;

    @Override
    public void open() {
        try{
            br = new BufferedReader(new FileReader("Toys_and_Games_5.json"));
            i = 0;
        } catch (FileNotFoundException e){
            logger.error("ReadLineAsinSpout failed to open the file", e);
        }
    }

    @Override
    public Tuple nextTuple(){
        try{
            //Util.noExceptionSleep(1);
            String line;
            if((line = br.readLine()) == null || i == numOfLines){
                logger.info("Read File finished");
                return new Tuple(null, "");
            }
            Tuple t = null;
            JSONObject jsonObject = new JSONObject(line);
            if(jsonObject.has("asin")){
                t = new Tuple(UUID.randomUUID(), jsonObject.getString("asin"));
            }
            if(i % 10000 == 0){
                logger.info("Spout has generate <{}> Tuples", Integer.toString(i));
            }
            i ++;
            return t;
        } catch(IOException e){
            logger.error("ReadLineAsinSpout failed to generate next Tuple", e);
            return null;
        }
    }

    @Override
    public void close(){
        try{
            logger.info("Spout is trying to close");
            br.close();
        } catch (IOException e){
            logger.error("ReadLineAsinSpout failed to close", e);
        }
    }

    @Override
    public void ack(UUID id){

    }

    @Override
    public void fail(UUID id){

    }

}
