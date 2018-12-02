package cs425.crane.applications.wordCount;

import cs425.crane.task.Spout;
import cs425.crane.message.Tuple;

import org.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;

public class ReadLineSpout implements Spout {

    private BufferedReader br;
    private final Logger logger = LoggerFactory.getLogger(ReadLineSpout.class);
    private int i;

    @Override
    public void open() {
        try{
            br = new BufferedReader(new FileReader("Toys_and_Games_5.json"));
            i = 0;
        } catch (FileNotFoundException e){
            logger.error("ReadLineSpout failed to open the file", e);
        }
    }

    @Override
    public Tuple nextTuple(){
        try{
            String line;
            if((line = br.readLine()) == null){
                logger.info("EOF reached");
                return new Tuple(null, "");
            }
            Tuple t = null;
            JSONObject jsonObject = new JSONObject(line);
            if(jsonObject.has("reviewText")){
                t = new Tuple(UUID.randomUUID(), jsonObject.getString("reviewText"));
            }
            if(i % 1000 == 0){
                logger.info(Integer.toString(i));
            }
            i ++;
            return t;
        } catch(IOException e){
            logger.error("ReadLineSpout failed to generate next Tuple", e);
            return null;
        }
    }

    @Override
    public void close(){
        try{
            logger.info("Spout is trying to close");
            br.close();
        } catch (IOException e){
            logger.error("ReadLineSpout failed to close", e);
        }
    }

    @Override
    public void ack(UUID id){

    }

    @Override
    public void fail(UUID id){

    }

}
