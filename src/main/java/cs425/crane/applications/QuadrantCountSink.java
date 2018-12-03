package cs425.crane.applications;

import cs425.crane.message.Tuple;
import cs425.crane.task.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.UUID;

public class QuadrantCountSink implements Sink {

    private HashMap<String, Integer> wordFrequeny;
    private final Logger logger = LoggerFactory.getLogger(QuadrantCountSink.class);
    private int i;

    @Override
    public void prepare(){
        this.wordFrequeny = new HashMap<>();
        this.wordFrequeny.put("First Quadrant", 0);
        this.wordFrequeny.put("Second Quadrant", 0);
        this.wordFrequeny.put("Third Quadrant", 0);
        this.wordFrequeny.put("Forth Quadrant", 0);
        this.wordFrequeny.put("Y", 0);
        this.wordFrequeny.put("X", 0);
        this.wordFrequeny.put("Origin", 0);
    }

    @Override
    public void process(Tuple tuple){
        i ++;
        this.wordFrequeny.put(tuple.getData().get(0).toString(),this.wordFrequeny.get(tuple.getData().get(0).toString()) + 1);
        if (i % 50000 == 0){
            logger.info("Sink result after <{}> Tuples received", Integer.toString(i));
            this.wordFrequeny.forEach((word, num) -> {
                String line = word + " : " + num.toString();
                logger.info(line);
            });
        }
    }

    @Override
    public void cleanUp(){
        try{
            logger.info("Save result to local machine");
            BufferedWriter bw = new BufferedWriter(new FileWriter("quadrantCount.result"));
            this.wordFrequeny.forEach((word, freq) -> {
                String line = word + " : " + freq.toString() + "\n";
                try{
                    bw.write(line);
                } catch(IOException e){
                    logger.error("Failed to write line to the result file", e);
                }
            });
            bw.flush();
            bw.close();
        } catch(IOException e){
            logger.error("Failed to write the result",e);
        }
    }

    @Override
    public void ack(UUID id){}

    @Override
    public void fail(UUID id){}

}
