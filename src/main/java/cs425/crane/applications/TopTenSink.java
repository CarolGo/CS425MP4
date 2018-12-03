package cs425.crane.applications;

import cs425.crane.message.Tuple;
import cs425.crane.task.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;


public class TopTenSink implements Sink {

    private HashMap<String, Integer> wordFrequeny;
    private HashMap<String, Integer> topTen;
    private final Logger logger = LoggerFactory.getLogger(TopTenSink.class);
    private int i;

    @Override
    public void prepare(){
        this.wordFrequeny = new HashMap<>();
        this.topTen = new HashMap<>();
    }

    @Override
    public void process(Tuple tuple){
        i ++;
        Integer freq = this.wordFrequeny.getOrDefault(tuple.getData().get(0).toString(), 0);
        this.wordFrequeny.put(tuple.getData().get(0).toString(),freq+1);
        if (i % 50000 == 0){
            logger.info("Sink result after <{}> Tuples received", Integer.toString(i));
            printTopTen();
        }
    }

    @Override
    public void cleanUp(){
        try{
            printTopTen();
            logger.info("Save result to local machine");
            BufferedWriter bw = new BufferedWriter(new FileWriter("topTenProductId.result"));
            this.topTen.forEach((k,v)->{
                try{
                    bw.write(k + " : " + v.toString() + "\n");
                } catch(IOException e){
                    logger.error("Failed to write line",e);
                }
            });
            bw.flush();
            bw.close();
        } catch(IOException e){
            logger.error("Failed to write the result",e);
        }
    }


    private void printTopTen(){
        this.topTen.clear();
        Set<Entry<String, Integer>> set = this.wordFrequeny.entrySet();
        List<Entry<String, Integer>> list = new ArrayList(set);
        Collections.sort(list, new Comparator<Entry<String, Integer>>() {
            @Override
            public int compare(Entry<String, Integer> a,
                               Entry<String, Integer> b) {
                return b.getValue() - a.getValue();
            }
        });

        // Output the top 10 numbers
        for (int i = 0; i < 10 && i < list.size(); i++) {
            topTen.put(list.get(i).getKey(), list.get(i).getValue());
            System.out.println(list.get(i));
        }
    }

    @Override
    public void ack(UUID id){}

    @Override
    public void fail(UUID id){}
}