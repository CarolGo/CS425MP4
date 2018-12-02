package cs425.crane.applications.wordCount;

import cs425.crane.message.Tuple;
import cs425.crane.task.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SplitBolt implements Bolt {


    private List<Tuple> TupleToSend;
    private final Logger logger = LoggerFactory.getLogger(SplitBolt.class);
    private Set<String> wordsWeWant;
    private int i;

    @Override
    public void prepare(){
        TupleToSend = new ArrayList<>();
        wordsWeWant = new HashSet<>();
        wordsWeWant.add("i");
        wordsWeWant.add("you");
        wordsWeWant.add("he");
        wordsWeWant.add("she");
        wordsWeWant.add("is");
        wordsWeWant.add("are");
        wordsWeWant.add("toy");
        i = 0;
    }


    @Override
    public void process(Tuple tuple){
        if(tuple.getId() == null){
            this.TupleToSend.add(tuple);
        }
        String review = tuple.getData().get(0).toString();
        String [] words = review.split(" ");
        for(String word: words){
            if(wordsWeWant.contains(word.toLowerCase())){
                Tuple t = new Tuple(UUID.randomUUID(), word);
                this.TupleToSend.add(t);
            }
        }
    }

    @Override
    public Tuple nextTuple(){
        if(this.TupleToSend.size() > 0){
            Tuple t = this.TupleToSend.get(0);
            this.TupleToSend.remove(0);
            if(i % 10000 == 0){
                logger.info(Integer.toString(i));
            }
            i ++;
            return t;
        } else{
            return null;
        }
    }

    @Override
    public void cleanUp(){
        TupleToSend.clear();
    }

}
