package ApplicationTest;


import cs425.crane.applications.wordCount.CountSink;
import cs425.crane.applications.wordCount.ReadLineSpout;
import cs425.crane.applications.wordCount.SplitFilterBolt;
import cs425.crane.message.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class wordCountTest {
    @Test
    @Disabled
    void ReadLineSpoutTest(){
        ReadLineSpout RLS = new ReadLineSpout();
        RLS.open();
        Tuple t;
        while((t = RLS.nextTuple()) != null){
            System.out.println(t.getData().get(0));
        }
    }
    @Test
    void AllTogetherTest(){
        ReadLineSpout RLS = new ReadLineSpout();
        SplitFilterBolt SB = new SplitFilterBolt();
        CountSink CS = new CountSink();
        RLS.open();
        SB.prepare();
        CS.prepare();
        Tuple t;
        while((t = RLS.nextTuple()) != null){
            if(t.getId() == null){
                SB.process(t);
                while((t = SB.nextTuple()) != null){
                    CS.process(t);
                }
                break;
            }
            SB.process(t);
            while((t = SB.nextTuple()) != null){
                CS.process(t);
            }
        }
        RLS.close();
        SB.cleanUp();
        CS.cleanUp();
    }
}
