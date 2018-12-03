package ApplicationTest;

import cs425.crane.applications.ReadLineAsinSpout;
import cs425.crane.applications.TopTenSink;
import cs425.crane.message.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class topTenProductIdTest {
    @Test
    void AllTogetherTest(){
        ReadLineAsinSpout RLS = new ReadLineAsinSpout();
        TopTenSink CS = new TopTenSink();
        RLS.open();
        CS.prepare();
        Tuple t;
        while((t = RLS.nextTuple()) != null){
            if(t.getId() == null){
                break;
            } else{
                CS.process(t);
            }
        }
        RLS.close();
        CS.cleanUp();
    }
}
