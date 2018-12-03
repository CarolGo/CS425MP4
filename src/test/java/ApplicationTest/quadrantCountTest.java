package ApplicationTest;

import cs425.crane.applications.QuadrantCountSink;
import cs425.crane.applications.ReadLineAsinSpout;
import cs425.crane.applications.ReadLineXYSpout;
import cs425.crane.applications.TopTenSink;
import cs425.crane.message.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class quadrantCountTest {
    @Test
    @Disabled
    void AllTogetherTest(){
        ReadLineXYSpout RLS = new ReadLineXYSpout();
        QuadrantCountSink CS = new QuadrantCountSink();
        RLS.open();
        CS.prepare();
        Tuple t;
        while((t = RLS.nextTuple()) != null){
            if(t.getId() == null){
                break;
            }
            CS.process(t);
        }
        RLS.close();
        CS.cleanUp();
    }
}
