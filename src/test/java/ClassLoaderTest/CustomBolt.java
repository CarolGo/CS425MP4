package ClassLoaderTest;

import cs425.crane.message.Tuple;
import cs425.crane.task.Bolt;


public class CustomBolt implements Bolt{

    @Override
    public void prepare(){
        //Whatever user input
        System.out.println("In prepare now");
    }

    @Override
    public void execute(Tuple t){
        //Whatever user input
        System.out.println(t.getId());
        System.out.println("In execute now");
    }

    @Override
    public void cleanUp(){
        //Whatever user input
        System.out.println("In cleanUp now");
    }
}
