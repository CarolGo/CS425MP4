package cs425.mp3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Scanner;


public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String... args) throws Exception {
        logger.info("Started at {}...", LocalDateTime.now());

        Scanner input = new Scanner(System.in);
        String cmd;
        Node node = new Node();
        FileOperation fOper = new FileOperation(node);
        while (true) {
            logger.info("Enter your command (id,list,join,leave,printLeader,put,get,delete,ls,store,get-versions): ");
            cmd = input.nextLine();
            logger.trace("User input: {}", cmd);
            switch (cmd) {
                case "id":
                    node.printId();
                    break;
                case "list":
                    node.printList();
                    break;
                case "join":
                    node.join();
                    break;
                case "leave":
                    node.leave();
                    break;
                case "printLeader":
                    node.printLeader();
                    break;
                default:
                    String[] arguments = cmd.split(" ");
                    switch (arguments[0]) {
                        case "put": //put localfilename sdfsfilename
                            fOper.put(arguments[1], arguments[2]);
                            break;
                        case "get": //get sdfsfilename localfilename
                            fOper.get(arguments[1], arguments[2]);
                            break;
                        case "delete":  //delete sdfsfilename
                            fOper.delete(arguments[1]);
                            break;
                        case "ls":  //ls sdfsfilename
                            fOper.listFileLocations(arguments[1]);
                            break;
                        case "store":
                            fOper.listFileLocal();
                            break;
                        case "get-versions":    //get-versions sdfsfilename numversions localfilename
                            fOper.getVersions(arguments[1], arguments[2], arguments[3]);
                            break;
                        default:
                            logger.warn("Use input invalid");
                            break;
                    }
                    break;
            }
        }
    }
}
