import cs425.Config;
import cs425.mp3.FileCommand;
import cs425.mp3.FileOperation;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class basicTest {

    @Test
    @Disabled
    void parseTest() throws Exception {
        FileOperation f = new FileOperation(null);
        Socket s = new Socket("127.0.0.1", Config.TCP_PORT);
        ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(s.getOutputStream()));
        out.writeObject(genFake());
        out.flush();
        out.close();
        s.close();
        System.err.println("Finished");
        f.stopServer();
    }

    static FileCommand genFake() {
        FileCommand fc = new FileCommand("query", "b", "asda/12", 1);
        return fc;
    }

}
