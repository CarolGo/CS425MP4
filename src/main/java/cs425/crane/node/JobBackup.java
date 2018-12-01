package cs425.crane.node;


import cs425.Config;
import cs425.mp3.FileOperation;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 * Class for master backup job information and new master read job information upon
 * the failure of the previous master
 */
public class JobBackup implements Serializable{
    private final int nextPortToAssign;
    private final HashMap<String, String> taskLocationMap;
    private final HashMap<String, String> taskTypeMap;

    public JobBackup(int nextPortToAssign,  HashMap<String, String> taskLocationMap, HashMap<String, String> taskTypeMap){
        this.nextPortToAssign = nextPortToAssign;
        this.taskLocationMap = taskLocationMap;
        this.taskTypeMap = taskTypeMap;
    }

    /**
     * Serialize and write the object instance to SDFS.
     * @param sdfsfilename SDFS filename to write
     * @param fnode FileOperation node to write to
     */
    public void writeToSDFS(String sdfsfilename, FileOperation fnode)throws IOException{
        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(sdfsfilename));
        out.writeObject(this);
        fnode.put(sdfsfilename, sdfsfilename);
        out.close();
        Files.deleteIfExists(Paths.get(sdfsfilename));
    }

    /**
     * Get and Deserialize the object instance on SDFS.
     * @param sdfsfilename
     * @param fnode FileOperation to read from
     * @return a JobBackup backup instance
     */
    public static JobBackup readFromSDFS(String sdfsfilename, FileOperation fnode) throws IOException, ClassNotFoundException{
        fnode.get(sdfsfilename,sdfsfilename);
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(Config.GET_PATH + "/" + sdfsfilename));
        Object o = in.readObject();
        in.close();
        if(o instanceof JobBackup) return (JobBackup)o;
        return null;
    }

    public int getNextPortToAssign() {
        return nextPortToAssign;
    }

    public HashMap<String, String> getTaskLocationMap() {
        return taskLocationMap;
    }

    public HashMap<String, String> getTaskTypeMap() {
        return taskTypeMap;
    }
}
