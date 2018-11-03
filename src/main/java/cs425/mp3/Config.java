package cs425.mp3;

/**
 * Some configuration
 */
public final class Config {
    private Config() {
    }

    /**
     * UDP port used to maintain cluster
     */
    public static final int UDP_PORT = 18080;

    /**
     * TCP port for file operations
     */
    public static final int TCP_PORT = 18081;

    public static final int TCP_FILE_TRANS_PORT = 18082;

    public static final int NETWORK_BUFFER_SIZE = 1350;

    public static final long JOIN_PERIOD = 2000;
    public static final int GOSSIP_ROUND = 4; //need gossip 4 rounds to achieve the infection of majority
    public static final int ELECTION_PERIOD = 200;


    public static final String STORAGE_PATH = "mp3_fs_data";
    public static final String GET_PATH = "mp3_fs_get_data";
    public static final String DEFAULT_MASTER_HOSTNAME = "fa18-cs425-g17-01.cs.illinois.edu";
    public static final int NUM_CORES = Runtime.getRuntime().availableProcessors();

    public static final int CONNECT_TIMEOUT_SECOND = 60;
    public static final int RW_TIMEOUT_SECOND = 300;
}
