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
    public static final int UDP_PORT = 8080;

    /**
     * TCP port for file operations
     */
    public static final int TCP_PORT = 8081;

    public static final int FILE_BUFFER_SIZE = 8192;

    public static final long JOIN_PERIOD = 2000;
    public static final int GOSSIP_ROUND = 4; //need gossip 4 rounds to achieve the infection of majority
    public static final int ELECTION_PERIOD = 200;


}
