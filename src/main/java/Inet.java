import java.net.InetAddress;
import java.net.UnknownHostException;

public class Inet {

    public static void main(String[] args) {
        try {
            System.out.println(InetAddress.getLocalHost().getCanonicalHostName());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
