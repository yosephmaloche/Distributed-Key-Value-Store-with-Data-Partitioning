/**
 * Created by jo on 24/07/17.
 * The purpose of this class is t0 handle all the jobs that ClientApp must do
 */
import java.io.Serializable;
import akka.actor.*;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import java.util.concurrent.TimeUnit;
import scala.concurrent.Await;
import scala.concurrent.Future;

public class ClientApp {
    static private String remotePath = null; // Akka path of the bootstrapping peer
    static private String ip = null;
    static private String port = null;
    static private String value = null;
    static private String key = null;

    public static class client extends UntypedActor {        //Created to implement it's 
        public void onReceive(Object message) {

        }

    }
    //to validate Ip address

    public static boolean validateIPAddress(String ipAddress) {
        String[] input = ipAddress.split("\\.");
        if (input.length != 4) {
            return false;
        }
        for (String str: input) {
            int i = Integer.parseInt(str);
            if ((i < 0) || (i > 255)) {
                return false;
            }
        }
        return true;
    }

    //According to RFC 793, the port is a 16 bit unsigned int.
    //This means the range is 0 - 65535( 2^16 - 1)
    // ports under 1024 are reserved for system services http, ftp, etc.

    public static boolean ValidportNum(String PortNum) {
        try {
            //Port number validation (16bit data)
            if ((Integer.parseInt(PortNum) < 0) || (Integer.parseInt(PortNum) > 65535)) {
                return false;
            }
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }
    public static void main(String[] args) throws Exception {
                if (args.length < 3 || args.length > 5) {
            System.out.println("Invalid user input length : Please,use the the input with length 3-5 ");
            return;
        }
        System.out.println("Welcome to user screen");
        if (!validateIPAddress(args[0])) {
            System.out.println("The Ip address is invalid");
            return;
        }
        if (!ValidportNum(args[1])) {
            System.out.println("The port number is invalid");
            return;
        }
        Config config = ConfigFactory.load("application");
        int myId = config.getInt("nodeapp.id");
        System.out.println("start loading configuration");
        ip = args[0];
        port = args[1];

        if (args.length == 3 && args[2].equals("leave")) { //leave request
            remotePath = "akka.tcp://mysystem@" + ip + ":" + port + "/user/node";
            final ActorSystem system = ActorSystem.create("mysystem", config);

            // Creating single user to communicate with NodeApp
            ActorSelection myTargetNode = system.actorSelection(remotePath);

            // we wait for a action complication
            Timeout timeout = new Timeout(100, TimeUnit.SECONDS);

            //Using Future’ will send a message to the receiving Actor asynchronously and will immediately return a ‘Future’holding the eventual reply message
            // send the real request
            Future < Object > request = Patterns.ask(myTargetNode, new NodeApp.Leave(myId), timeout);
            System.out.println("The requested node" + ":"+ remotePath + " is Successfully terminated ");
            Await.result(request, timeout.duration());

        } else if (args.length == 4 && args[2].equals("read")) { //read request
            key = args[3];
            // The Akka path to the bootstrapping peer
            remotePath = "akka.tcp://mysystem@" + ip + ":" + port + "/user/node";

            // convert the key from string to int
            int key1 = Integer.parseInt(key);
            // ask to the client
            final ActorSystem system = ActorSystem.create("mysystem", config);
            //creating the node actor
            final ActorRef client_node = system.actorOf(Props.create(client.class), "ClientNode");

            // Creating single user to communicate with NodeApp
            ActorSelection myTargetNode = system.actorSelection(remotePath);

            // we wait for a reply
            Timeout timeout = new Timeout(100, TimeUnit.SECONDS);

            Future < Object > request = Patterns.ask(myTargetNode, new NodeApp.ClientReadRequest(key1), timeout);
            Object reply = Await.result(request, timeout.duration());
            NodeApp.ClientReadAnswer answer = (NodeApp.ClientReadAnswer) reply;
            // to print the read value sent from the node by the read request key
            System.out.println("The read value  is  " + (answer).getValue());

        } else if (args.length == 5 && args[2].equals("write")) { //write request
            // System.out.println("write ");
            key = args[3];
            value = args[4];
            remotePath = "akka.tcp://mysystem@" + ip + ":" + port + "/user/node";
            //Converting from string to int
            int key2 = Integer.parseInt(key);
            // Create the actor system
            ActorSystem system = ActorSystem.create("mysystem", config);
            // Creating single user to communicate with NodeAp
            ActorSelection myTargetNode = system.actorSelection(remotePath);
            Timeout timeout = new Timeout(100, TimeUnit.SECONDS); // it will stop by itself after the time duration mentioned
            //Using Future’ will send a message to the receiving Actor asynchronously and will immediately return a ‘Future’.
            Future < Object > request = Patterns.ask(myTargetNode, new NodeApp.ClientWriteRequest(key2, value), timeout);
            // we wait for a write reply

            Object reply = Await.result(request, timeout.duration());
            NodeApp.ClientWriteAnswer answer = (NodeApp.ClientWriteAnswer) reply;
            System.out.println("The data "+answer+" "+key2 +":"+value+ " " + "is written successfully on requested node"+" " +remotePath);
           
        } else
            System.out.println("Wrong number of arguments: [remote_ip remote_port 'read' or 'write' or 'leave' or key value]");

    }
}