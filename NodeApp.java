/**
 * Created by jo on 23/07/17.
 */
/************************************************************************
 *Distributed Systems1 Course Project                                   *
 * Yoseph M.Maloche and Selamawit Y.Gebremeskel                         *
 * Distributed Key-Value Store with Data Partitionin                    *
 ************************************************************************
 */
import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import java.io.*;

//The Node class we do the tasks of "Join  for node" and
//For client Read,write,join,leave
public class NodeApp {
    static private String remotePath = null; // Akka path of the bootstrapping peer
    static private int myId; // ID of the local node
    public static String nodmsg = null;
    public static class Join implements Serializable {
        int id;
        public Join(int id) {
            this.id = id;
        }
    }
    //Leave request class
    public static class Leave implements Serializable {
        private int id;
        public int getId() {
            return id;
        }

        public Leave(int i) {
            this.id = i;
        }
    }
    //Leave answer class will notify the nodes before leave
    // and they will remove the left one then print the remaining list later
    public static class LeaveAnswer implements Serializable {
        public int id;
        public LeaveAnswer(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }
    public static class RequestNodelist implements Serializable {}
    public static class Nodelist implements Serializable {
        Map < Integer, ActorRef > nodes;                        //A Map is an object that maps keys to values.
        public Nodelist(Map < Integer, ActorRef > nodes) {
            this.nodes = Collections.unmodifiableMap(new HashMap < > (nodes));
        }
    }
    // client read request  -> a random node
    static class ClientReadRequest implements Serializable {
        private int key;

        int getKey() {
            return key;
        }

        ClientReadRequest(int ke) {
            this.key = ke;
        }
    }
    //Read answer to client
    public static class ClientReadAnswer implements Serializable {
        private int key;
        private String value;

        public ClientReadAnswer(int key, String value) {
            this.key = key;
            this.value = value;
        }
        public ClientReadAnswer(String value) {
            this.value = value;
        }

        public int getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }
    }
    //client write request  -> a random node
    public static class ClientWriteRequest implements Serializable {
        private int key;
        private String value;

        public int getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public ClientWriteRequest(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }
    //client write answer Serializable class
    public static class ClientWriteAnswer implements Serializable {
        private int key;
        private String value;
        public String confirm;

        //write answer constructor
        public ClientWriteAnswer(int ke, String val, String con) {
            this.key = ke;
            this.value = val;
            this.confirm = con;
        }

    }

    public static class Node extends UntypedActor {
        // The table of all nodes in the system id->ref
        private Map < Integer, ActorRef > nodes = new HashMap < > ();         //provide key-value access to data
        private Map < Integer, String > Data = new HashMap < > ();			  //HashMap is part of the new Collections Framework
        public int key1;
        public String value;

        public void preStart() {
            if (remotePath != null) {
                getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
            }
            nodes.put(myId, getSelf());
        }

        //all the recieving messages will be manipulated here
        public void onReceive(Object message) {
            if (message instanceof RequestNodelist) {
                getSender().tell(new Nodelist(nodes), getSelf());
            } else if (message instanceof Nodelist) {
                nodes.putAll(((Nodelist) message).nodes);
                for (ActorRef n: nodes.values()) {
                    n.tell(new Join(myId), getSelf());
                }
            }
            //Node join operation
            else if (message instanceof Join) {
                int id = ((Join) message).id;
                System.out.println("Node " + id + " joined");
                nodes.put(id, getSender());
            }
            // read request operation from the client
            else if (message instanceof ClientReadRequest) {

                int key_search = ((ClientReadRequest) message).getKey();
                for (Map.Entry < Integer, String > entry: Data.entrySet()) {
                    if (entry.getKey() == key_search) {

                        ClientReadAnswer reply = new ClientReadAnswer(entry.getValue());
                        getSender().tell(reply, getSelf());                                //the reply will be sent to client
                        System.out.println("The key to read is " + key_search);
                    }
                }

            }
            //write operation from client
            else if (message instanceof ClientWriteRequest) {
                key1 = ((ClientWriteRequest) message).getKey();
                value = ((ClientWriteRequest) message).getValue();
                // Timer is need here
                Data.put(((ClientWriteRequest) message).key, ((ClientWriteRequest) message).value);

                try {
                    File file = new File("./Data.txt");
                    if (!file.exists()) {
                        file.createNewFile();
                    }
                    FileWriter fW= new FileWriter(file, true);   //true used to append to eliminate delition after first write
                    fW.write(((ClientWriteRequest) message).getKey() + " " + ((ClientWriteRequest) message).getValue() + "\r\n");  //\r back to the beginning of the line ....n\ means "move down a line
                    System.out.println("Success: successfully written to local storage");
                    fW.flush();          //when all the temporary memory location are full then we use flush() which flushes all the streams of data and executes them completely and gives a new space to new streams in buffer temporary location
                    fW.close();			//	
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Error: data is not written!");
                }
                System.out.println(" The written data is : " + ((ClientWriteRequest) message).key + " : " + ((ClientWriteRequest) message).value);

            } 

            else if (message instanceof ClientWriteAnswer) {
                Data.put(((ClientWriteAnswer) message).key, ((ClientWriteAnswer) message).value);
                System.out.println(" The written data is : " + ((ClientWriteAnswer) message).key + " : " + ((ClientWriteAnswer) message).value);
                // send the data to write it to the hash map
                //This code will save the data to the log
                getSender().tell(new ClientWriteAnswer(key1, value, "confirmed"), getSelf());

            }
            // Nodes leaves the network and will be removed from the list
            else if (message instanceof Leave) {
                System.out.println("The Node which is requested to leave is" + myId);
                for (ActorRef n: nodes.values()) { 
                    n.tell(new LeaveAnswer(myId), null);
                }
                nodes.remove(myId);
                System.out.println("The requesting node to leave is terminating");
             	//Terminate the node which is left
                getContext().system().terminate();

            } else if (message instanceof LeaveAnswer) {
                nodes.remove(((LeaveAnswer) message).id);
                //all the nodes will update the list and will print during leave
                for (Map.Entry < Integer, ActorRef > entry: nodes.entrySet()) {

                    System.out.println("The new node list have " + entry.getValue());
                }
            } else
                unhandled(message); // this actor does not handle any incoming messages
        }
    }

    public static void main(String[] args) {
        // If we leave empty, it will start bootstrapping node otherwise We
        if (args.length != 0 && args.length != 3) {
            System.out.println("Please use the argument: [remote_ip remote_port] which has length of 0 or 3");
            return;
        }

        // Load the "application.conf"
        Config config = ConfigFactory.load("application");
        myId = config.getInt("nodeapp.id");
        if (args.length == 3) {
            if (args[0].equals("join")) {
                nodmsg = "join";
                String ip = args[1];
                String port = args[2];
                // The Akka path to the bootstrapping peer
                remotePath = "akka.tcp://mysystem@" + ip + ":" + port + "/user/node";
                System.out.println("The Joining Node is  " + ip + ":" + port);
            } else System.out.println("Invalid data: Please check your input data [spelling,ip or port]");
        } else
            //myId is the id from configuration file for the specific node
            System.out.println("Starting disconnected node " + myId);

        // Create the actor system
        final ActorSystem system = ActorSystem.create("mysystem", config);

        // Create a single node actor....The basic communication will be made bye this command line. It will start listening 
        final ActorRef receiver = system.actorOf(
                Props.create(Node.class), // actor class
                "node" // actor name
        );
    }

}
