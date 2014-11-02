package kduraj;

import java.io.File;
import java.sql.SQLException;
import java.util.List;

public class App {
    
    /*--------------------------------------------------------------------------------------------*/ 
    public static final String url      = "jdbc:mysql://127.0.0.1:3306/files";
    public static final String user     = "root";
    public static final String password = "";
    /*--------------------------------------------------------------------------------------------*/
    static String[][] params = new String [][] {
                    {   "/Users/xcode", "java"    },
                    {   "/Users/xcode", "txt"    },		    
    };

    /*-------------------------------------------------------------------------------------*/
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        
        System.out.println("Loading Files");        
        //List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < params.length; i++) {

            Runnable task = new BrowseDirectories(true, new File(params[i][0]), params[i][1]);
            Thread worker = new Thread(task);
            worker.setName(String.valueOf(i));
            worker.start();
            //threads.add(worker);
        }
    }
    /*-------------------------------------------------------------------------------------*/
     private static void who_is_running(List<Thread> threads) {
        
        int running = 0;
        
        do {
            running = 0;
            for (Thread thread : threads) {
            
            	System.out.println("thread = " + thread.getName());
            	if (thread.isAlive()) {
            		running++;
                }
            }
            System.out.println(running + " running threads. ");
            try { Thread.sleep(1000); } catch(Exception ex) { }
            		
        } while (running > 0);
        

    }
    /*-------------------------------------------------------------------------------------*/
}

