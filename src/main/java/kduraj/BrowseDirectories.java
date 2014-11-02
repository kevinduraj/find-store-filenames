package kduraj;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

/*------------------------------------------------------------------------------------------------*/
public class BrowseDirectories implements Runnable {

    boolean createTable = false;
    Collection<File> allfiles;
    File file;
    String extension;
    int file_count = 0;
    String PREFIX = "filetype_";

    /*--------------------------------------------------------------------------------------------*/
    public BrowseDirectories(boolean createTable, File file, String extension) {
        this.createTable = createTable;
        this.file = file;
        this.extension = extension;
        allfiles = new ArrayList<>();
    }

    /*--------------------------------------------------------------------------------------------*/
    @Override
    public void run() {
        try {
            if (createTable) {
                create_filetype_table();
            }
            browseSubDir(this.file);
            insert_filenames();
            
        } catch (SQLException | ClassNotFoundException ex) {
            
            System.out.println("file = " + file.toString());
            System.err.println("SQLException: "
                    + Thread.currentThread().getStackTrace()[2].getClassName() + "\n"
                    + Thread.currentThread().getStackTrace()[2].getMethodName() + ":"
                    + Thread.currentThread().getStackTrace()[2].getLineNumber() + "\n"
                    + ex.getMessage() + "\n");
        }
    }
    /*--------------------------------------------------------------------------------------------*/

    public void browseSubDir(File file2) {
        try {
            File[] children = file2.listFiles();
            if (children != null) {
                for (File child : children) {

                    if (child.toString().endsWith(extension)) {

                        if ((++file_count % 10) == 0) {
                            System.out.println("FILE=" + file_count + " : " + child.toString());
                        }
                        allfiles.add(child);
                    }
                    browseSubDir(child);
                }
            }
        } catch (Exception ex) {
            System.err.println("SQLException: "
                    + Thread.currentThread().getStackTrace()[2].getClassName() + "\n"
                    + Thread.currentThread().getStackTrace()[2].getMethodName() + ":"
                    + Thread.currentThread().getStackTrace()[2].getLineNumber() + "\n"
                    + ex.getMessage() + "\n");
        }
    }
    /*--------------------------------------------------------------------------------------------*/

    public void insert_filenames() {
        int cnt = 0;

        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(App.url, App.user, App.password);

            String SQL = "INSERT INTO " + PREFIX + extension + " ( filename, note )"
                    + " VALUES ( ?, ? ) "
                    + " ON DUPLICATE KEY UPDATE counter=counter+1; ";
            stmt = conn.prepareStatement(SQL);

            for (File fileLocal : allfiles) {

                if ((++cnt % 500) == 0) {
                    System.out.println("INSERT=" + cnt + " : " + fileLocal.toString());
                }

                stmt.setString(1, fileLocal.toString());
                stmt.setString(2, "new");
                stmt.executeUpdate();
            }

        } catch (SQLException | ClassNotFoundException ex) {
            
            System.err.println("SQLException: "
                    + Thread.currentThread().getStackTrace()[2].getClassName() + "\n"
                    + Thread.currentThread().getStackTrace()[2].getMethodName() + ":"
                    + Thread.currentThread().getStackTrace()[2].getLineNumber() + "\n"
                    + ex.getMessage() + "\n");
        } finally {
            try {
                
                stmt.close();
                conn.close();
                
            } catch (SQLException ex) {
                
                System.err.println("SQLException: "
                        + Thread.currentThread().getStackTrace()[2].getClassName() + "\n"
                        + Thread.currentThread().getStackTrace()[2].getMethodName() + ":"
                        + Thread.currentThread().getStackTrace()[2].getLineNumber() + "\n"
                        + ex.getMessage() + "\n");
                
            }
        }

    }
    /*--------------------------------------------------------------------------------------------*/

    public void create_filetype_table() throws SQLException, ClassNotFoundException {

        String SQL;
        PreparedStatement stmt;
        System.out.println("create filetype table");

        Class.forName("com.mysql.jdbc.Driver");
        
        try (Connection conn = DriverManager.getConnection(App.url, App.user, App.password)) {
            
            SQL = "DROP TABLE IF EXISTS " + PREFIX + extension + ";";
            stmt = conn.prepareStatement(SQL);
            stmt.executeUpdate();
            
            SQL = "CREATE TABLE IF NOT EXISTS " + PREFIX + extension + " ( \n"
                    + "  filename        varchar(128)  NOT NULL, \n"
                    + "  dts             timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP, \n"
                    + "  counter         int(11)       NOT NULL DEFAULT '1',   \n"
                    + "  note            char(16)      NOT NULL DEFAULT 'new', \n"
                    + "  PRIMARY KEY     (filename),   \n"
                    + "  KEY processed   (dts)         \n"
                    + ") ENGINE=MyISAM;";
            //+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;";
            
            System.out.println("\n\nSQL = " + SQL + "\n\n");
            
            stmt = conn.prepareStatement(SQL);
            stmt.executeUpdate();
            
            stmt.close();
        }

    }
    /*--------------------------------------------------------------------------------------------*/

}
/*------------------------------------------------------------------------------------------------*/
