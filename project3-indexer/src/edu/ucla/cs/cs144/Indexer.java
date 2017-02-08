package edu.ucla.cs.cs144;

import java.io.IOException;
import java.io.StringReader;
import java.io.File;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class Indexer {
    
    /** Creates a new instance of Indexer */
    public Indexer() {
    }

    private IndexWriter indexWriter = null;

    public IndexWriter getIndexWriter(boolean create) throws IOException {
        if (indexWriter == null) {
            Directory indexDir = FSDirectory.open(new File("/var/lib/lucene/index"));
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_2, new StandardAnalyzer());
            indexWriter = new IndexWriter(indexDir, config);
        }
        return indexWriter;
    }

    public void closeIndexWriter() throws IOException {
        if (indexWriter != null) {
            indexWriter.close();
        }
    }

    public void indexItem(int itemID, String name, String description, String categories) throws IOException {
        IndexWriter writer = getIndexWriter(false);
        Document doc = new Document();
        doc.add(new StringField("ItemID", String.valueOf(itemID), Field.Store.YES));
        doc.add(new StringField("Name", name, Field.Store.YES));
        String fullSearchableText = itemID + " " + name + " " + description + " " + categories;
        doc.add(new TextField("Conent", fullSearchableText, Field.Store.NO));
        writer.addDocument(doc);
    }
 
    public void rebuildIndexes() {

        Connection conn = null;

        // create a connection to the database to retrieve Items from MySQL
    	try {
    	    conn = DbManager.getConnection(true);
    	} catch (SQLException ex) {
    	    System.out.println(ex);
    	}

    	try {
            getIndexWriter(true);

            Statement stmt = conn.createStatement();
            String query =  "SELECT Item.ItemID, Item.Name, Item.Description, IDCategories.Categories "
                            + "FROM (SELECT ItemID, GROUP_CONCAT(Category.Category SEPARATOR ' ') AS Categories "
                            + "FROM Category GROUP BY ItemID) AS IDCategories "
                            + "INNER JOIN Item ON Item.ItemID = IDCategories.ItemID";

            int itemID;
            String name, description, categories;

            ResultSet rs = stmt.executeQuery(query);

            while(rs.next()) {
                itemID = rs.getInt("ItemID");
                name = rs.getString("Name");
                description = rs.getString("Description");
                categories = rs.getString("Categories");
                indexItem(itemID, name, description, categories);
            }

            closeIndexWriter();

            // close the database connection
    	    conn.close();
    	} catch (Exception ex) {
    	    System.out.println(ex);
    	}
    }    

    public static void main(String args[]) {
        Indexer idx = new Indexer();
        idx.rebuildIndexes();
    }   
}
