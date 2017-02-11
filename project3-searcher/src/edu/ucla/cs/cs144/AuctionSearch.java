package edu.ucla.cs.cs144;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.ucla.cs.cs144.DbManager;
import edu.ucla.cs.cs144.SearchRegion;
import edu.ucla.cs.cs144.SearchResult;

public class AuctionSearch implements IAuctionSearch {
	private IndexSearcher searcher = null;
	private QueryParser parser = null;

	public AuctionSearch() {
		try {
			searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File("/var/lib/lucene/index/"))));
			parser = new QueryParser("Content", new StandardAnalyzer());
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	public Document getDocument(int docId) throws IOException {
		return searcher.doc(docId);
	}

	public String getRegionBounds(double lx, double ly, double rx, double ry) {
		return "GeomFromText('Polygon((" + lx + " " + ly + ", " + lx + " " + ry + ", " + rx + " " + ry + ", " + rx + " " + ly + ", " + lx + " " + ly +  "))')";
	}
	
	public SearchResult[] basicSearch(String query, int numResultsToSkip, 
			int numResultsToReturn) {
		try {
			Query queryObj = parser.parse(query);

			TopDocs results = searcher.search(queryObj, numResultsToSkip + numResultsToReturn);

			ScoreDoc[] hits = results.scoreDocs;
			int numHits = hits.length;

			// We need to check how many results we need to return
			if (numHits < numResultsToSkip)
				numResultsToReturn = 0;
			else
				numResultsToReturn = Math.min(numHits, numResultsToReturn);

			SearchResult[] searchResults = new SearchResult[numResultsToReturn];

			for (int i = numResultsToSkip, j = 0; i < numResultsToSkip + numResultsToReturn; i++, j++) {
				Document doc = this.getDocument(hits[i].doc);
				searchResults[j] = new SearchResult(doc.get("ItemID"), doc.get("name"));
			}
		
		return searchResults;

		} catch (IOException e) {
			System.out.println(e);
		} catch (ParseException e) {
			System.out.println(e);
		}

		return new SearchResult[0];
	}

	public SearchResult[] spatialSearch(String query, SearchRegion region,
			int numResultsToSkip, int numResultsToReturn) {
		
		try {
			// Results list (will be transferred to array later)
			ArrayList<SearchResult> searchResults = new ArrayList<SearchResult>();

			// Establish DB connection
			Connection con = DbManager.getConnection(true);

			// Do basic search
			int originalNumResultsToReturn = numResultsToReturn;
			SearchResult[] results = basicSearch(query, 0, numResultsToReturn);

			// Get region bounds as a MySQL geometry
			String regionBounds = this.getRegionBounds(region.getLx(), region.getLy(), region.getRx(), region.getRy());

			// Create JDBC prepared statements and use MBRContains
			PreparedStatement checkPosition = con.prepareStatement(
				"SELECT MBRContains(" + regionBounds + ",Position) AS inRegion FROM Location WHERE ItemID=?"
			);

			// Keep basic searching and spatial searching until numResultsToReturn results have been added
			while (numResultsToReturn > 0) {
				// Check each item found by basic search if in region
				for (int i = 0; i < results.length; i++) {
					SearchResult item = results[i];

					// Execute query
					checkPosition.setString(1, item.getItemId());
					ResultSet checkPositionRS = checkPosition.executeQuery();

					// Add to search results if in region
					if (checkPositionRS.next() && checkPositionRS.getBoolean("inRegion")) {
						if (numResultsToSkip > 0) {
							numResultsToSkip--;
						
						} else if (numResultsToReturn > 0) {
							System.out.println(numResultsToReturn);
							searchResults.add(item);
							numResultsToReturn--;
						} else {
							checkPositionRS.close();
							break;
						}
					}

					checkPositionRS.close();
				}

				results = basicSearch(query, results.length, originalNumResultsToReturn);
			}

			checkPosition.close();
			con.close();

			// Transfer searchResults to an array
			int numResults = searchResults.size();

			SearchResult[] finalSearchResults = new SearchResult[numResults];
			for (int i = 0; i < numResults; i++)
				finalSearchResults[i] = searchResults.get(i);

			return finalSearchResults;
			
		} catch (SQLException e) {
			System.out.println(e);
		}

		return new SearchResult[0];
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		return "";
	}
	
	public String echo(String message) {
		return message;
	}

}
