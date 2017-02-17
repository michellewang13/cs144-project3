package edu.ucla.cs.cs144;

import java.util.*;
import java.sql.*;
import java.util.*;

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
				numResultsToReturn = Math.min(numHits - numResultsToSkip, numResultsToReturn);

			SearchResult[] searchResults = new SearchResult[numResultsToReturn];

			for (int i = numResultsToSkip, j = 0; i < numResultsToSkip + numResultsToReturn; i++, j++) {
				Document doc = this.getDocument(hits[i].doc);
				searchResults[j] = new SearchResult(doc.get("ItemID"), doc.get("Name"));
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
			int index = 0;

			int originalNumResultsToReturn = numResultsToReturn;
			SearchResult[] results = basicSearch(query, index, numResultsToReturn);
			index += results.length;

			// Get region bounds as a MySQL geometry
			String regionBounds = this.getRegionBounds(region.getLx(), region.getLy(), region.getRx(), region.getRy());

			// Create JDBC prepared statements and use MBRContains
			PreparedStatement checkPosition = con.prepareStatement(
				"SELECT MBRContains(" + regionBounds + ",Position) AS inRegion FROM Location WHERE ItemID=?"
			);

			// Keep basic searching and spatial searching until numResultsToReturn results have been added
			while (numResultsToReturn > 0) {
				System.out.println(String.format("numResultsToReturn: %d", numResultsToReturn));
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
							//System.out.println(numResultsToReturn);
							searchResults.add(item);
							numResultsToReturn--;
						} else {
							checkPositionRS.close();
							break;
						}
					}

					checkPositionRS.close();
				}

				results = basicSearch(query, index, originalNumResultsToReturn);
				index += results.length;

				if (results.length <= 0)
					break;
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
        Connection conn = null;
        String xmlData = "";

        try {
            conn = DbManager.getConnection(true);
            Statement stmt = conn.createStatement();

            Map<String, String> itemsMap = getItemData(itemId, stmt);
            if (itemsMap == null) {
                return "";
            }

            String name = itemsMap.get("Name");
            String location = itemsMap.get("Location");
            String country = itemsMap.get("Country");
            String latitude = itemsMap.get("Latitude");
            String longitude = itemsMap.get("Longitude");
            String userID = itemsMap.get("UserID");
            String description = itemsMap.get("Description");
            String started = itemsMap.get("Started");
            String ends = itemsMap.get("Ends");
            String currently = itemsMap.get("Currently");
            String buyPrice = itemsMap.get("Buy_Price");
            String firstBid = itemsMap.get("First_Bid");
            String numBids = itemsMap.get("NumBids");

            ArrayList<String> categoryList = getCategoryData(itemId, stmt);
            String sellerRating = getSellerData(userID, stmt);
            ArrayList<Map<String, String>> bidsList = getBidData(itemId, stmt);

            // format into XML
            String categories, bids, locationAttributes;
            categories = formatCategories(categoryList);
            bids = formatBids(bidsList);
            locationAttributes = (latitude.equals("") && longitude.equals(""))? "" : formatLatitudeLongitude(latitude, longitude);
            buyPrice = buyPrice.equals("0.00")? "" : "  <Buy_Price>$" + buyPrice + "</Buy_Price>\n";

            if (bids.isEmpty()) {
            	xmlData = String.format(
	                            "<Item ItemID=\"%s\">\n" +
	                            "  <Name>%s</Name>\n" +
	                            "%s" +
	                            "  <Currently>$%s</Currently>\n" +
	                            "%s" +
	                            "  <First_Bid>$%s</First_Bid>\n" +
	                            "  <Number_of_Bids>%s</Number_of_Bids>\n" +
	                            "  <Bids />\n" +
	                            "  <Location%s>%s</Location>\n" +
	                            "  <Country>%s</Country>\n" +
	                            "  <Started>%s</Started>\n" +
	                            "  <Ends>%s</Ends>\n" +
	                            "  <Seller Rating=\"%s\" UserID=\"%s\" />\n" +
	                            "  <Description>%s</Description>\n" +
	                            "</Item>",
	                    itemId, name, categories, currently, buyPrice, firstBid,
	                    numBids, locationAttributes, location, country,
	                    started, ends, sellerRating, userID, description
	            );
            }
            else {
	            xmlData = String.format(
	                            "<Item ItemID=\"%s\">\n" +
	                            "  <Name>%s</Name>\n" +
	                            "%s" +
	                            "  <Currently>$%s</Currently>\n" +
	                            "%s" +
	                            "  <First_Bid>$%s</First_Bid>\n" +
	                            "  <Number_of_Bids>%s</Number_of_Bids>\n" +
	                            "  <Bids>\n%s  </Bids>\n" +
	                            "  <Location%s>%s</Location>\n" +
	                            "  <Country>%s</Country>\n" +
	                            "  <Started>%s</Started>\n" +
	                            "  <Ends>%s</Ends>\n" +
	                            "  <Seller Rating=\"%s\" UserID=\"%s\" />\n" +
	                            "  <Description>%s</Description>\n" +
	                            "</Item>",
	                    itemId, name, categories, currently, buyPrice, firstBid,
	                    numBids, bids, locationAttributes, location, country,
	                    started, ends, sellerRating, userID, description
	            );
			}
        } catch (SQLException ex) {
            System.out.println(ex);
        }

        return xmlData;
	}

	public String formatCategories(ArrayList<String> categoryList) {
		String categories = "";
		for (String category : categoryList) {
			categories += String.format("  <Category>%s</Category>\n", category);
		}
		return categories;
	}

	public String formatBids(ArrayList<Map<String, String>> bidList) {
		String bids = "";
		for (Map<String, String> bidMap : bidList) {
			String detailedLocation = "";
			String location = bidMap.get("Location");
			String country = bidMap.get("Country");

			if (location != null) {
				detailedLocation += String.format("        <Location>%s</Location>\n", location);
			}
			if (country != null) {
				detailedLocation += String.format("        <Country>%s</Country>\n", country);
			}

			bids += String.format("    <Bid>\n      <Bidder Rating=\"%s\" UserID=\"%s\">\n" + 
									"%s" + 
									"      </Bidder>\n" + 
									"      <Time>%s</Time>\n" + 
									"      <Amount>$%s</Amount>\n" + 
									"  </Bid>\n", bidMap.get("Bidder_Rating"), 
									bidMap.get("UserID"), detailedLocation, bidMap.get("Time"), bidMap.get("Amount"));
		}
		return bids;
	}

	public String formatLatitudeLongitude(String latitude, String longitude) {
		String s = "";
		if (latitude != null && longitude != null) {
			s = String.format(" Latitude=\"%s\" Longitude=\"%s\"", latitude, longitude);
		}
		return s;
	}

	public Map<String, String> getItemData(String itemId, Statement stmt) throws SQLException {
		String name, currently, buyPrice, firstBid, numBids, location, country, latitude, longitude, started, ends, userId, description;
		ResultSet rs = stmt.executeQuery("SELECT * FROM Item WHERE ItemID = " + itemId);

		if (!rs.next()) {
			return null;
		}

		name = escapeChar(rs.getString("Name"));
		currently = String.format("%.2f", rs.getDouble("Currently"));
		buyPrice = String.format("%.2f", rs.getDouble("Buy_Price"));
		firstBid = String.format("%.2f", rs.getDouble("First_Bid"));
		numBids = Integer.toString(rs.getInt("Number_of_Bids"));
		
		location = escapeChar(rs.getString("Location"));
		country = escapeChar(rs.getString("Country"));
		latitude = rs.getString("Latitude");
		longitude = rs.getString("Longitude");
		userId = escapeChar(rs.getString("UserID"));
		description = escapeChar(rs.getString("Description"));

		SimpleDateFormat outputFormat = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
		started = outputFormat.format(rs.getTimestamp("Started"));
		ends = outputFormat.format(rs.getTimestamp("Ends"));

		Map<String, String> itemsMap = new HashMap<String, String>();
		itemsMap.put("Name", name);
		itemsMap.put("Currently", currently);
		itemsMap.put("Buy_Price", buyPrice);
		itemsMap.put("First_Bid", firstBid);
		itemsMap.put("Location", location);
		itemsMap.put("Country", country);
		itemsMap.put("Latitude", latitude);
		itemsMap.put("Longitude", longitude);
		itemsMap.put("Started", started);
		itemsMap.put("Ends", ends);
		itemsMap.put("UserID", userId);
		itemsMap.put("Description", description);
		itemsMap.put("NumBids", numBids);

		return itemsMap;
	}

	public ArrayList<String> getCategoryData(String itemId, Statement stmt) throws SQLException {
		ResultSet rs = stmt.executeQuery("SELECT Category FROM Category WHERE ItemID = " + itemId);

		ArrayList<String> categoryList = new ArrayList<String>();
		while(rs.next()) {
			categoryList.add(escapeChar(rs.getString("Category")));
		}

		return categoryList;	
	} 

	public ArrayList<Map<String, String>> getBidData(String itemId, Statement stmt) throws SQLException {
		ResultSet rs = stmt.executeQuery("SELECT * FROM Bid WHERE ItemID = " + itemId);

		ArrayList<Map<String, String>> bidList = new ArrayList<Map<String, String>>();

		while (rs.next()) {
			Map<String, String> bidMap = new HashMap<String, String>();
			String userId = escapeChar(rs.getString("UserID"));
			bidMap.put("UserID", userId);

			SimpleDateFormat outputFormat = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
			String time = outputFormat.format(rs.getTimestamp("Time")); 
			bidMap.put("Time", time);
			bidMap.put("Amount", String.format("%.2f", rs.getFloat("Amount")));
			bidList.add(bidMap);
		}

		for (Map<String, String> bid : bidList) {
			String userId = bid.get("UserID");
			bid.putAll(getBidderData(userId, stmt));
		}

		return bidList;
	}
	
	public String getSellerData(String userId, Statement stmt) throws SQLException {
		int rating = 0;
		ResultSet rs = stmt.executeQuery("SELECT Seller_Rating FROM User WHERE UserID = '" + userId + "'");

		while (rs.next()) {
			rating = rs.getInt("Seller_Rating");
		}

		return Integer.toString(rating);
	}

	public Map<String, String> getBidderData(String userId, Statement stmt) throws SQLException {
		ResultSet rs = stmt.executeQuery("SELECT * FROM User WHERE UserID = '" + userId + "'");
		Map<String, String> bidderMap = new HashMap<String, String>();

		while (rs.next()) {
			bidderMap.put("Bidder_Rating", Integer.toString(rs.getInt("Bidder_Rating")));
			bidderMap.put("Location", escapeChar(rs.getString("Location")));
			bidderMap.put("Country", escapeChar(rs.getString("Country")));
		}

		return bidderMap;
	}

	private String escapeChar(String s) {
        s = s.replace("&", "&amp;");
        s = s.replace("\"", "&quot;");
        s = s.replace("'", "&apos;");
        s = s.replace("<", "&lt;");
        s= s.replace(">", "&gt;");
        return s;
    }

	public String echo(String message) {
		return message;
	}

}
