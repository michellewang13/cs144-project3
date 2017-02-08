This example contains a simple utility class to simplify opening database
connections in Java applications, such as the one you will write to build
your Lucene index. 

To build and run the sample code, use the "run" ant target inside
the directory with build.xml by typing "ant run".
____________________________________________________________________________

We decided to create an index on the following attributes: ItemID, Name, and Content, which consists of the Name, Category, and Description fields of the Item object concatenated together. We only store the ItemID and Name fields since we are only required to return the itemId and names of Items. We use the Content field for keyword-based searching so we don't store this to save index space.