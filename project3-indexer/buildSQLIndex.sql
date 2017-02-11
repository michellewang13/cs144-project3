CREATE TABLE Location (
	ItemID INTEGER NOT NULL,
	Position POINT NOT NULL,
	PRIMARY KEY(ItemID),
	FOREIGN KEY(ItemID) references Item(ItemID)
) ENGINE = MyISAM;

INSERT INTO Location 
	SELECT ItemID, Point(Latitude, Longitude) 
	FROM Item 
	WHERE Latitude<>'null' AND Longitude<>'null';

CREATE SPATIAL INDEX SpatialIndex ON Location(Position);