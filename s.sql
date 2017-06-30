CREATE TABLE publicationType (pubTypeID INTEGER, pType VARCHAR(50));
CREATE TABLE venueType (venueTypeID INTEGER, vType VARCHAR(50));
CREATE TABLE institute (instituteID INTEGER, iName VARCHAR(200), yearFounded VARCHAR(4));
CREATE TABLE venue (venueID INTEGER, venueTypeID INTEGER, vName VARCHAR(50), eventYear VARCHAR(4));
CREATE TABLE publication (publicationID INTEGER, pubTypeID INTEGER, title VARCHAR(200), venueID INTEGER, pubDate DATE);
CREATE TABLE author (authorID INTEGER, firstName VARCHAR(50), lastName VARCHAR(50), instituteID INTEGER);
CREATE TABLE authorship (authorID INTEGER, publicationID INTEGER);

/*
QUERY 1: Basic selection
*/
SELECT I.iName, I.yearFounded, A.firstName
FROM author A, institute I
WHERE A.instituteID = I.instituteID
  AND author.instituteID = 1
ORDER BY A.instituteID ASC;
