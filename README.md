# Distributed Friend Recommender using Hadoop and MapReduce
This program was created in the fall term 2021 for the course LOG8415 at Polytechnique Montréal.

# Algorithm

The algorithm consists of two Map-Reduce steps:

MAP 1:
For each person, extract the couples of people who might know each other via him.
Input: FriendID				OtherID,OtherID,...
Output: OtherIDxOtherID		True/False

REDUCE 1:
For each couple of people, extract the number of common friends they have. 0 if they already know each other.
Input: OtherIDxAnotherID		True/False
Output: OtherIDxAnotherID		Integer

MAP 2:
Send each person his possible friends
Input: MyIDxOtherID		Integer
Output: MyID 		OtherID Integer
		OtherID 	MyID Integer

REDUCE 2:
Sort the possible friends for each person
Input: MyID 	OtherID Integer
Output: MyID	OtherID AnotherID YetAnotherID ...

# Usage

**Building**: mvn package

**Running**: hadoop jar target/all-jar-with-dependencies.jar INPUT_PATH/INPUT_FILE.txt OUTPUT_PATH

*The directories OUTPUT\_PATH/first-mapper and OUTPUT\_PATH/second-mapper need to be non-existent or empty.*

**Output**: in OUTPUT_PATH/second-mapper/part-r-0000
