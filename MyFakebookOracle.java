package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

public class MyFakebookOracle extends FakebookOracle {

    static String prefix = "tajik.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding tables in your database
    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;


    // DO NOT modify this constructor
    public MyFakebookOracle(String dataType, Connection c) {
        super();
        oracleConnection = c;
        // You will use the following tables in your Java code
        cityTableName = prefix + dataType + "_CITIES";
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITY";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITY";
        programTableName = prefix + dataType + "_PROGRAMS";
        educationTableName = prefix + dataType + "_EDUCATION";
        eventTableName = prefix + dataType + "_USER_EVENTS";
        albumTableName = prefix + dataType + "_ALBUMS";
        photoTableName = prefix + dataType + "_PHOTOS";
        tagTableName = prefix + dataType + "_TAGS";
    }


    @Override
    // ***** Query 0 *****
    // This query is given to your for free;
    // You can use it as an example to help you write your own code
    //
    public void findMonthOfBirthInfo() {

        // Scrollable result set allows us to read forward (using next())
        // and also backward.
        // This is needed here to support the user of isFirst() and isLast() methods,
        // but in many cases you will not need it.
        // To create a "normal" (unscrollable) statement, you would simply call
        // Statement stmt = oracleConnection.createStatement();
        //
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {

            // For each month, find the number of users born that month
            // Sort them in descending order of count
            ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from " +
                    userTableName +
                    " where month_of_birth is not null group by month_of_birth order by 1 desc");

            this.monthOfMostUsers = 0;
            this.monthOfLeastUsers = 0;
            this.totalUsersWithMonthOfBirth = 0;

            // Get the month with most users, and the month with least users.
            // (Notice that this only considers months for which the number of users is > 0)
            // Also, count how many total users have listed month of birth (i.e., month_of_birth not null)
            //
            while (rst.next()) {
                int count = rst.getInt(1);
                int month = rst.getInt(2);
                if (rst.isFirst())
                    this.monthOfMostUsers = month;
                if (rst.isLast())
                    this.monthOfLeastUsers = month;
                this.totalUsersWithMonthOfBirth += count;
            }

            // Get the names of users born in the "most" month
            rst = stmt.executeQuery("select user_id, first_name, last_name from " +
                    userTableName + " where month_of_birth=" + this.monthOfMostUsers);
            while (rst.next()) {
                Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                this.usersInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
            }

            // Get the names of users born in the "least" month
            rst = stmt.executeQuery("select first_name, last_name, user_id from " +
                    userTableName + " where month_of_birth=" + this.monthOfLeastUsers);
            while (rst.next()) {
                String firstName = rst.getString(1);
                String lastName = rst.getString(2);
                Long uid = rst.getLong(3);
                this.usersInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
            }

            // Close statement and result set
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 1 *****
    // Find information about users' names:
    // (1) The longest first name (if there is a tie, include all in result)
    // (2) The shortest first name (if there is a tie, include all in result)
    // (3) The most common first name, and the number of times it appears (if there
    //      is a tie, include all in result)
    //
    /*
    public void findNameInfo() { // Query1
        // Find the following information from your database and store the information as shown
        this.longestFirstNames.add("JohnJacobJingleheimerSchmidt");
        this.shortestFirstNames.add("Al");
        this.shortestFirstNames.add("Jo");
        this.shortestFirstNames.add("Bo");
        this.mostCommonFirstNames.add("John");
        this.mostCommonFirstNames.add("Jane");
        this.mostCommonFirstNamesCount = 10;
    }*/
    public void findNameInfo() throws SQLException { // Query1
    	//query name lengths
    	String query = "SELECT FIRST_NAME, LENGTH(FIRST_NAME) FROM " + userTableName
    			+ " GROUP BY FIRST_NAME ORDER BY LENGTH(FIRST_NAME) DESC";
  
    	try(Statement stmt = 
    			oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)) {
    		ResultSet rs = stmt.executeQuery(query);
    		
    		//get longest names
    		rs.next();
    		int max = rs.getInt(2);
    		this.longestFirstNames.add(rs.getString(1));
    		rs.next();
    		while(rs.getInt(2) == max){
    			this.longestFirstNames.add(rs.getString(1));
    			rs.next();
    		}
    		
    		//get shortest names
    		rs.last();
    		int min = rs.getInt(2);
    		this.shortestFirstNames.add(rs.getString(1));
    		rs.previous();
    		while(rs.getInt(2) == min){
    			this.shortestFirstNames.add(rs.getString(1));
    			rs.previous();
    		}
  
    	} catch (SQLException e) {
    		System.err.println(e.getMessage());
    	}
    	//query name frequency
    	query = "SELECT COUNT(USER_ID), FIRST_NAME FROM " + userTableName
    			+ " GROUP BY FIRST_NAME ORDER BY COUNT(USER_ID) DESC";
    	
    	try(Statement stmt = 
    			oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)) {
    		ResultSet rs = stmt.executeQuery(query);
    		//find common names
    		rs.next();
    		this.mostCommonFirstNamesCount = rs.getInt(1);
    		this.mostCommonFirstNames.add(rs.getString(2));
    		int max = rs.getInt(1);

    		rs.next();
    		while(rs.getInt(1) == max){
    			this.mostCommonFirstNames.add(rs.getString(2));
    			rs.next();
    		}
    	} catch (SQLException e) {
        	System.err.println(e.getMessage());
    	}        
    }
    
    @Override
    // ***** Query 2 *****
    // Find the user(s) who have no friends in the network
    //
    // Be careful on this query!
    // Remember that if two users are friends, the friends table
    // only contains the pair of user ids once, subject to
    // the constraint that user1_id < user2_id
    //
    public void lonelyUsers() {
    	String query = "SELECT USER_ID, FIRST_NAME, LAST_NAME FROM " + userTableName
    			+ " WHERE USER_ID IN (SELECT USER_ID FROM " + userTableName
    			+ " MINUS (SELECT DISTINCT U.USER_ID FROM " + userTableName + " U, " 
    			+ friendsTableName + " F WHERE F.USER1_ID = U.USER_ID GROUP BY U.USER_ID"
    			+ " UNION SELECT DISTINCT U.USER_ID FROM " + userTableName + " U, "
    			+ friendsTableName + " F WHERE F.USER2_ID = U.USER_ID GROUP BY U.USER_ID))";
  
    	try(Statement stmt = 
    			oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)) {
    		ResultSet rs = stmt.executeQuery(query);
    		
    		while(rs.next()){
    			this.lonelyUsers.add(new UserInfo(rs.getLong(1), rs.getString(2), rs.getString(3)));
    		}
    	} catch (SQLException e) {
    		System.err.println(e.getMessage());
    	}
    }

    @Override
    // ***** Query 3 *****
    // Find the users who do not live in their hometowns
    // (I.e., current_city != hometown_city)
    //
    public void liveAwayFromHome() throws SQLException {
    	String query = "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME FROM " + userTableName
    			+ " U, " + currentCityTableName + " C, " + hometownCityTableName
    			+ " H WHERE U.USER_ID = C.USER_ID AND U.USER_ID = H.USER_ID"
    			+ " AND H.HOMETOWN_CITY_ID != C.CURRENT_CITY_ID ORDER BY U.USER_ID";
  
    	try(Statement stmt = 
    			oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)) {
    		ResultSet rs = stmt.executeQuery(query);
    		
    		while(rs.next()){
    			this.liveAwayFromHome.add(new UserInfo(rs.getLong(1), rs.getString(2), rs.getString(3)));
    		}
    	} catch (SQLException e) {
    		System.err.println(e.getMessage());
    	}
    }

    @Override
    // **** Query 4 ****
    // Find the top-n photos based on the number of tagged users
    // If there are ties, choose the photo with the smaller numeric PhotoID first
    //
    public void findPhotosWithMostTags(int n) {
    	String query = "SELECT P.PHOTO_ID, COUNT(T.TAG_SUBJECT_ID), P.ALBUM_ID,"
    			+ " A.ALBUM_NAME, P.PHOTO_CAPTION, P.PHOTO_LINK FROM " + photoTableName
    			+ " P, " + tagTableName + " T, " + albumTableName + " A"
    			+ " WHERE T.TAG_PHOTO_ID = P.PHOTO_ID AND P.ALBUM_ID = A.ALBUM_ID"
    			+ " GROUP BY P.PHOTO_ID, P.ALBUM_ID, A.ALBUM_NAME, P.PHOTO_CAPTION, P.PHOTO_LINK"
    			+ " ORDER BY COUNT(T.TAG_SUBJECT_ID) DESC, P.PHOTO_ID";
    	String query2;
    	
    	try(Statement stmt = 
    		oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
    			ResultSet.CONCUR_READ_ONLY)) {
    		ResultSet rs = stmt.executeQuery(query);
    		
    		int j = 0;
    		//loops through n top photos (already organized in right order)
    		while(j < n){
    			rs.next();
    			PhotoInfo p = new PhotoInfo(rs.getString(1), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6));
    			TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
    			
    			query2 = "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME FROM "
    					+ userTableName + " U, " + tagTableName + " T "
    					+ "WHERE T.TAG_SUBJECT_ID = U.USER_ID AND T.TAG_PHOTO_ID = " + rs.getString(1);
    			//sets up new connection to database to fetch tag data for each photo
    			try(Statement stmt2 =
    				oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
    					ResultSet.CONCUR_READ_ONLY)) {
    				ResultSet rs2 = stmt2.executeQuery(query2);
    				
    				while(rs2.next()){
    			        tp.addTaggedUser(new UserInfo(rs2.getLong(1), rs2.getString(2), rs2.getString(3)));
    				}
    			} catch (SQLException e) {
    				System.err.println(e.getMessage());
    			}
    			this.photosWithMostTags.add(tp);
    			j++;
    		}
    	} catch (SQLException e) {
    		System.err.println(e.getMessage());
    	}
    }

    @Override
    // **** Query 5 ****
    // Find suggested "match pairs" of users, using the following criteria:
    // (1) One of the users is female, and the other is male
    // (2) Their age difference is within "yearDiff"
    // (3) They are not friends with one another
    // (4) They should be tagged together in at least one photo
    //
    // You should return up to n "match pairs"
    // If there are more than n match pairs, you should break ties as follows:
    // (i) First choose the pairs with the largest number of shared photos
    // (ii) If there are still ties, choose the pair with the smaller user_id for the female
    // (iii) If there are still ties, choose the pair with the smaller user_id for the male
   
   
    public void matchMaker(int n, int yearDiff) {
    	 //selects all doubles in age limit, different genders, in a picture together MINUS THE SET OF FRIEND DOUBLES
    	String query = "SELECT U1.USER_ID, U2.USER_ID, COUNT(P.PHOTO_ID) FROM "
    			+ userTableName + " U1, " + userTableName + " U2, " + tagTableName + " T1, "
    			+ tagTableName + " T2, " + photoTableName + " P "
    			+ "WHERE U1.USER_ID = T1.TAG_SUBJECT_ID AND U2.USER_ID = T2.TAG_SUBJECT_ID"
    			+ " AND T1.TAG_PHOTO_ID = P.PHOTO_ID AND T2.TAG_PHOTO_ID = P.PHOTO_ID"
    			+ " AND(U1.USER_ID, U2.USER_ID) IN (SELECT DISTINCT U1.USER_ID, U2.USER_ID FROM "
				+ userTableName + " U1, " + userTableName + " U2, " + tagTableName + " T1, "
    			+ tagTableName + " T2 WHERE U1.GENDER != U2.GENDER AND U1.GENDER = 'male' "
    			+ "AND (U2.YEAR_OF_BIRTH - U1.YEAR_OF_BIRTH) <= " + yearDiff
    			+ " AND (U2.YEAR_OF_BIRTH - U1.YEAR_OF_BIRTH) >= 0"
    			+ " AND (T1.TAG_SUBJECT_ID = U1.USER_ID AND T2.TAG_SUBJECT_ID = U2.USER_ID)"
    			+ " AND T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID AND (U1.USER_ID, U2.USER_ID) NOT IN"
    			+ " (SELECT U1.USER_ID, U2.USER_ID FROM " + userTableName + " U1, " + userTableName + " U2, "
    			+ friendsTableName + " F WHERE F.USER1_ID = U1.USER_ID AND F.USER2_ID = U2.USER_ID "
    			+ "UNION SELECT U2.USER_ID, U1.USER_ID FROM " + userTableName + " U1, " + userTableName + " U2, "
    			+ friendsTableName + " F WHERE F.USER1_ID = U1.USER_ID AND F.USER2_ID = U2.USER_ID)"
    			+ "UNION SELECT DISTINCT U2.USER_ID, U1.USER_ID FROM "
    			+ userTableName + " U1, " + userTableName + " U2, " + tagTableName + " T1, "
    			+ tagTableName + " T2 WHERE U1.GENDER != U2.GENDER AND U2.GENDER = 'male' "
    			+ "AND (U2.YEAR_OF_BIRTH - U1.YEAR_OF_BIRTH) <= " + yearDiff + " AND (U2.YEAR_OF_BIRTH - U1.YEAR_OF_BIRTH) >= 0 "
    			+ "AND (T1.TAG_SUBJECT_ID = U1.USER_ID AND T2.TAG_SUBJECT_ID = U2.USER_ID) "
    			+ "AND T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID AND (U1.USER_ID, U2.USER_ID) NOT IN"
    			+ "(SELECT U1.USER_ID, U2.USER_ID FROM " + userTableName + " U1, " + userTableName + " U2, "
    			+ friendsTableName + " F WHERE F.USER1_ID = U1.USER_ID AND F.USER2_ID = U2.USER_ID "
    			+ "UNION SELECT U2.USER_ID, U1.USER_ID FROM " + userTableName + " U1, " + userTableName + " U2, "
    			+ friendsTableName + " F WHERE F.USER1_ID = U1.USER_ID AND F.USER2_ID = U2.USER_ID)) "
    			+ "GROUP BY U1.USER_ID, U2.USER_ID ORDER BY COUNT(P.PHOTO_ID) DESC, U2.USER_ID, U1.USER_ID";
    	//get user info
    	String query2;
    	// get picture info
    	String query3;
    	
    	try(Statement stmt = 
    		oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
    			ResultSet.CONCUR_READ_ONLY)) {
    		ResultSet rs = stmt.executeQuery(query);
    		
    		//for top n matches find info
    		int i = 0;
    		while (i < n && rs.next()){
    			long id1 = rs.getLong(1);
    			long id2 = rs.getLong(2);
        		query2 = "SELECT U1.*, U2.* FROM " + userTableName + " U1, " + userTableName + " U2 "
        				+ " WHERE U1.USER_ID = " + id1 + " AND U2.USER_ID = " + id2;
        		
        		try(Statement stmt2 = 
        	    		oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        	    			ResultSet.CONCUR_READ_ONLY)) {
        	    		ResultSet rs2 = stmt2.executeQuery(query2);
        	    		
        	    		//findsinfo on pair
        	    		rs2.next();
        	    		
        	    		MatchPair mp = new MatchPair(rs2.getLong(8), rs2.getString(9), rs2.getString(10),
        	    				rs2.getInt(11), rs2.getLong(1), rs2.getString(2), rs2.getString(3), rs2.getInt(4));	
        	    		
        	    		//find all pictures of pair
        	    		query3 = "SELECT P.*, A.ALBUM_NAME FROM "
        	    				+ userTableName + " U1, " + userTableName + " U2, " + tagTableName + " T1, "
        	        			+ tagTableName + " T2, " + photoTableName + " P, " + albumTableName + " A "
        	        			+ "WHERE U1.USER_ID = " + id1
        	        			+ " AND U2.USER_ID = " + id2 + " AND U1.USER_ID = T1.TAG_SUBJECT_ID "
        	        			+ "AND U2.USER_ID = T2.TAG_SUBJECT_ID AND T1.TAG_PHOTO_ID = P.PHOTO_ID "
        	        			+ "AND T2.TAG_PHOTO_ID = P.PHOTO_ID AND A.ALBUM_ID = P.ALBUM_ID";
        	    		
        	    		try(Statement stmt3 = 
                	    		oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                	    			ResultSet.CONCUR_READ_ONLY)) {
                	    	
        	    			ResultSet rs3 = stmt3.executeQuery(query3);
                	    		
        	    			while(rs3.next()){
        	    				mp.addSharedPhoto(new PhotoInfo(rs3.getString(1), rs3.getString(2),
                	                    rs3.getString(7), rs3.getString(3), rs3.getString(6)));
        	    			}
        	    			
        	    			this.bestMatches.add(mp);
                	    } catch (SQLException b) {
                	    		System.err.println(b.getMessage());
                	    }
        	    		
        	    } catch (SQLException c) {
        	    		System.err.println(c.getMessage());
        	    }
        		i++;
    		}
    	} catch (SQLException e) {
    		System.err.println(e.getMessage());
    	}
    }

    // **** Query 6 ****
    // Suggest users based on mutual friends
    //
    // Find the top n pairs of users in the database who have the most
    // common friends, but are not friends themselves.
    //
    // Your output will consist of a set of pairs (user1_id, user2_id)
    // No pair should appear in the result twice; you should always order the pairs so that
    // user1_id < user2_id
    //
    // If there are ties, you should give priority to the pair with the smaller user1_id.
    // If there are still ties, give priority to the pair with the smaller user2_id.
    
    @Override
    public void suggestFriendsByMutualFriends(int n) {
    	
    	String query = "SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, "
    			+ "U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME  FROM "
    			+ userTableName + " U1, " + userTableName + " U2, "
    			+ userTableName + " U3, " + friendsTableName + " F1, "  
    			+ friendsTableName + " F2 WHERE U1.USER_ID = F1.USER1_ID"
				+ " AND F1.USER2_ID = U3.USER_ID AND U2.USER_ID = F2.USER1_ID"
				+ " AND F2.USER2_ID = U3.USER_ID AND U1.USER_ID < U2.USER_ID "
				+ "AND U1.USER_ID != U3.USER_ID AND U2.USER_ID != U3.USER_ID"
				+ " AND (U1.USER_ID, U2.USER_ID) NOT IN (SELECT U1.USER_ID, U2.USER_ID FROM "
				+ userTableName + " U1, " + userTableName + " U2, " + friendsTableName + " F "
				+ "WHERE F.USER1_ID = U1.USER_ID AND F.USER2_ID = U2.USER_ID UNION"
				+ " SELECT U2.USER_ID, U1.USER_ID FROM "
				+ userTableName + " U1, " + userTableName + " U2, " + friendsTableName + " F "
				+ "WHERE F.USER1_ID = U1.USER_ID AND F.USER2_ID = U2.USER_ID)"
				+ " GROUP BY (U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, "
				+ "U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME) "
				+ "ORDER BY COUNT(U3.USER_ID) DESC, U1.USER_ID, U2.USER_ID";
    	
    	String query2;
    	
    	try(Statement stmt = 
        		oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        			ResultSet.CONCUR_READ_ONLY)) {
        		ResultSet rs = stmt.executeQuery(query);
        		
        		int i = 0;
        		while(rs.next() && i < n){
        			UsersPair p = new UsersPair(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getLong(4), rs.getString(5), rs.getString(6));
        			
        			query2 = "SELECT U3.USER_ID, U3.FIRST_NAME, U3.LAST_NAME FROM "
        					+ userTableName + " U1, " + userTableName + " U2, "
        	    			+ userTableName + " U3, " + friendsTableName + " F1, "  
        	    			+ friendsTableName + " F2 WHERE F1.USER1_ID = U1.USER_ID "
        	    			+ "AND F1.USER2_ID = U3.USER_ID AND F2.USER1_ID = U2.USER_ID"
        	    			+ " AND F2.USER2_ID = U3.USER_ID AND ((U1.USER_ID = " + rs.getLong(1) 
        	    			+ " AND U2.USER_ID = " + rs.getLong(4) + ")"
        	    			+ " OR (U2.USER_ID = " + rs.getLong(4) + " AND U1.USER_ID = " + rs.getLong(1) + "))";
        			
        			//for each user find all mutual friends thhrough new db connection
        			try(Statement stmt2 = 
        	        		oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        	        			ResultSet.CONCUR_READ_ONLY)) {
        	        		ResultSet rs2 = stmt2.executeQuery(query2);
        	        		
        	        		while(rs2.next()){
        	        			p.addSharedFriend(rs2.getLong(1), rs2.getString(2), rs2.getString(3));
        	        			
        	        		}
        	        		this.suggestedUsersPairs.add(p);
        	        		
        	       	} catch (SQLException c) {
        	       		System.err.println(c.getMessage());
        	   	    }
        	        i++;
        		}
       	} catch (SQLException e) {
       		System.err.println(e.getMessage());
   	    }
    }

    @Override
    // ***** Query 7 *****
    //
    // Find the name of the state with the most events, as well as the number of
    // events in that state.  If there is a tie, return the names of all of the (tied) states.
    //
    public void findEventStates() {
    	String query = "SELECT COUNT(E.EVENT_ID), C.STATE_NAME FROM "
    			+ eventTableName + " E, " + cityTableName + " C WHERE "
    			+ "E.EVENT_CITY_ID = C.CITY_ID GROUP BY C.STATE_NAME "
    			+ "ORDER BY COUNT(E.EVENT_ID) DESC";
    	try(Statement stmt = 
        		oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        			ResultSet.CONCUR_READ_ONLY)) {
        		ResultSet rs = stmt.executeQuery(query);
        		
        		rs.next();
        		int max = rs.getInt(1);
        		this.eventCount = max;
        		this.popularStateNames.add(rs.getString(2));
        		
        		while(rs.next() && rs.getInt(1) == max){
        			this.popularStateNames.add(rs.getString(2));
        		}
        		
       	} catch (SQLException e) {
       		System.err.println(e.getMessage());
   	    }
    }

    @Override
    // ***** Query 8 *****
    // Given the ID of a user, find information about that
    // user's oldest friend and youngest friend
    //
    // If two users have exactly the same age, meaning that they were born
    // on the same day, then assume that the one with the larger user_id is older
    //
    public void findAgeInfo(Long user_id) {
    	String query = "SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, "
    			+ "U1.YEAR_OF_BIRTH, U1.MONTH_OF_BIRTH, U1.DAY_OF_BIRTH FROM "
    			+ userTableName + " U1, " + userTableName + " U2, "
    			+ friendsTableName + " F WHERE U1.USER_ID = F.USER1_ID "
    			+ "AND F.USER2_ID = U2.USER_ID AND U2.USER_ID = " + user_id
    			+ " UNION SELECT U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME, "
    			+ "U2.YEAR_OF_BIRTH, U2.MONTH_OF_BIRTH, U2.DAY_OF_BIRTH FROM "
    			+ userTableName + " U1, " + userTableName + " U2, "
    			+ friendsTableName + " F WHERE U1.USER_ID = F.USER1_ID"
    			+ " AND F.USER2_ID = U2.USER_ID AND U1.USER_ID = " + user_id
    			+ " ORDER BY YEAR_OF_BIRTH, MONTH_OF_BIRTH, DAY_OF_BIRTH, USER_ID DESC";
    	
    	try(Statement stmt = 
        		oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        			ResultSet.CONCUR_READ_ONLY)) {
        		ResultSet rs = stmt.executeQuery(query);
        		
        		rs.next();
                this.oldestFriend = new UserInfo(rs.getLong(1), rs.getString(2), rs.getString(3));
        		
                rs.last();
                this.youngestFriend = new UserInfo(rs.getLong(1), rs.getString(2), rs.getString(3));
 
       	} catch (SQLException e) {
       		System.err.println(e.getMessage());
   	    }
    }

    @Override
    //	 ***** Query 9 *****
    //
    // Find pairs of potential siblings.
    //
    // A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
    // if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
    // on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
    
    public void findPotentialSiblings() {
    	String query = "SELECT U1.*, U2.* FROM "
    			+ userTableName + " U1, " + userTableName + " U2, "
    			+ friendsTableName + " F, " + hometownCityTableName + " C1, "
    			+ hometownCityTableName + " C2 WHERE U1.USER_ID = F.USER1_ID "
    			+ "AND U2.USER_ID = F.USER2_ID AND U1.LAST_NAME = U2.LAST_NAME "
    			+ "AND U1.USER_ID = C1.USER_ID AND U2.USER_ID = C2.USER_ID "
    			+ "AND C1.HOMETOWN_CITY_ID = C2.HOMETOWN_CITY_ID "
    			+ "AND (U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH < 10) "
    			+ "AND (U2.YEAR_OF_BIRTH - U1.YEAR_OF_BIRTH < 10)"
    			+ " ORDER BY U1.USER_ID, U2.USER_ID";
    	try(Statement stmt = 
        		oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        			ResultSet.CONCUR_READ_ONLY)) {
        		ResultSet rs = stmt.executeQuery(query);
        		
        		while(rs.next()){
        	        SiblingInfo s = new SiblingInfo(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getLong(8), rs.getString(9), rs.getString(10));
        	        this.siblings.add(s);
        		}
       	} catch (SQLException e) {
       		System.err.println(e.getMessage());
   	    }
    }
}
