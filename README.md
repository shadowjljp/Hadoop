#Hadoop map reduce to handle incoming data
1. The classic “Mutual/Common friend list of two friends".
2. information extraction
3. Find friend pair(s) whose number of common friends is the maximum in all the pairs. 

Output Format:
<User_A>, <User_B><TAB><Mutual/Common Friend Number>
4. Use in-memory join at the Mapper to output the list of the names and the date of birth (mm/dd/yyyy) of their mutual friends.
5. Use in-memory join at the Reducer to print For each user print User ID and maximum age of direct friends of this user.
6. Construct inverted index in the following ways: <word, line number> pairs.
