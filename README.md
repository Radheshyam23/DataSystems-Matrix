# Data Systems Project - Phase 2 - Team 41
This project aims to further expand the functionality of [SimpleRA](https://github.com/SimpleRA/SimpleRA).

In this phase, we have worked on and implemented a methodology to sort the data files. Since the relations are often huge, they are stored across multiple pages, hence, we implemented 'external sorting' to sort the relation. External sorting was then used to implement ORDER BY, GROUP BY and JOIN. We will look at each of their implementations in detail.

### `SORT <table_name> BY <column_name1, column_name2,..., column_namek> IN <ASC|DESC, ASC|DESC,..., ASC|DESC>`

Given a relation R(K, A1, … An), we need to sort it based on the mentioned columns in the specified ordering. We will look at the implementation step by step.

1. Query Parsing and storing
The query is read word by word and all the column names are stored in a vector called `sortColumnList` and their corresponding sorting method is stored in a vector called `sortTypeList`. Based on the column names, we obtain the column indices by comparing with the table and store it in another vector, `sortColumnIndices`. This vector has the column indices, and in the order of precedence.

2. Internal Sorting
If the relation fits into one page, then sorting is trivially just sorting of that one page. If there are however, multiple pages, then we use external sorting. The first step in this, is to sort each page internally. To perform sorting of each page, we load the contents of the page into a vector, then use the c++ inbuilt sort function. We built a custom comparator function, called `sortComparator()` which we pass to the sort function to explain the process. This function takes two records as an input, and compares the columns mentioned in the `sortColumnIndex` and tells teh sort function which record is to come before the other. Once the vector is sorted, we write back the vector into that page and move to the next page. We do this for all the pages, hence, by the end of this step, we are sure that all the pages are internally sorted.

3. Merge step - Initialisation
The next step to be done is to now merge these sorted files. We have a buffer size of 10 pages. Hence, we keep 9 pages open in the read mode, and create another page for writing. We maintain a priority queue which contains the topmost row (which we have not yet processed) from each open page (9 pages), and the corresponding page number.

4. Merge Step - Next smallest row
Since we have a priority queue, we obtain the smallest row from all these 9 open pages. Say that the smallest row was from page i. We then write it to the write page (the 10th page). Now, we move to the next row in page i and push that to the priority queue.


5. Merge Step - Write Page full
Once the write page gets filled up, we store it in memory, and load a new blank page for writing.

6. Merge Step - All Read Page completed
Once all the 9 pages which we had opened are fully read, we would have stored 9 write pages in the memory, and these 9 write pages are now sequentially sorted. i.e. the last row of page i-1 will be before the 1st row of page i. Once all the pages are completed, we delete these read pages, and overwrite it by the written pages (basically renaming).

7. Subsequent steps:
This happens at two levels:
- First, we do it for all groups of 9 pages. At the end of this, we will have chunks of consecutive sorted pages, 0-8, 9-17, 18-26, ...
- Now, we level it up and almost treat one chunk as one page and repeat the process. So in the next level iteration, we will load the first page of each chunk in the priority queue, so the queue will have pages 0, 9, 18, ...
When say page 9 is fully read, we move to page 10, as if we were just moving to the next record in page 9. (in effect, we remvoe 9 from buffer and add 10 to buffer).
Hence, at the end of this iteration, we will have pages 0 to 89 sorted as one chunk, and so on. If we have more pages, we go for yet another level.

The total number of levels will be:
`order_level = ceil(log(numBlocks)/log(poolSize));`

### `<new_table> <- ORDER BY <attribute> ASC|DESC ON <table_name>`
ORDER BY command is supposed to sort the relation based on the ordering column and store it into a new relation.
For the implementation of this, we first created a new table and copied the records from the original table into this new table. Then, we perform a SORT operation on the new table based on the specifications in the query. Since ORDER BY is effectively a SORT operation on a single column, the SORT command is utilized and thus a new table is made.

### `<new_relation_name> <- JOIN <tablename1>, <tablename2> ON <column1> <bin_op> <column2>`

JOIN command is used to join two relations based on the specified column in the query. The algorithm used to implement this was mostly similar to the one mentioned in the Elmasri and Navathe textbook (called "sort-merge join" algorithm), although slight changes were made to account for the fact that the columns to be joined may not necessarily be primary keys.

The actual implementation of the algorithm is relatively straightforward : we first sort the two relations based on the columns to be joined, and then we merge the two relations based on the sorted columns. The sorting is done using the SORT that we implemented in the Task 1 of this project phase. We then use a "two pointer approach" to merge the two relations. The two pointers are initialized to the first record of each relation, and then we compare the values of the columns to be joined. In case of an equi-join, if the values are equal, we add the two records to the new relation and increment both pointers. If the value of the first record is less than the value of the second record, we increment the first pointer. If the value of the second record is less than the value of the first record, we increment the second pointer. We continue this process until we reach the end of either relation. 

Naturally, appropriate changes had to be made in the case of non-equi joins depending on the "bin_op" operator mentioned in the query. Also, as mentioned before, changes were made to accomodate for the fact that the column to be joined on can also be a non-primary key, and thus, there is a possibilty of repeated values.

### `<new_table> <- GROUP BY <grouping_attribute> FROM <table_name> HAVING <aggregate(attribute)> <bin_op> <attribute_value> RETURN <aggregate_func(attribute)>`


## Assumptions

- The requirements document allows us a usage of pool size for the buffer as '10'. We have made this change accordingly in server.cpp `uint BLOCK_COUNT = 10`;
- The order of preference for sorting is based on the order in which the columns names were mentioned in the query. i.e. if we say SORT BY B,A,C... then we first sort by column B. If there is any record with same value for B, we then sort those records by A, and then followed by C and so on... This is how the requriement was mentioned.

## Learnings

- The logger proved to be invaluable when debugging code. It enabled us to notice bugs when trying out implementations and allowed us to navigate to the code parts that gave us trouble.

- Using functions such as syntactic parse and semantic parse in separate files allows for a more modular code rather than having a low number of files with too much code to go through.

- Algorithmically, sorting is just a simple call of a function which can perform a O(nlogn) sort. However, this project made us understand the depths of large data managing, and better understand the memory limitations of our computers.

- Special considerations must be made to account for operations that use different operations when executing. For example, the SORT logic is used across most of the commands implemented in Task 2 and therefore it was important to make sure the SORT logic was correctly implemented.

## Member Contributions

- Sudha Tanay Doddi [2020115010]
    - Documentation and Report Making
    - Debugging
    - EXTERNAL SORT
    - ORDER BY


- Radheshyam Thiyagarajan [2020115009]
    - Documentation and Report Making
    - Debugging
    - EXTERNAL SORT
    - ORDER BY

- Pranjal Thapliyal [2020101108]
    - Documentation and Report Making
    - Debugging
    - JOIN
    - GROUP BY