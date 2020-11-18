# SimpleDB

SimpleDB is a basic database system, including heapfiles, basic operators(scan, filter, join, etc.), buffer pool and catalog. It serves as semester programming project for CS 660 (Graduate Introduction to Database System) at Boston University. The code skeleton is provided.

### Part 1 ###
Implement the core modules required to access stored data on disk.     
1. Implement Tuples   :heavy_check_mark:
2. Implement Catalog   :heavy_check_mark:
3. Implement Buffer Pool  :heavy_check_mark:
4. Implement Heap Page   :heavy_check_mark:
5. Implement Heap File   :heavy_check_mark:
6. Implement Operator (Seq Scan)   :heavy_check_mark:

### Part 2 ###
Implement the page replacement procedure in the buffer and a B+ tree index for efficient lookups and range scans.            
1. Implement page eviction method in BufferPool   :heavy_check_mark:
2. Search in B+ tree     :heavy_check_mark:
3. Insert in B+ tree     :heavy_check_mark:
4. Delete in B+ tree     :heavy_minus_sign:

### Part 3 ###
Write a set of operators for SimpleDB to implement table modifications (e.g., insert and delete records), selections, joins, and aggregates.
1. Implement Filter & Join    :heavy_check_mark:
2. Implement Aggregate    :heavy_check_mark:
3. HeapFile Mutability    :heavy_check_mark:
4. Implement Insertion & Deletion    :heavy_check_mark:

### Part 4 ###
Implement a query optimizer on top of SimpleDB.
1. Record table statistics for selectivity estimation    :heavy_check_mark:
2. Table stats    :heavy_check_mark: <font color=#008000>No code changed, but all tests passed</font>
3. Implement methods for estimating the selectivity and cost of a join
4. Join Ordering

## ✍️ Collaborators ##
- [@CaptainDra](https://github.com/CaptainDra)
- [@LyapunovJingci](https://github.com/LyapunovJingci)

