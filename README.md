
# Lightweight Database Management System

## Parsing Tree Logic

To extract the join and selection conditions from the where clause, I used the following strategy. I created a visitor class to visit the where expression. Inside the class, I created two dictionary, one called joinConditions and another called selectionConditions that both mapped a string to a list of expressions. To decipher whether part of the expression belonged in the joinConditions dictionary or not, I checked to see if the subexpression contained two distinct column references or just references from a single column. If this was true, I appended the expression to the list of expressions corresponding to key "col1_col2" and "col2_col1" in joinConditions. If it did not, then I appended the expression to selectionConditions to the key "col". 

Lastly, I also iterated through the tableNames, to find all the tables to be joined using cross product (since there would not be a where expression in this case). 


## Query Optimization Logic

First, I created the left deep join tree. I made sure to put any selection conditions that eliminated any tuples underneath the actual join operators so I can limit the number of tuples inside the join operator. After the join operator, the next logical operator to add is the groupby/sum operator. This is because it severely decreases the number of tuples by grouping. After grouping, I was able to project and eliminate all columns that do not show up in the select part of the query. Then the distinct operator made the most sense, to further eliminate tuples. Lastly, since sorting logically takes the most time (O(n log n)) this was done last. 
