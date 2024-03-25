package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

/*
 * Class to build a query plan from a given parsed SQL statement. The strategy to extract join
 * conditions and create the join tree and build the query is explained in the README file. 
 */

public class QueryPlanBuilder {

    private PlainSelect statement;

    public static Map<String, String> aliasMap = new HashMap<>();


    /*
     * build a query plan for a given statement
     * @param statement The parsed SQL query in a statement object
     * @return the root operator to finally execute
     */
    public Operator buildQueryPlan(Statement statement) throws Exception {
        //Statement statement = CCJSqlParserUtil.parse(sql);
        if (statement instanceof Select) {
            this.statement = (PlainSelect) statement;
            Operator root = processPlainSelectWithJoin();
            //return root;
            root = processSumOperator(root); // conduct groupby operations
            root = processProjectOperator(root); // project
            root = handleDistinct(root); // remove distinct
            return processSortOperator(root); // finally sort
        }
        throw new IllegalArgumentException("Query type must be Select");
    }

    /*
     * given an operator, conduct projection if necessary
     * @param root operator to handle
     * @return the operator after projection has been applied
     */
    private Operator processProjectOperator(Operator root) {
        List<SelectItem<?>> arr = statement.getSelectItems(); //guaranteed to be length at least 1 I think
        if (arr.size() == 1 && arr.get(0).getExpression().toString().equals("*")) {
            return root;
        }
        if (arr.get(0).getExpression().toString().equals("*")) {
            List<String> tableList = extractTables();
            List<String> toReturn = new ArrayList<>();
            for (String table : tableList) {
                // do something
                for (String col : DataCatalog.getInstance().getSchema(table)) {
                    toReturn.add(aliasMap.get(table) + "." + col);
                }
            }
            toReturn.add(arr.get(arr.size() - 1).getExpression().toString());
            return new ProjectOperator(root, toReturn, true);
        }
        return new ProjectOperator(root, arr);
    }

    /*
     * Given an SQL query, extract all the tables and populate alias table
     * @return list of tables as a string
     */
    public List<String> extractTables() {
        List<String> tables = new ArrayList<>();
        tables.add(statement.getFromItem().toString());
			if (statement.getJoins() != null) {
				for (Join j : statement.getJoins()) {
					tables.add(j.toString());
				}
			}

        if (!tables.get(0).contains(" ")) {
            for (String table : tables) {
                aliasMap.put(table, table);
            }
        return tables;
        }

        List<String> toReturn = new ArrayList<>();
        
        for (String table : tables) {
            String alias = table.split(" ")[1];
            String tableName = table.split(" ")[0];
            aliasMap.put(alias, tableName);
            toReturn.add(alias);
        }


        return toReturn;
    }

    /*
     * handle distinct operator if necessary
     * @param root the operator to handle
     * @return operator after distinct has been applied
     */
    private Operator handleDistinct(Operator root) {
        if (statement.getDistinct() != null) {
            return new DuplicateEliminationOperator(root);
        }
        return root;
    }

    /*
     * process groupby operator, find all the columns to groupby and create groupby op if necessary
     * @param op child operator to handle
     * @return operator with or without sum operator
     */
    private Operator processSumOperator(Operator op) {
        List<SelectItem<?>> items = statement.getSelectItems();
        SelectItem<?> item = items.get(items.size() - 1);
		Expression exp = item.getExpression();
        List<String> groupByStrings = new ArrayList<>();
        if (statement.getGroupBy() != null) {
            for (Object expression : statement.getGroupBy().getGroupByExpressionList().toArray()) {
                groupByStrings.add(expression.toString());
            }
        }
        // if (!(exp instanceof Function || groupByStrings.size() == 0)) {
        //     return op;
        // }
        if (! (exp instanceof Function)) {
            if (groupByStrings.size() == 0) {
                return op;
            }
        }
        
        //System.out.println("SUM operator");
        List<String> productList = null;
        if (exp instanceof Function) {
            Function func = (Function) exp;
            productList = Arrays.asList(func.getParameters().toString().split("\\*"));
            for (int i = 0; i < productList.size(); i++) {
                productList.set(i, productList.get(i).replaceAll("\\s+", ""));
            }
            return new SumOperator(op, groupByStrings, productList, exp.toString());
        } else {
            return new SumOperator(op, groupByStrings, null, null);
        }
        
    }

    /*
     * process sorting operator, do sorting if necessary
     * @param op child operator of sort 
     * @return operator with or without sorting operator
     */
    private Operator processSortOperator(Operator op) {
        if (statement.getOrderByElements() != null) {
            return new SortOperator(op, statement.getOrderByElements());
        }
        return op;
    }

    /*
     * modify the join conditions to include cross products by accessing the order of table names
     * @param tableNames the table names in order in which they appear
     * @param joinConditions join conditions to modify
     */
    private void modifyJoinConditions(List<String> tableNames, Map<String, List<Expression>> joinConditions) {
        for (int i = 0; i < tableNames.size() - 1; i++) {
            if (!joinConditions.containsKey(tableNames.get(i) + "_" + tableNames.get(i + 1))) {
                joinConditions.putIfAbsent(tableNames.get(i) + "_" + tableNames.get(i + 1), new ArrayList<>());
                joinConditions.get(tableNames.get(i) + "_" + tableNames.get(i + 1)).add(null);
                joinConditions.putIfAbsent(tableNames.get(i + 1) + "_" + tableNames.get(i), new ArrayList<>());
                joinConditions.get(tableNames.get(i + 1) + "_" + tableNames.get(i)).add(null);
            }
        }
    }

    /*
     * Given the statement, process all scan, selection, and join operators.
     * All these done together since they are generally at the bottom of the tree
     * @return operator at the root of left deep join tree
     */
    private Operator processPlainSelectWithJoin() {
        List<String> tableNames = extractTables();
        ProcessWhereExpression processor = new ProcessWhereExpression();
        Expression where = statement.getWhere();
        if (where != null) {
            where.accept(processor);
        }
        Map<String, List<Expression>> joinConditions = processor.getJoinConditions();
        modifyJoinConditions(tableNames, joinConditions);
        Map<String, List<Expression>> selectionConditions = processor.getSelectionConditions();
        Map<String, Operator> opMap = new HashMap<>();
        for (String table : tableNames) {
            Operator cur = new ScanOperator(aliasMap.get(table), table);
            if (selectionConditions.containsKey(table)) {
                for (int i = 0; i < selectionConditions.get(table).size(); i++) {
                    cur = new SelectOperator(cur, selectionConditions.get(table).get(i));
                }
            }
            opMap.put(table, cur);
        }
        if (tableNames.size() == 1) {
            return opMap.get(tableNames.get(0));
        } else {
            String first = tableNames.get(0);
            String second = tableNames.get(1);
            Operator root = new JoinOperator(opMap.get(first), 
                opMap.get(second), joinConditions.get(first + "_" + second));
            for (int i = 2; i < tableNames.size(); i++) {
                String firstTable = "";
                String secondTable = tableNames.get(i);
                for (int j = 0; j < i; j++) {
                    firstTable = tableNames.get(j);
                    if (joinConditions.containsKey(firstTable + "_" + secondTable)) {
                        root = new JoinOperator(root, opMap.get(secondTable), 
                            joinConditions.get(firstTable + "_" + secondTable));
                    }
                }
            }
        return root;   
        }
    }
}


