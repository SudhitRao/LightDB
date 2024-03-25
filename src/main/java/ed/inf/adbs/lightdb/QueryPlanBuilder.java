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

public class QueryPlanBuilder {

    private PlainSelect statement;

    public static Map<String, String> aliasMap = new HashMap<>();

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

    private Operator handleDistinct(Operator root) {
        if (statement.getDistinct() != null) {
            return new DuplicateEliminationOperator(root);
        }
        return root;
    }

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

    private Operator processSortOperator(Operator op) {
        if (statement.getOrderByElements() != null) {
            return new SortOperator(op, statement.getOrderByElements());
        }
        return op;
    }

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


