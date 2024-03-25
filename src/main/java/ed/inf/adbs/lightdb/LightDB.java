package ed.inf.adbs.lightdb;



import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;


/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		// Just for demonstration, replace this function call with your logic
		//DataCatalog.initialize(databaseDir);
		//parsingExample(inputFile);
		//catalogExample(databaseDir);
		//scanExample(databaseDir);
		//selectExample(databaseDir);
		//projectExample(databaseDir);
		queryExample(databaseDir, inputFile, outputFile);
		//joinExample(databaseDir);
		//joinParserExample(outputFile);
		//aliasExample(databaseDir);
		//orderByExample(databaseDir);
		//extractTables("SELECT * FROM Sailors S, Reserves R ORDER BY S.f");
		//distinctExample();
		//groupByExample();
		//projectExample(outputFile);
	}

	public static void groupByExample() {
		//String eval = "SELECT SUM(Sailors.A) FROM Sailors, Reserves;";
		//String eval = "SELECT * FROM Reserves GROUP BY Reserves.G";
		//String eval = "SELECT * FROM Boats GROUP BY Boats.E";
		//String eval = "SELECT SUM(1) FROM Sailors GROUP BY Sailors.B;";
		//String eval = "SELECT SUM(Sailors.A) FROM Sailors, Reserves;";
		//String eval = "SELECT Reserves.H, SUM(Reserves.G * Reserves.G) FROM Reserves GROUP BY Reserves.H;";
		//String eval = "SELECT * FROM Sailors GROUP BY Sailors.B, Sailors.C";
		String eval = "SELECT Sailors.B, Sailors.C FROM Sailors, Reserves WHERE Sailors.A = Reserves.G GROUP BY Sailors.B, Sailors.C ORDER BY Sailors.C, Sailors.B;";
		//String eval =  "SELECT SUM(Sailors.A) FROM Sailors, Reserves";
		try {
			Statement statement = CCJSqlParserUtil.parse(eval);
			PlainSelect select = (PlainSelect) statement;
			List<String> groupByStrings = new ArrayList<>();
			if (select.getGroupBy() != null) {
				for (Object exp : select.getGroupBy().getGroupByExpressionList().toArray()) {
					groupByStrings.add(exp.toString());
				}
			}
			List<SelectItem<?>> items = select.getSelectItems();
			SelectItem<?> item = items.get(items.size() - 1);
			Expression exp = item.getExpression();
			List<String> productList = null;
			if (exp instanceof Function) {
				Function func = (Function) exp;
				productList = Arrays.asList(func.getParameters().toString().split("\\*"));
				for (int i = 0; i < productList.size(); i++) {
					productList.set(i, productList.get(i).replaceAll("\\s+", ""));
				}
			}
			System.out.println(groupByStrings);
			System.out.println(productList);
			System.out.println(exp.toString());


			//TEST 1
			//ScanOperator scan = new ScanOperator("Boats", "Boats");
			// ScanOperator scan = new ScanOperator("Sailors", "Sailors");
			// List<String> p = new ArrayList<>();
			// p.add("1");
			// SumOperator root = new SumOperator(scan, groupByStrings, p, "SUM(1)");
			// root.dump()

			// TEST 2
			// ScanOperator scan = new ScanOperator("Reserves", "Reserves");
			// List<String> p = new ArrayList<>();
			// p.add("Reserves.G");
			// p.add("Reserves.G");
			// SumOperator root = new SumOperator(scan, groupByStrings, p, "SUM(Reserves.G * Reserves.G)");
			// root.dump();

			//System.out.println(select.getGroupBy().getGroupByExpressionList());
			//System.out.println(select.getSelectItems().get(0).getExpression());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void distinctExample() {
		//String eval = "SELECT DISTINCT R.G FROM Reserves R;";
		String eval = "SELECT R.G FROM Reserves R;";
		try {
			Statement statement = CCJSqlParserUtil.parse(eval);
			PlainSelect select = (PlainSelect) statement;
			System.out.println(select.getDistinct());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void orderByExample(String filename) {
		//String eval = "SELECT * FROM Sailors ORDER BY Sailors.B, Sailors.C;";
		String eval = "SELECT * FROM Sailors";
		try {
			Statement statement = CCJSqlParserUtil.parse(eval);
			if (statement != null) {
				PlainSelect select = (PlainSelect) statement;
				List<OrderByElement> orderList = select.getOrderByElements();
				System.out.println(orderList);
				for (OrderByElement elem : orderList) {
					System.out.println(elem.toString());
				}
				ScanOperator scanOperator = new ScanOperator("Sailors", "Sailors");
				SortOperator sortOperator = new SortOperator(scanOperator, orderList);
				sortOperator.dump();
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

	public static void aliasExample(String filename) {
		//String eval = "SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A";
		String eval = "SELECT * FROM R, S, T WHERE R.A = 1 AND R.B >= S.C AND T.G < 5 AND T.G = S.H";
		//List<String> tables = QueryPlanBuilder.extractTables(eval);
		//System.out.println(tables);
		//System.out.println(QueryPlanBuilder.aliasMap);
	} 

	public static void joinParserExample(String fileName) {
		//String eval = "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G";
		//String eval = "SELECT * FROM R, S, T WHERE R.A = 1 AND R.B >= S.C AND T.G < 5 AND T.G = S.H";
		String eval = "SELECT * FROM R WHERE R.A > R.B AND R.C = R.D";
		//String eval = "SELECT * FROM R, S";
		try {
			//String eval = "SELECT * FROM Sailors";
			Statement statement = CCJSqlParserUtil.parse(eval);
			if (statement != null) {
				PlainSelect select = (PlainSelect) statement;
				ProcessWhereExpression processor = new ProcessWhereExpression();
				Expression where = select.getWhere();
				where.accept(processor);
				Map<String, List<Expression>> joinConditions = processor.getJoinConditions();
        		Map<String, List<Expression>> selectionConditions = processor.getSelectionConditions();
				System.out.println(joinConditions);
				System.out.println(selectionConditions);
				System.out.println(extractTables(eval));
				//System.out.println(select.getF);
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

	public static List<String> extractTables(String sql) {
        List<String> tableNames = null;
        try {
            // Parse the SQL statement
            Statement statement = CCJSqlParserUtil.parse(sql);
		 	tableNames = new ArrayList<>();
            
            // Use TablesNamesFinder to find all table names within the statement
            PlainSelect select = (PlainSelect) statement;
			tableNames.add(select.getFromItem().toString());
			if (select.getJoins() != null) {
				for (Join j : select.getJoins()) {
					tableNames.add(j.toString());
				}
			}
			System.out.println(tableNames);
			return tableNames;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return tableNames;
    }

	public static void joinExample(String filename) {
		//String eval = "SELECT * FROM Sailors WHERE Sailors.C >= 200 AND Sailors.C <= 500";
		String eval = "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G";
		//String eval = "SELECT * FROM Sailors, Boats, Reserves";
		//String eval = "SELECT * FROM Sailors, Reserves";
		try {
			//String eval = "SELECT * FROM Sailors";
			Statement statement = CCJSqlParserUtil.parse(eval);
			if (statement != null) {
				System.out.println("Read statement: " + statement);
				PlainSelect select = (PlainSelect) statement;
				System.out.println(select.getWhere());
				List<Join> joins = select.getJoins();
				System.out.println(joins);
				ScanOperator scan1 = new ScanOperator("Sailors", "Sailors");
				ScanOperator scan2 = new ScanOperator("Reserves", "Reserves");
				JoinOperator root = new JoinOperator(scan1, scan2, select.getWhere());
				//scan1.dump();
				//scan2.dump();
				root.dump();
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

	public static void queryExample(String databaseDir, String inputFile, String outputFile) {
		//String eval = "SELECT * FROM Sailors";
		//String eval = "SELECT Sailors.C, Sailors.A FROM Sailors WHERE Sailors.C >= 200 AND Sailors.C <= 500";
		//String eval = "SELECT * FROM Sailors WHERE Sailors.C >= 200 AND Sailors.C <= 500";
		//String eval = "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G";
		//String eval = "SELECT * FROM Sailors, Boats, Reserves";
		//String eval = "SELECT * FROM Sailors, Reserves";
		//String eval = "SELECT * FROM Sailors, Boats, Reserves WHERE Sailors.A = Boats.F AND Reserves.G = Sailors.A";
		//String eval = "SELECT Sailors.A, Boats.F, Reserves.G FROM Sailors, Boats, Reserves WHERE Sailors.A = Boats.F AND Reserves.G = Sailors.A";
		//String eval = "SELECT S.A FROM Sailors S";
		//String eval = "SELECT * FROM Sailors S ORDER BY S.B, S.C";
		//String eval = "SELECT * FROM Sailors ORDER BY Sailors.B";
		//String eval = "SELECT DISTINCT R.G FROM Reserves R;";
		String eval = "SELECT S.A, B.F, R.G FROM Sailors S, Boats B, Reserves R WHERE S.A = B.F AND R.G = S.A";


		//Interpreter interpreter = new Interpreter(databaseDir, eval, outputFile, false);
		Interpreter interpreter = new Interpreter(databaseDir, inputFile, outputFile, true);
		try {
			interpreter.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void projectExample(String filename) {
		try {
			//String eval = "SELECT Sailors.C, Sailors.A FROM Sailors WHERE Sailors.C >= 200 AND Sailors.C <= 500";
			//String eval = "SELECT * FROM Sailors";
			String eval = "SELECT Reserves.H, SUM(Reserves.G * Reserves.G) FROM Reserves GROUP BY Reserves.H;";
			//String eval = "SELECT Reserves.H, SUM(1) FROM Reserves GROUP BY Reserves.H;";
			Statement statement = CCJSqlParserUtil.parse(eval);
			if (statement != null) {
				PlainSelect select = (PlainSelect) statement;
				List<SelectItem<?>> arr = select.getSelectItems();
				System.out.println(arr);
				for (SelectItem<?> item : arr) {
					Expression exp = item.getExpression();
					if (exp instanceof Column) {
						Column col = (Column) exp;
						System.out.println(col.getColumnName());
						System.out.println(col.getTable().getName());
					} else {
						System.out.println(exp instanceof Function);
						Function func = (Function) exp;
						System.out.println(func.getName());
						System.out.println(func.getParameters());
						System.out.println(Arrays.asList(func.getParameters().toString().split("\\*")));
					}
					
				}
				
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

	public static void selectExample(String filename) {
		try {
			String eval = "SELECT * FROM Sailors WHERE Sailors.C >= 200 AND Sailors.C <= 500";
			//String eval = "SELECT * FROM Sailors";
			Statement statement = CCJSqlParserUtil.parse(eval);
			if (statement != null) {
				System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				System.out.println("Select body is " + select.getSelectBody());
				PlainSelect select2 = (PlainSelect) statement;
				System.out.println(select2.getWhere());
				Expression exp = select2.getWhere();
				//TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
				String tableName = select2.getFromItem().toString();
				ScanOperator scan = new ScanOperator(tableName, tableName);
				Operator root = new SelectOperator(scan, exp);
				root.dump();
				
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

	public static void catalogExample(String filename) {
		//DataCatalog.initialize(filename);

		DataCatalog catalog = DataCatalog.getInstance();

		System.out.println(catalog.getSchema("Boats"));
		System.out.println(catalog.getPath("Reserves"));
		System.out.println(catalog.getColumnIndex("Sailors", "C"));
	}

	public static void scanExample(String filename) {
		
		//DataCatalog.initialize(filename);
		
		ScanOperator op = new ScanOperator("Boats", "Boats");
		Tuple tup;
		while ((tup = op.getNextTuple()) != null) {
			System.out.println(tup);
		}
		op.reset();
		while ((tup = op.getNextTuple()) != null) {
			System.out.println(tup);
		}

	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement from
	 * a file and prints it to screen; then extracts SelectBody from the query and
	 * prints it to screen.
	 */

	public static void parsingExample(String filename) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
            //Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Boats");
			//Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Boats WHERE Boats.id = 4");
			if (statement != null) {
				System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				System.out.println("Select body is " + select.getSelectBody());
				PlainSelect select2 = (PlainSelect) statement;
				System.out.println(select2.getWhere());
				Expression exp = select2.getWhere();
				
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}

