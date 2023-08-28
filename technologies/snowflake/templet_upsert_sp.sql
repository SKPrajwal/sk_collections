CREATE OR REPLACE PROCEDURE <database>.<schema>.<table>_sp() 
RETURNS VARCHAR(16777216) 
LANGUAGE JAVASCRIPT 
EXECUTE AS OWNER
  as
    $$
    
    function execute_statement(statement) {
     var rs = snowflake.createStatement( {sqlText:statement}).execute();
     return rs
    }
    
    function getTotalInsertedRecordCount(){
    insert_query = `SELECT "number of rows inserted" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))`;
     rs = execute_statement(insert_query)
     rs.next()
     return "\n\n Rows inserted: " + rs.getColumnValue(1) + "| \n\n\n"
    }

    function getTotalUpsertedRecordCount(){
     upsert_query = `SELECT "number of rows inserted", "number of rows updated" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))`
     rs = execute_statement(upsert_query)
     rs.next()
     return  "\n\n Rows inserted: " + rs.getColumnValue(1) + ",  Rows updated: " + rs.getColumnValue(2) + " | \n\n\n"
    }
       
    merge_query = ``;
    
    result = ""

    try{
      execute_statement(merge_query);
      var result = result + getTotalUpsertedRecordCount() 
      execute_statement(commit_query);
    }
        
		catch (err) {
		  result = "Failed: Code: " + err.code + " | State: " + err.state;
		  result += "\\n Message: " + err.message;
		  result += "\\nStack Trace:\\n" + err.stackTraceTxt;
		}
    return result;
        
    $$;
