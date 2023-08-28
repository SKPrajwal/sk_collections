create or replace procedure <database>.<schema>.<table>_SP()
  returns varchar
  language javascript
  as
  $$
  function execute_statement_get_first(statement) {
     var rs = execute_statement(statement)
     rs.next()
     return rs.getColumnValue(1)
    }

    function execute_statement(statement) {
     var rs = snowflake.createStatement( {sqlText:statement}).execute();
     return rs
    }

    function getTotalCopiedRecordCount(){
    insert_query = `SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))`;
     rs = execute_statement(insert_query)
     copied_files = 0
     rows_copied = 0
     rs.next()
     
      if(rs.getColumnValue(1)=="Copy executed with 0 files processed.") {  
        return "NO FILES PROCESSED ::  Rows Copied: 0 | \n\n\n"
        }
        
      do{
        copied_files++
        rows_copied += rs.getColumnValue(4)
      }while(rs.next()==true)
      
      if(copied_files==1)
        return "STATUS : 1 FILE LOADED::  Rows Copied: " + rows_copied + "| \n\n\n"
      else if(copied_files>1)
        return "STATUS : " + copied_files + " FILES LOADED::  Rows Copied: " + rows_copied + "| \n\n\n"
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

    //Current table specific functions
	function deleteTable(tbl){
      var clone_tbl = `delete from ` + tbl + `
        where INSERT_TIMESTAMP <= current_timestamp();`
     rs = execute_statement(clone_tbl)
    }
    function cloneTable(tbl){
      var clone_tbl = `create or replace table ` + tbl + `_CLONE
                like ` + tbl + `;`
     rs = execute_statement(clone_tbl)
    }
    function swapTable(tbl){
      var swap_tbl = `ALTER TABLE ` + tbl + `_CLONE
                SWAP WITH ` + tbl + `;`
     rs = execute_statement(swap_tbl)
    }
    function dropCloneTable(tbl){
      var drop_tbl = `DROP TABLE ` + tbl + `_CLONE;`
     rs = execute_statement(drop_tbl)
    }
    function refreshTable(current_table,query){
        cloneTable(current_table)
        execute_statement(query)
        var rs = current_table + "\n\n -- LOAD QUERY   ::  " + getTotalInsertedRecordCount()        
        swapTable(current_table)
        dropCloneTable(current_table)     
        return rs 
    }

    var insert_query = `insert into ....`

    commit_query = `commit;`;

    result = ""

      try{
          result += refreshTable("<database>.<schema>.<table>",insert_query)
          execute_statement(commit_query)
      }
      catch (err) {
          result =  "Failed: Code: " + err.code + " | STATE_NAME: " + err.STATE_NAME;
          result += "\n  Message: " + err.message;
          result += "\nStack Trace:\n" + err.stackTraceTxt;
      }
      return result
        
    $$;
	
