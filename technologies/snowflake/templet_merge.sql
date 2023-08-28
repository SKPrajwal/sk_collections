MERGE into <TARGET_TABLE> AS TGT 
using (SOURCE_TABLE) SRC
ON 
  TGT.<Id1> = SRC.<Id1> AND
  TGT.<Id2> = SRC.<Id2>
when matched 
AND (
  TGT.<COLUMN1> <> SRC.<COLUMN1> OR
  TGT.<COLUMN2> <> SRC.<COLUMN2>
)
then update 
set
  TGT.<column1> = SRC.<column1>,
  TGT.<column2> = SRC.<column2>
when not matched then insert (
  <column1>,
  <column2>
) 
values
(
  SRC.<value1>,
  SRC.<value2>
);
