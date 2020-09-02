# BulkSaveCommand
A wrapper around System.Data.SqlClient.SqlBulkCopy making it much more flexible. Easily persist massive amounts of data in your System.Data.DataTable into Microsoft SQL Server.  

This work is in the public domain (see LICENSE file or https://unlicense.org for more info) meaning you can use, modify and redistribute this work in any way, (for personal, commercial or other purposes) without any limits and any need for attribution.


Main features:  
● Allows Upsert and Delete operations.  
● Custom column statements - Instead of saving data to a column you can specify a SQL statement that should be run instead (such as GETDATE()).  
● Column matching - Any columns present in the source DataTable that are not present in the database's target table will be removed from the source table before saving. This would allow you to: 1. Drop columns in the database without having to worry about refactoring your code; 2. Rename database columns if prior to renaming you refactor your code such that the source DataTable contains both the old and new column (with the same data).  

Optional features (change properties as necessary for your use case):  
● TruncateData - Textual data is automatically truncated to the target column's maximum data length to avoid "String or binary data will be truncated" errors.  
● RemoveDuplicates - Rows with duplicate primary keys will be removed (leaving 1 row) to avoid "Violation of PRIMARY KEY constraint" errors.  
● TrimStrings - Strings are trimmed to remove leading/trailing spaces.  
● NullifyEmptyStrings - Empty strings are replaced with NULL.  
