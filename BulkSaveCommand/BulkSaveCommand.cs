using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace BulkSaveCommand
{
    /// <summary>
    /// Can be used to save or delete large amounts of data from/to a table. Functions in the following way:
    /// 1. A local temporary table is created in the database by duplicating the model of the target table.
    /// 2. SqlBulkCopy is used to insert all the values to the temporaray table.
    /// 3. A merge statement is built with the help of system views that provide metadata about the target table's model.
    /// </summary>
    public class BulkSaveCommand
    {
        private readonly SqlConnection _connection;
        private readonly string _targetTable;
        private readonly string _transactionName;

        private string TargetSchemaName { get { return _targetTable.Contains(".") ? _targetTable.Split('.')[0] : "dbo"; } }
        private string TargetTableName { get { return _targetTable.Contains(".") ? _targetTable.Split('.')[1] : _targetTable; } }
        /// <summary>
        /// The cached columns that are present in both the DataTable(s) processed by this class and the target table.
        /// </summary>
        private ColumnList _commonColumns;
        /// <summary>
        /// The cached columns (model) of the target table.
        /// </summary>
        private ColumnList _targetTableColumns;

        /// <summary>
        /// The delete key
        /// </summary>
        public const string DeleteKey = "DeleteKey";

        /// <summary>
        /// Defines how many rows are written to the temporary table in a single batch. Default is 1000.
        /// </summary>
        public int BatchSize { get; set; }
        /// <summary>
        /// Defines if textual data should be automatically truncated according to target column length. Default is true.
        /// </summary>
        public bool TruncateData { get; set; }
        /// <summary>
        /// Defines if .Trim() is called on all values designated for columns with a limited data length (mainly varchar). Default is true.
        /// </summary>
        public bool TrimStrings { get; set; }
        /// <summary>
        /// Defines if empty/whitespace only strings should be saved as NULLs instead for nullable columns. Default is true.
        /// </summary>
        public bool NullifyEmptyStrings { get; set; }
        /// <summary>
        /// Defines if duplicate rows (according to target table PK) should be automatically removed from the table. Default is true.
        /// </summary>
        public bool RemoveDuplicates { get; set; }
        /// <summary>
        /// Defines is the target table's schema info (column names, lengths, etc.) should be cached by this class for better performance. Default is true.
        /// </summary>
        public bool CacheSchema { get; set; }
        /// <summary>
        /// Custom statements that need to be run instead of using data from the given DataTable.
        /// In the main dictionary the key defines which action the statement(s) should apply for. In the subdictionary the key is the column's name and the value the SQL statement.
        /// For example to set the value of an InsertDate column only when a row is actually inserted, do myCmd.CustomStatements[DbAction.Insert].Add("InsertDate", "GETDATE()");
        /// The source table alias is 'source' and the target table alias is 'target'.
        /// The syntax is the same for insert/update/upsert. This member and all of its subcollections are initialized automatically.
        /// </summary>
        public IDictionary<DbAction, IDictionary<string, string>> CustomStatements { get; set; }

        /// <summary>
        /// Create a new instance.
        /// </summary>
        /// <param name="connection">The SQL connection to use. The database property needs to be set and the user needs to have read access to the following system views:
        ///                          INFORMATION_SCHEMA.COLUMNS, INFORMATION_SCHEMA.TABLE_CONSTRAINTS, INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE. 
        ///                          In addition of course the user needs write access to the target table.
        ///                          It is the creator of this class that is in charge of opening/closing the connection.</param>
        /// <param name="targetTable">The table to save data to. If the table's schema is dbo then just the table name is required (ex: MyTable). 
        ///                           If the table's schema is other than dbo then both the the schema and table name are required (ex: schema.MyTable).
        ///                           The target table must have a primary key and at least 1 non-identity column.</param>
        public BulkSaveCommand(SqlConnection connection, string targetTable)
        {
            _connection = connection;
            _targetTable = targetTable;
            _transactionName = "BulkSaveCommand " + _targetTable;

            BatchSize = 1000;
            TrimStrings = true;
            CacheSchema = true;
            TruncateData = true;
            RemoveDuplicates = true;
            NullifyEmptyStrings = true;

            CustomStatements = new Dictionary<DbAction, IDictionary<string, string>>();
            foreach (DbAction action in Enum.GetValues(typeof(DbAction)))
                CustomStatements.Add(action, new Dictionary<string, string>());
        }

        /// <summary>
        /// Write the given DataTable to the target table provided in the constructor. The target table may not be a temporary table.
        /// The input will be modified according to the options set for this class. 
        /// Make sure to read the descriptions of all the options (properties) of this class before using it.
        /// </summary>
        /// <param name="table">DataTable filled with data to write or delete. Be warned that the table might be modified by this method.</param>
        /// <param name="action">What action to take when saving data.</param>
        /// <param name="transaction">Optional parameter. If provided, every database operation performed by this class will be part of the transaction. 
        ///                           It will be the creator of this class that is responsible for committing it. 
        ///                           If no value is given, a transaction is created, used and commited by this method anyway.</param>
        /// <returns>Object with details about the results of the command's execution.</returns>
        public BulkSaveResult WriteToServer(DataTable table, DbAction action, SqlTransaction transaction = null)
        {
            var returnValue = new BulkSaveResult(0, 0, 0);
            if (table.Rows.Count == 0) return returnValue;

            //initialize columns lists if necessary
            if (_commonColumns == null || _targetTableColumns == null || !CacheSchema)
            {
                RefreshModel(table, transaction);
            }

            //run with given transaction or start one
            if (transaction == null)
            {
                using (transaction = _connection.BeginTransaction(_transactionName))
                {
                    returnValue = WriteTableToServer(table, action, transaction);
                    transaction.Commit();
                }
            }
            else
            {
                returnValue = WriteTableToServer(table, action, transaction);
            }

            return returnValue;
        }

        /// <summary>
        /// Refresh the cached model of the target table.
        /// </summary>
        /// <param name="table">The DataTable that is currently being processed. This will be used to refresh the _commonColumns collection.</param>
        /// <param name="transaction">Transaction for this query.</param>
        private void RefreshModel(DataTable table, SqlTransaction transaction = null)
        {
            _targetTableColumns = new ColumnList(GetAllColumns(transaction));
            _commonColumns = GetCommonColumns(table, _targetTableColumns);
        }

        /// <summary>
        /// Ensure that the target table has a PK and that the source and target tables have the same number of PK columns.
        /// </summary>
        private void ValidatePkColumns()
        {
            if (_targetTableColumns.GetPkColumns().Count == 0)
                throw new InvalidOperationException("The target table must have a primary key.");

            if (_targetTableColumns.GetPkColumns().Count != _commonColumns.GetPkColumns().Count)
                throw new InvalidOperationException("All primary key columns must be included in the DataTable object.");
        }

        private BulkSaveResult WriteTableToServer(DataTable table, DbAction action, SqlTransaction transaction, bool retry = true)
        {
            ValidatePkColumns();

            try
            {
                //fix input data
                LimitColumnDataLength(table, _commonColumns);
                EliminateExcessiveColumns(table, _commonColumns);

                //create a temp table name and the bulk copy object
                var tempTableName = "#" + Guid.NewGuid().ToString();
                var bulkCopy = CreateBulkCopy(transaction, tempTableName, _commonColumns);

                //create a temp table in the database (can only be seen by this session) and copy the data into it
                CreateTemporaryTable(transaction, tempTableName, _targetTableColumns);
                bulkCopy.WriteToServer(table);

                //DELETE or MERGE
                if (action == DbAction.Delete)
                {
                    var deleteSql = BuildDeleteStatement(tempTableName, action, table);
                    var deleteCount = ExecuteDeleteStatement(transaction, deleteSql);

                    return new BulkSaveResult(0, 0, deleteCount);
                }
                else
                {
                    //create and execute a MERGE statement saving the data from the temp table into the actual table
                    var saveSql = BuildSaveStatement(tempTableName, action, _commonColumns);
                    var mergeCount = ExecuteMergeStatement(transaction, saveSql);

                    return new BulkSaveResult(mergeCount[0], mergeCount[1], 0);
                }
            }
            catch
            {
                //no retry, throw
                if (!retry) throw;

                //maybe the DB model changed, refresh and try again
                RefreshModel(table, transaction);
                return WriteTableToServer(table, action, transaction, false);
            }
        }

        private SqlBulkCopy CreateBulkCopy(SqlTransaction transaction, string tempTableName, List<Column> columns)
        {
            var bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = tempTableName,
                BatchSize = BatchSize
            };
            columns.ForEach(column => bulkCopy.ColumnMappings.Add(column.Name, column.Name));

            return bulkCopy;
        }

        #region Sanitize Input Data

        /// <summary>
        /// Remove any columns from the DataTable that are not present in the provided ColumnList.
        /// </summary>
        private void EliminateExcessiveColumns(DataTable table, ColumnList columns)
        {
            var commonColumnNames = columns.Select(col => col.Name.ToLower()).ToList();
            var columnsToRemove = new List<string>();

            foreach (DataColumn column in table.Columns)
            {
                if (!commonColumnNames.Contains(column.ColumnName.ToLower()))
                {
                    columnsToRemove.Add(column.ColumnName);
                }
            }

            columnsToRemove.ForEach(col => table.Columns.Remove(col));
            table.AcceptChanges();
        }

        /// <summary>
        /// Sanitize the data in the DataTable according to the options set.
        /// This will: 
        /// 1. Truncate data to the maximum value of the column if the TruncateData option is set.
        /// 2. Trim all the strings if the TrimStrings option is set.
        /// 3. Replace all empty strings with NULL if the NullifyEmptyStrings option is set.
        /// </summary>
        /// <param name="table">Table with data to sanitize.</param>
        /// <param name="columns">ColumnList with the target table's model. This will be used to see where data needs to be truncated.</param>
        private void LimitColumnDataLength(DataTable table, ColumnList columns)
        {
            var limitedColumns = columns.GetLimitedColumns();

            foreach (DataRow row in table.Rows)
            {
                foreach (var column in limitedColumns)
                {
                    var value = Convert.ToString(row[column.Name]);

                    if (TruncateData && column.DataLength != -1)    //datalength for [n]varchar(max) is -1
                    {
                        var endIndex = Math.Min(value.Length, column.DataLength.Value);
                        value = value.Substring(0, endIndex);
                    }
                    if (TrimStrings)
                    {
                        value = value.Trim();
                    }

                    var nullify = NullifyEmptyStrings && column.IsNullable && string.IsNullOrWhiteSpace(value);
                    row[column.Name] = nullify ? DBNull.Value : (object)value;
                }
            }
        }

        #endregion

        #region Get Columns

        /// <summary>
        /// Get the columns that are present in both the provided DataTable and the ColumnList.
        /// </summary>
        private ColumnList GetCommonColumns(DataTable table, ColumnList columns)
        {
            var sourceTableColumns = table.Columns.OfType<DataColumn>().Select(col => col.ColumnName);
            var commonColumns = columns.Where(column => sourceTableColumns.Any(colName => StringsEqual(colName, column.Name))).ToList();

            return new ColumnList(commonColumns);
        }

        /// <summary>
        /// Get the model of our target table.
        /// </summary>
        /// <param name="transaction">Transaction for this query.</param>
        /// <returns>All of the target table's columns.</returns>
        private IEnumerable<Column> GetAllColumns(SqlTransaction transaction)
        {
            const string sql =
                @"SELECT 
                  c.COLUMN_NAME,
                  c.IS_NULLABLE, 
                  c.COLUMN_DEFAULT,
                  c.CHARACTER_MAXIMUM_LENGTH,
                  COLUMNPROPERTY(OBJECT_ID(c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY,
                  CASE WHEN ccu.TABLE_SCHEMA IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
                  FROM 
                  INFORMATION_SCHEMA.COLUMNS c LEFT OUTER JOIN
                  INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON c.TABLE_SCHEMA = tc.TABLE_SCHEMA AND c.TABLE_NAME = tc.TABLE_NAME LEFT OUTER JOIN
                  INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu ON tc.TABLE_SCHEMA = ccu.TABLE_SCHEMA AND tc.TABLE_NAME = ccu.TABLE_NAME AND tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME AND c.COLUMN_NAME = ccu.COLUMN_NAME 
                  WHERE
                  c.TABLE_SCHEMA = @schemaName AND c.TABLE_NAME = @tableName AND
                  (tc.CONSTRAINT_TYPE = 'PRIMARY KEY' OR tc.CONSTRAINT_TYPE IS NULL)";

            using (var cmd = new SqlCommand(sql, _connection, transaction))
            {
                cmd.Parameters.Add("@schemaName", SqlDbType.NVarChar).Value = TargetSchemaName;
                cmd.Parameters.Add("@tableName", SqlDbType.NVarChar).Value = TargetTableName;

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        yield return
                            new Column
                            {
                                Name = Convert.ToString(reader["COLUMN_NAME"]),
                                IsNullable = StringsEqual(Convert.ToString(reader["IS_NULLABLE"]), "YES"),
                                Default = Convert.ToString(reader["COLUMN_DEFAULT"]),
                                DataLength = reader["CHARACTER_MAXIMUM_LENGTH"] as int?,
                                IsIdentity = Convert.ToBoolean(reader["IS_IDENTITY"]),
                                IsPrimaryKey = Convert.ToBoolean(reader["IS_PRIMARY_KEY"])
                            };
                    }
                }
            }
        }

        #endregion

        #region Build SQL Statements

        /// <summary>
        /// Create a temporary table that duplicates the model of our target table.
        /// </summary>
        /// <param name="transaction">Transaction for this query.</param>
        /// <param name="tempTableName">A name for the temporary table.</param>
        /// <param name="columns">ColumnList containing the target table's column data.</param>
        private void CreateTemporaryTable(SqlTransaction transaction, string tempTableName, ColumnList columns)
        {
            var sql = string.Format("SELECT * INTO [{0}] FROM [{1}].[{2}] WHERE 1 = 0", tempTableName, TargetSchemaName, TargetTableName);
            var random = new Random();

            //this will add all the default constraints to the temp table (if caller's DataTable does not have one of the NOT NULL columns with a DEFAULT bulk copy will fail)
            foreach (var column in columns.Where(col => !string.IsNullOrWhiteSpace(col.Default)))
            {
                sql += string.Format(";ALTER TABLE [{0}] ADD CONSTRAINT [DF_{1}] DEFAULT {2} FOR [{3}]", tempTableName, random.Next(), column.Default, column.Name);
            }

            using (var cmd = new SqlCommand(sql, _connection, transaction))
                cmd.ExecuteNonQuery();
        }

        private string BuildSaveStatement(string tempTableName, DbAction action, ColumnList columns)
        {
            return
                BuildDuplicateRemovalStatement(tempTableName, columns.GetPkColumns()) + Environment.NewLine +
                BuildMergeStatement(tempTableName, action, columns);
        }

        private string BuildDeleteStatement(string tempTableName, DbAction action, DataTable table)
        {
            string sql = @"DELETE target FROM [{TARGET_SCHEMA_NAME}].[{TARGET_TABLE_NAME}] AS target JOIN
					                          [{TEMP_TABLE_NAME}] AS source ON ({MATCH_PREDICATE})";

            //set table and schema names
            sql = sql.Replace("{TARGET_SCHEMA_NAME}", TargetSchemaName);
            sql = sql.Replace("{TARGET_TABLE_NAME}", TargetTableName);
            sql = sql.Replace("{TEMP_TABLE_NAME}", tempTableName);

            //determine the column names that we need to use to delete rows
            var deleteColumnNames = table.Columns.OfType<DataColumn>()
                                                 .Where(col => col.ExtendedProperties.ContainsKey(DeleteKey))
                                                 .Select(col => col.ColumnName).ToList();

            //if no columns throw exception
            if (deleteColumnNames.Count == 0)
                throw new Exception("At least one column in the table has to be marked with DeleteKey when action is DeleteAndInsert.");

            //now get the columns objects from the names
            var commonColumns = _commonColumns.Where(column => deleteColumnNames.Any(colName => StringsEqual(colName, column.Name))).ToList();

            //set the predicate
            var pkPredicate = SetStatement(commonColumns, action, "source.{0} = target.{0} AND ");
            sql = sql.Replace("{MATCH_PREDICATE}", pkPredicate.TrimEnd(" AND "));

            return sql;
        }

        private string BuildMergeStatement(string tempTableName, DbAction action, ColumnList columns)
        {
            string sql = @"DECLARE @DummyUpdateField INT
                           DECLARE @SummaryOfChanges TABLE(Change VARCHAR(20));
                           MERGE INTO [{TARGET_SCHEMA_NAME}].[{TARGET_TABLE_NAME}] AS target
	                           USING (SELECT * FROM [{TEMP_TABLE_NAME}]) AS source
	                           ON ({PK_MATCH_PREDICATE})
	                           WHEN MATCHED AND '{DB_ACTION}' IN ('UPSERT', 'UPDATE') THEN
	                           UPDATE SET 
                                   {SET_STATEMENTS}
	                           WHEN NOT MATCHED BY target AND '{DB_ACTION}' IN ('UPSERT', 'INSERT') THEN
	                           INSERT (
		                           {INSERT_COLUMN_LIST}
	                           ) VALUES (
		                           {INSERT_VALUE_LIST}
	                           )
                           OUTPUT $action INTO @SummaryOfChanges;

                           SELECT COUNT(*) AS [Count] FROM @SummaryOfChanges WHERE Change = 'INSERT'
                           UNION ALL
                           SELECT COUNT(*) AS [Count] FROM @SummaryOfChanges WHERE Change = 'UPDATE'";

            //set table and schema names
            sql = sql.Replace("{TARGET_SCHEMA_NAME}", TargetSchemaName);
            sql = sql.Replace("{TARGET_TABLE_NAME}", TargetTableName);
            sql = sql.Replace("{TEMP_TABLE_NAME}", tempTableName);

            //set the action
            sql = sql.Replace("{DB_ACTION}", GetStringDbAction(action));

            //set the PK match predicate
            var pkPredicate = SetStatement(columns.GetPkColumns(), action, "source.{0} = target.{0} AND ");
            sql = sql.Replace("{PK_MATCH_PREDICATE}", pkPredicate.TrimEnd(" AND "));

            //set the SET statement for the UPDATE statement, add a dummy statement instead if there are no non-PK columns
            var setStatement = SetStatement(_commonColumns.GetNormalColumns(), action, "target.{0} = {1},");
            setStatement = string.IsNullOrWhiteSpace(setStatement) ? "@DummyUpdateField = @DummyUpdateField" : setStatement;
            sql = sql.Replace("{SET_STATEMENTS}", setStatement.TrimEnd(','));

            //set the column list for the INSERT statement
            var columnList = SetStatement(columns.GetNonIdentityColumns(), action, "{0},");
            sql = sql.Replace("{INSERT_COLUMN_LIST}", columnList.TrimEnd(","));

            //set the VALUES part for the INSERT statement
            var sourceList = SetStatement(_commonColumns.GetNonIdentityColumns(), action, "{1},");
            sql = sql.Replace("{INSERT_VALUE_LIST}", sourceList.TrimEnd(","));

            return sql;
        }

        /// <summary>
        /// Set an SQL statement for the given columns.
        /// </summary>
        /// <param name="columns">Columns to include in the statement.</param>
        /// <param name="action">The target database action for this statement.</param>
        /// <param name="statement">SQL statement to set for each columns in parameter 1. In this string
        ///                         {0} will be formatted to the column name (brackets included)
        ///                         {1} to either the the column name (brackets included) or a custom statement if one has been specified for the given DB action (parameter 2).</param>
        /// <returns></returns>
        private string SetStatement(List<Column> columns, DbAction action, string statement)
        {
            return columns.Aggregate("", (current, column) => current + string.Format(statement, "[" + column.Name + "]", GetColumnStatement(action, column)));
        }

        /// <summary>
        /// Get either the column name or a custom SQL statement for the given action and column combination.
        /// </summary>
        private string GetColumnStatement(DbAction action, Column column)
        {
            var customStatement = CustomStatements.Where(keyVal => keyVal.Key.HasFlag(action))
                                                  .SelectMany(keyVal => keyVal.Value)
                                                  .FirstOrDefault(innerKeyVal => StringsEqual(innerKeyVal.Key, column.Name));

            return string.IsNullOrWhiteSpace(customStatement.Value) ? "source.[" + column.Name + "]" : customStatement.Value;
        }

        private string BuildDuplicateRemovalStatement(string tempTableName, List<Column> pkColumnNames)
        {
            if (!RemoveDuplicates || pkColumnNames.Count == 0) return "";

            var sql = @"WITH cte AS (
	                        SELECT {PK_COLUMN_LIST}, 
		                        ROW_NUMBER() OVER(PARTITION BY {PK_COLUMN_LIST} ORDER BY [{ORDER_BY_COLUMN}]) AS [RowNumber]
	                        FROM [{TEMP_TABLE_NAME}]
                        )
                        DELETE cte WHERE [RowNumber] > 1";

            var pkColumnList = pkColumnNames.Aggregate("", (current, column) => current + "[" + column + "],");
            sql = sql.Replace("{PK_COLUMN_LIST}", pkColumnList.TrimEnd(","));
            sql = sql.Replace("{ORDER_BY_COLUMN}", pkColumnNames[0].Name);
            sql = sql.Replace("{TEMP_TABLE_NAME}", tempTableName);

            return sql;
        }

        private int ExecuteDeleteStatement(SqlTransaction transaction, string sql)
        {
            using (var cmd = new SqlCommand(sql, _connection, transaction))
            {
                return cmd.ExecuteNonQuery();
            }
        }

        private int[] ExecuteMergeStatement(SqlTransaction transaction, string sql)
        {
            using (var cmd = new SqlCommand(sql, _connection, transaction))
            {
                return ExecuteReaderAndGetRows(cmd, "Count").Select(row => Convert.ToInt32(row)).ToArray();
            }
        }

        #endregion

        #region Helpers

        private string GetStringDbAction(DbAction dbAction)
        {
            if (dbAction == DbAction.Insert) return "INSERT";
            if (dbAction == DbAction.Update) return "UPDATE";
            if (dbAction == DbAction.Upsert) return "UPSERT";
            if (dbAction == DbAction.Delete) return "DELETE";

            return "";
        }

        private bool StringsEqual(string str1, string str2)
        {
            if (str1 == null && str2 == null) return true;
            if (str1 == null || str2 == null) return false;

            return string.Equals(str1.Trim(), str2.Trim(), StringComparison.InvariantCultureIgnoreCase);
        }

        private IEnumerable<string> ExecuteReaderAndGetRows(SqlCommand cmd, string columnName)
        {
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    yield return Convert.ToString(reader[columnName]);
                }
            }
        }

        private class ColumnList : List<Column>
        {
            public ColumnList(IEnumerable<Column> columns)
            {
                AddRange(columns);
            }

            public List<Column> GetLimitedColumns()
            {
                return this.Where(column => column.DataLength.HasValue).ToList();
            }

            public List<Column> GetPkColumns()
            {
                return this.Where(column => column.IsPrimaryKey).ToList();
            }

            public List<Column> GetNormalColumns()
            {
                return this.Where(column => !column.IsPrimaryKey && !column.IsIdentity).ToList();
            }

            public List<Column> GetNonIdentityColumns()
            {
                return this.Where(column => !column.IsIdentity).ToList();
            }
        }

        private class Column
        {
            public string Name { get; set; }
            public bool IsNullable { get; set; }
            public string Default { get; set; }
            public int? DataLength { get; set; }
            public bool IsIdentity { get; set; }
            public bool IsPrimaryKey { get; set; }

            public override string ToString()
            {
                return Name;
            }
        }

        #endregion
    }

    internal static class StringExtensions
    {
        public static string TrimEnd(this string source, string value)
        {
            return source.EndsWith(value) ? source.Remove(source.LastIndexOf(value, StringComparison.Ordinal)) : source;
        }
    }
}