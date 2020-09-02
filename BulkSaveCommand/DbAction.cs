
namespace BulkSaveCommand
{
    /// <summary>
    /// Database action class
    /// </summary>
    public enum DbAction
    {
        /// <summary>
        /// Insert all the rows in the given DataTable.
        /// </summary>
        Insert = 1,
        /// <summary>
        /// Update all the rows in the given DataTable.
        /// </summary>
        Update = 2,
        /// <summary>
        /// Insert the row if not found, update if found.
        /// </summary>
        Upsert = Insert + Update,
        /// <summary>
        /// Delete all rows that match the values of the columns specified in the DataTable using DeleteKey.
        /// For example when you do the following: myDataTable.Columns["MyColumn"].ExtendedProperties.Add(BulkSaveCommand.DeleteKey, true);
        /// then all rows that match the value of 'MyColumn' of each row will be deleted. When using multiple columns then it is the combination of column values that is used when deleting.
        /// This action type is useful when dealing with mapping tables.
        /// </summary>
        Delete = 4
    }
}
