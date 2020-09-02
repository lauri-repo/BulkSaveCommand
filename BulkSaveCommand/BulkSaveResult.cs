
namespace BulkSaveCommand
{
    /// <summary>
    /// Details about the results of the command's execution.
    /// </summary>
    public class BulkSaveResult
    {
        /// <summary>
        /// Create a new instance.
        /// </summary>
        public BulkSaveResult(int insertCount, int updateCount, int deleteCount)
        {
            InsertCount = insertCount;
            UpdateCount = updateCount;
            DeleteCount = deleteCount;
        }

        /// <summary>
        /// Number of rows inserted.
        /// </summary>
        public int InsertCount { get; }
        /// <summary>
        /// Number of rows updated.
        /// </summary>
        public int UpdateCount { get; }
        /// <summary>
        /// Number of rows deleted.
        /// </summary>
        public int DeleteCount { get; }
    }
}
