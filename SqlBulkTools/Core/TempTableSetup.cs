using System.Collections.Generic;
using System.Data.SqlClient;

namespace SqlBulkTools.Core
{
    /// <summary>
    /// 
    /// </summary>
    public class TempTableSetup
    {
        /// <summary>
        /// 
        /// </summary>
        public string InsertQuery { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public List<SqlParameter> SqlParameterList { get; set; }
    }
}
