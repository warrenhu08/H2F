using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Data;
using System.Data.Common;
using Dapper;
using static Dapper.SqlMapper;

namespace H2F.Framework.Common.Helper
{
    /// <summary>
    /// 数据库连接信息
    /// <connectionStrings>
    ///    <add name = "default" connectionString="server=localhost;user id=root;password=XXX;database=h2f;pooling=true;Min Pool Size=0;Max Pool Size=512;" providerName="MySql.Data.MySqlClient"/>
    /// </connectionStrings>
    /// </summary>
    public class DbConnectionSetting
    {
        /// <summary>
        /// 连接名称
        /// </summary>
        public string DbConnectionName { get; set; }
        /// <summary>
        /// DbProvider名称
        /// </summary>
        public string DbProviderName { get; set; }
        /// <summary>
        /// 连接字符串
        /// </summary>
        public string DbConnectionString { get; set; }
    }

    /// <summary>
    /// 创建/获取数据库连接
    /// </summary>
    internal class DbConnectionFactory
    {
        //缓存数据库连接信息
        const string DefaultDbConnectionName = "default";
        private readonly IList<DbConnectionSetting> _dbConnectionSettings = null;

        static readonly DbConnectionFactory _instance = new DbConnectionFactory();

        /// <summary>
        /// 单例
        /// </summary>
        public static DbConnectionFactory Instance { get { return _instance; } }

        private DbConnectionFactory()
        {
            _dbConnectionSettings = BuildDbConnectionSettings();
        }

        /// <summary>
        /// 获取数据库连接
        /// </summary>
        /// <param name="dbConnectionName"></param>
        /// <returns></returns>
        internal IDbConnection GetDbConnection(string dbConnectionName = null)
        {
            if (string.IsNullOrEmpty(dbConnectionName))            
                dbConnectionName = DefaultDbConnectionName;            

            //缓存中查找数据库连接信息
            var dbConnectionSetting = _dbConnectionSettings.FirstOrDefault(c => c.DbConnectionName == dbConnectionName);
            if (dbConnectionSetting == null)
                throw new ConfigurationErrorsException(string.Format("没有找到{0}对应的数据库连接字符串!", dbConnectionName));

            DbProviderFactory dbFactory = DbProviderFactories.GetFactory(dbConnectionSetting.DbProviderName);
            var dbConnection = dbFactory.CreateConnection();
            dbConnection.ConnectionString = dbConnectionSetting.DbConnectionString;

            return dbConnection;
        }

        /// <summary>
        /// 根据配置项，创建数据库连接信息 
        /// </summary>
        /// <returns></returns>
        internal IList<DbConnectionSetting> BuildDbConnectionSettings()
        {
            var dbConnectionSettings = new List<DbConnectionSetting>();

            var connectionStringSettings = ConfigurationManager.ConnectionStrings;
            if (connectionStringSettings.Count == 0)
                throw new ConfigurationErrorsException("没有找到数据库连接字符串!");

            foreach (ConnectionStringSettings connectionStringSetting in connectionStringSettings)
            {
                if (string.IsNullOrEmpty(connectionStringSetting.Name))
                    throw new ConfigurationErrorsException("数据库连接字符串没有设置对应的名称!");

                var providerName = connectionStringSetting.ProviderName.Trim();
                if ("MySql.Data.MySqlClient".Equals(providerName, StringComparison.OrdinalIgnoreCase))
                { }
                else if ("System.Data.SqlClient".Equals(providerName, StringComparison.OrdinalIgnoreCase))
                { }
                else
                {
                    continue;
                    //throw new ConfigurationErrorsException(string.Format("暂不支持{0}数据库!", providerName));
                }
               
                dbConnectionSettings.Add(new DbConnectionSetting
                {
                    DbConnectionName = connectionStringSetting.Name,
                    DbProviderName = providerName,
                    DbConnectionString = connectionStringSetting.ConnectionString.Trim()
                });
            }            
            
            return dbConnectionSettings;
        }
    }

    /// <summary>
    /// Dapper CRUD入口
    /// </summary>
    public partial class DapperHelper
    {
        /// <summary>
        /// 执行一条Sql查询语句；返回数据/实体对象列表
        /// </summary>
        /// <typeparam name="T">数据/实体对象</typeparam>
        /// <param name="sql">Sql查询语句</param>
        /// <param name="param">Sql参数</param>
        /// <param name="commandType">Sql语句类型 (Sql 或 存储过程)，默认为Sql语句</param>
        /// <returns>数据/实体对象列表</returns>
        public static IEnumerable<T> Query<T>(string sql, object param = null, CommandType? commandType = null, string dbConnectionName = null) where T : class
        {
            IEnumerable<T> result = Enumerable.Empty<T>();

            using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
            {
                result = connection.Query<T>(sql, param, commandType: commandType);
            }

            return result;
        }

        /// <summary>
        /// 执行一条Sql查询语句；返回第一条匹配记录，并转换成相应的数据/实体对象
        /// </summary>
        /// <typeparam name="T">数据/实体对象</typeparam>
        /// <param name="sql">Sql查询语句</param>
        /// <param name="param">Sql参数</param>
        /// <param name="commandType">Sql语句类型 (Sql 或 存储过程)，默认为Sql语句</param>
        /// <returns>返回第一条匹配记录，并转换成相应的数据/实体对象</returns>
        public static T QueryFirst<T>(string sql, object param = null, CommandType? commandType = null, string dbConnectionName = null) where T : class
        {
            T result = default(T);

            using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
            {
                result = connection.QueryFirst<T>(sql, param, commandType: commandType);
            }

            return result;
        }

        /// <summary>
        /// 执行一条Sql查询语句；返回第一条匹配记录，没有匹配记录返回空，并转换成相应的数据/实体对象
        /// </summary>
        /// <typeparam name="T">数据/实体对象</typeparam>
        /// <param name="sql">Sql查询语句</param>
        /// <param name="param">Sql参数</param>
        /// <param name="commandType">Sql语句类型 (Sql 或 存储过程)，默认为Sql语句</param>
        /// <returns>返回第一条匹配记录，没有匹配记录返回空，并转换成相应的数据/实体对象</returns>
        public static T QueryFirstOrDefault<T>(string sql, object param = null, CommandType? commandType = null, string dbConnectionName = null) where T : class
        {
            T result = default(T);

            using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
            {
                result = connection.QueryFirstOrDefault<T>(sql, param, commandType: commandType);
            }

            return result;
        }

        /// <summary>
        /// 执行多条Sql查询语句；返回数据表格
        /// </summary>
        /// <param name="sql">Sql查询语句</param>
        /// <param name="param">Sql参数</param>
        /// <param name="commandType">Sql语句类型 (Sql 或 存储过程)，默认为Sql语句</param>
        /// <returns>返回数据表格</returns>
        public static GridReader QueryMultiple(string sql, object param = null, CommandType? commandType = null, string dbConnectionName = null)
        {
            GridReader result = default(GridReader);

            using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
            {
                result = connection.QueryMultiple(sql, param, commandType: commandType);
            }

            return result;
        }

        /// <summary>
        /// 执行一条Sql查询语句；返回唯一一条匹配记录，并转换成相应的数据/实体对象
        /// </summary>
        /// <typeparam name="T">数据/实体对象</typeparam>
        /// <param name="sql">Sql查询语句</param>
        /// <param name="param">Sql参数</param>
        /// <param name="commandType">Sql语句类型 (Sql 或 存储过程)，默认为Sql语句</param>
        /// <returns>返回唯一一条匹配记录，并转换成相应的数据/实体对象</returns>
        public static T QuerySingle<T>(string sql, object param = null, CommandType? commandType = null, string dbConnectionName = null) where T : class
        {
            T result = default(T);

            using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
            {
                result = connection.QuerySingle<T>(sql, param, commandType: commandType);
            }

            return result;
        }

        /// <summary>
        /// 执行一条Sql查询语句；返回唯一一条匹配记录，没有匹配记录返回空，并转换成相应的数据/实体对象
        /// </summary>
        /// <typeparam name="T">数据/实体对象</typeparam>
        /// <param name="sql">Sql查询语句</param>
        /// <param name="param">Sql参数</param>
        /// <param name="commandType">Sql语句类型 (Sql 或 存储过程)，默认为Sql语句</param>
        /// <returns>返回唯一一条匹配记录，没有匹配记录返回空，并转换成相应的数据/实体对象</returns>
        public static T QuerySingleOrDefault<T>(string sql, object param = null, CommandType? commandType = null, string dbConnectionName = null) where T : class
        {
            T result = default(T);

            using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
            {
                result = connection.QuerySingleOrDefault<T>(sql, param, commandType: commandType);
            }

            return result;
        }

        /// <summary>
        /// 执行一条Sql语句，查询和非查询语句均可(插入语句后跟一个Select XXX可返回插入行主键)；返回第一行第一列的值
        /// </summary>
        /// <typeparam name="T">返回值类型</typeparam>
        /// <param name="sql">Sql查询语句</param>
        /// <param name="param">Sql参数</param>
        /// <param name="commandType">Sql语句类型 (Sql 或 存储过程)，默认为Sql语句</param>
        /// <returns>第一行第一列的值</returns>
        public static T ExecuteScalar<T>(string sql, object param = null, CommandType? commandType = null, string dbConnectionName = null)
        {
            T result = default(T);

            var type = typeof(T);
            if (type == typeof(string) || type.IsValueType || type.IsEnum)
            {
                using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
                {
                    result = connection.ExecuteScalar<T>(sql, param, commandType: commandType);
                }
            }

            return result;
        }

        /// <summary>
        /// 执行一条Sql查询语句；返回源数据
        /// </summary>
        /// <param name="sql">Sql查询语句</param>
        /// <param name="param">Sql参数</param>
        /// <param name="commandType">Sql语句类型 (Sql 或 存储过程)，默认为Sql语句</param>
        /// <returns>源数据</returns>
        public static IDataReader ExecuteReader(string sql, object param = null, CommandType? commandType = null, string dbConnectionName = null)
        {
            IDataReader result = null;

            using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
            {
                result = connection.ExecuteReader(sql, param, commandType: commandType);
            }

            return result;
        }

        /// <summary>
        /// 执行一条CRUD Sql语句；返回受影响行数
        /// </summary>
        /// <param name="sql">CRUD Sql语句</param>
        /// <param name="param">Sql参数</param>
        /// <param name="transaction">事务对象(如需要)</param>
        /// <param name="commandType">Sql语句类型 (Sql 或 存储过程)，默认为Sql语句</param>
        /// <returns>受影响行数</returns>
        public static int Execute(string sql, object param = null, IDbTransaction transaction = null, CommandType? commandType = null, string dbConnectionName = null)
        {
            int affectedRows = 0;

            if (transaction != null)
            {
                transaction.Connection.Execute(sql, param, transaction, commandType: commandType);
            }
            else
            {
                using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
                {
                    affectedRows = connection.Execute(sql, param, transaction, commandType: commandType);
                }
            }
            
            return affectedRows;
        }        

        /// <summary>
        /// 执行事务
        /// </summary>
        /// <param name="sqls">多条Sql语句</param>
        /// <returns>成功：true, 失败：false</returns>
        public static bool ExecuteTransaction(IEnumerable<string> sqls, string dbConnectionName = null)
        {
            using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
            {
                if (connection.State == ConnectionState.Closed)
                    connection.Open();

                using (var ts = connection.BeginTransaction())
                {
                    try
                    {
                        foreach (var sql in sqls)
                        {
                            connection.Execute(sql, transaction: ts);
                        }
                        
                        ts.Commit();
                    }
                    catch (Exception ex)
                    {
                        //回滚事物
                        ts.Rollback();
                        connection.Close();
                        connection.Dispose();
                        throw ex;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// 执行事务
        /// </summary>
        /// <param name="sqlAndParams">多条Sql语句和Sql参数键值对</param>
        /// <returns></returns>
        public static bool ExecuteTransaction(IEnumerable<Tuple<string, object>> sqlAndParams, string dbConnectionName = null)
        {
            using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
            {
                if (connection.State == ConnectionState.Closed)
                    connection.Open();

                using (var ts = connection.BeginTransaction())
                {
                    try
                    {
                        foreach (var sqlAndParam in sqlAndParams)
                        {
                            connection.Execute(sqlAndParam.Item1, sqlAndParam.Item2, transaction: ts);
                        }

                        ts.Commit();
                        return true;
                    }
                    catch (Exception ex)
                    {
                        //回滚事物
                        ts.Rollback();
                        connection.Close();
                        connection.Dispose();
                        throw ex;
                    }
                }
            }
        }

        /// <summary>
        /// 执行事务
        /// </summary>
        /// <param name="transaction">一系列数据库操作</param>
        /// <returns>成功：true, 失败：false</returns>
        public static bool ExecuteTransaction(Action<IDbTransaction> transaction, string dbConnectionName = null)
        {
            using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
            {
                if (connection.State == ConnectionState.Closed)
                    connection.Open();

                using (var ts = connection.BeginTransaction())
                {
                    try
                    {
                        transaction.Invoke(ts);
                        ts.Commit();
                    }
                    catch (Exception ex)
                    {
                        //回滚事物
                        ts.Rollback();
                        connection.Close();
                        connection.Dispose();
                        throw ex;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// 执行事务；返回受影响行数
        /// </summary>
        /// <param name="transaction">一系列数据库操作</param>
        /// <returns>受影响行数</returns>
        public static int ExecuteTransaction(Func<IDbTransaction, int> transaction, string dbConnectionName = null)
        {
            int affectedRows = 0;

            using (var connection = DbConnectionFactory.Instance.GetDbConnection(dbConnectionName))
            {
                if (connection.State == ConnectionState.Closed)
                    connection.Open();

                using (var ts = connection.BeginTransaction())
                {
                    try
                    {
                        affectedRows = transaction.Invoke(ts);
                        ts.Commit();
                    }
                    catch (Exception ex)
                    {
                        //回滚事物
                        ts.Rollback();
                        connection.Close();
                        connection.Dispose();
                        throw ex;
                    }
                }
            }

            return affectedRows;
        }
    }
}
