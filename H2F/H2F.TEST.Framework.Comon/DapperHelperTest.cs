using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using H2F.Framework.Common.Helper;
using System.Collections.Generic;
using System.Transactions;
using System.Data.Common;
using System.Linq;

namespace H2F.TEST.Framework.Common
{
    /// <summary>
    /// DapperHelper 单元测试
    /// </summary>
    [TestClass]
    public class DapperHelperTest
    {
        /// <summary>
        /// 数据库表实体，测试用
        /// </summary>
        internal class User
        {
            /// <summary>
            /// 主键，非自增
            /// </summary>
            public int Id { get; set; }

            public string Name { get; set; }
        }

        /// <summary>
        /// 删除，清空表数据
        /// </summary>
        [TestMethod]
        public void TestDeleteAll()
        {
            var effectedRows = DapperHelper.Execute("delete from user");
            bool result = effectedRows >= 0 ? true : false;

            Assert.AreEqual(true, result);
        }

        /// <summary>
        /// 取第一行第一列值
        /// </summary>
        [TestMethod]
        public void TestExecuteScalar()
        {
            TestDeleteAll();

            Assert.AreEqual(0, DapperHelper.ExecuteScalar<int>("select count(Id) from user"));
        }

        /// <summary>
        /// 非条件查询
        /// </summary>
        [TestMethod]
        public void TestQueryWithoutFilter()
        {
            TestDeleteAll();
            var result = DapperHelper.Query<User>("select Id, Name from user");
            
            Assert.AreEqual(0, result.Count());
        }

        /// <summary>
        /// 条件查询
        /// </summary>
        [TestMethod]
        public void TestQueryWithFilter()
        {
            TestDeleteAll();
            
            //单个插入
            User user = new User { Id = 1, Name = "Test 01" };
            DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", user);

            var result = DapperHelper.Query<User>("select Id, Name from user where Id = 1");

            Assert.AreEqual(1, result.Count());
        }

        /// <summary>
        /// 插入
        /// </summary>
        [TestMethod]
        public void TestInsert()
        {
            //清表
            TestDeleteAll();

            //单个插入
            User user = new User { Id = 1, Name = "Test 01" };
            DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", user);

            //批量插入
            List<User> users = new List<User>();
            users.Add(new User { Id =  2, Name = "Test 02" });
            users.Add(new User { Id = 3, Name = "Test 03" });
            DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", users);

            //查询数据行
            Assert.AreEqual(3, DapperHelper.ExecuteScalar<int>("select count(Id) from user"));
        }

        /// <summary>
        /// 插入并返回主键(忽略该测试用例，因为该测试用例的表主键不是自增的)
        /// </summary>
        [TestMethod]
        public void TestInsertAndGetKey()
        {
            //清表
            TestDeleteAll();

            //单个插入
            User user = new User { Id = 1, Name = "Test 01" };
            var userId = DapperHelper.ExecuteScalar<int>("insert into user(Id,Name) values(@Id, @Name);SELECT LAST_INSERT_ID();", user);
            
            //查询数据行
            //Assert.AreEqual(1, userId);
        }

        /// <summary>
        /// 更新
        /// </summary>
        [TestMethod]
        public void TestUpdate()
        {
            TestDeleteAll();

            DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 1, Name = "Test 01" });
            DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 2, Name = "Test 02" });
            DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 3, Name = "Test 03" });

            DapperHelper.Execute("update user set Name = @Name where Id = @Id ", new User { Id = 1, Name = "Test 01 has been updated." });

            Assert.AreEqual("Test 01 has been updated.", DapperHelper.ExecuteScalar<string>("select Name from user where Id = @Id", new User { Id = 1 }));
        }

        /// <summary>
        /// 插入
        /// </summary>
        [TestMethod]
        public void TestQueryWithDataReader()
        {
            //清表
            TestDeleteAll();

            //单个插入
            User user = new User { Id = 1, Name = "Test 01" };
            DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", user);

            List<User> users = new List<User>();            

            var dataReader = DapperHelper.ExecuteReader("select `Id`, `Name` from `user`", new { });            
            if (dataReader != null && dataReader.FieldCount > 0)
            {
                while (dataReader.Read())
                {
                    users.Add(new User
                    {
                        Id = (int)Convert.ChangeType(dataReader["Id"] ?? 0, typeof(int)),
                        Name = (dataReader["Name"] ?? "").ToString()
                    });
                }
            }

            Assert.AreEqual(1, users.Count);
        }

        [TestMethod]
        public void TestTransactionWithMultipleSql()
        {
            TestDeleteAll();

            try
            {
                List<string> sqls = new List<string> {
                    "insert into user(Id,Name) values(1, 'Test 01')",
                    "insert into user(Id,Name) values(2, 'Test 02')",
                    //"insert into user(Id,Name) values(2, 'Test 02')",     //可测试事务是否正常
                    "insert into user(Id,Name) values(3, 'Test 03')"};

                DapperHelper.ExecuteTransaction(sqls);

                //查询数据行
                Assert.AreEqual(3, DapperHelper.ExecuteScalar<int>("select count(Id) from user"));
            }
            catch (Exception)
            {
                //查询数据行
                Assert.AreEqual(0, DapperHelper.ExecuteScalar<int>("select count(Id) from user"));
            }
        }

        /// <summary>
        /// 事务，自定义逻辑，用TransactionScope
        /// </summary>
        [TestMethod]
        public void TestTransactionWithTransactionScope()
        {
            TestDeleteAll();

            try
            {
                using (var tran = new TransactionScope())
                {
                    DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 1, Name = "Test 01" });
                    DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 2, Name = "Test 02" });
                    DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 2, Name = "Test 02" });    //错误记录                    
                    DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 3, Name = "Test 03" });

                    tran.Complete();
                }

                //查询数据行
                Assert.AreEqual(3, DapperHelper.ExecuteScalar<int>("select count(Id) from user"));
            }
            catch (Exception)
            {
                //查询数据行
                Assert.AreEqual(0, DapperHelper.ExecuteScalar<int>("select count(Id) from user"));
            }
        }

        /// <summary>
        /// 事务, 自定义逻辑放在委托里面
        /// </summary>
        [TestMethod]
        public void TestTransactionWithAction()
        {
            TestDeleteAll();

            try
            {
                DapperHelper.ExecuteTransaction(tran => {
                    DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 1, Name = "Test 01" }, tran);
                    DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 2, Name = "Test 02" }, tran);
                    DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 2, Name = "Test 02" }, tran);  //错误记录        
                    DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 3, Name = "Test 03" }, tran);
                });

                //查询数据行
                Assert.AreEqual(3, DapperHelper.ExecuteScalar<int>("select count(Id) from user"));
            }
            catch (Exception)
            {
                //查询数据行
                Assert.AreEqual(0, DapperHelper.ExecuteScalar<int>("select count(Id) from user"));
            }
        }

        /// <summary>
        /// 事务，自定义逻辑放在委托里面，返回受影响行数
        /// </summary>
        [TestMethod]
        public void TestExcuteTransactionWithFunc()
        {
            TestDeleteAll();

            try
            {
                var effectRows = DapperHelper.ExecuteTransaction((tran) =>
                {
                    var effectRow = 0;
                    effectRow += DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 1, Name = "Test 01" }, tran);
                    effectRow += DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 2, Name = "Test 02" }, tran);
                    effectRow += DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 2, Name = "Test 02" }, tran);  //错误记录，屏蔽可测正常情况
                    effectRow += DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", new User { Id = 3, Name = "Test 03" }, tran);
                    return effectRow;
                });

                Assert.AreEqual(3, effectRows);
                //查询数据行
                Assert.AreEqual(3, DapperHelper.ExecuteScalar<int>("select count(Id) from user"));
            }
            catch (Exception)
            {
                //查询数据行
                Assert.AreEqual(0, DapperHelper.ExecuteScalar<int>("select count(Id) from user"));
            }
        }

        /// <summary>
        /// 批量插入(高并发测试)
        /// </summary>
        [TestMethod]
        public void TestInsertBatch()
        {
            TestDeleteAll();

            int count = 10000;

            List<System.Threading.Tasks.Task> tasks = new List<System.Threading.Tasks.Task>();

            List<User> users = new List<User>();

            for (int i = 0; i < count; i++)
            {
                users.Add(new User { Id = i, Name = i.ToString() });
            }

            foreach (var item in users)
            {
                tasks.Add(System.Threading.Tasks.Task.Factory.StartNew(() => {  DapperHelper.Execute("insert into user(Id,Name) values(@Id, @Name)", item); }));
            }

            System.Threading.Tasks.Task.WaitAll(tasks.ToArray());
            
            //查询数据行
            Assert.AreEqual(count, DapperHelper.ExecuteScalar<int>("select count(Id) from user"));
        }
    }
}
 