﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
//
using Newtonsoft.Json;
namespace H2F.Framework.Common.Extension
{
    /// <summary>
    /// 功能：常用json操作扩展方法，基于Newtonsoft.Json提供常用序列化和反序列化快捷功能
    /// 作者：何蛟
    /// 创建时间： 2018-12-9 12:05
    /// </summary>
    public static class JsonExtension
    {
        public static string ToJson(this object obj, bool ignoreNull = false)
        {
            if (obj.IsNull())
            {
                return null;
            }
            else
            {
                return JsonConvert.SerializeObject(obj, Formatting.None, new JsonSerializerSettings
                {
                    DateFormatString = "yyyy-MM-dd HH:mm:ss",
                    NullValueHandling = (ignoreNull ? NullValueHandling.Ignore : NullValueHandling.Include)
                });
            }
        }

        public static T FromJson<T>(this string jsonStr)
        {
            return jsonStr.IsNullOrEmpty() ? default(T) : JsonConvert.DeserializeObject<T>(jsonStr);
        }

        public static byte[] SerializeUtf8(this string str)
        {
            return str.IsNull() ? null : Encoding.UTF8.GetBytes(str);
        }

        public static string DeserializeUtf8(this byte[] stream)
        {
            return stream == null ? null : Encoding.UTF8.GetString(stream);
        }

        public static byte[] SerializeUtf8JsonFormat(this object obj)
        {
            var json = obj.ToJson();
            return json.IsNull() ? null : json.SerializeUtf8();
        }

        public static T DeserializeUtf8JsonFormat<T>(this byte[] stream)
        {
            string str;
            if (stream == null)
            {
                return default(T);
            }
            else
            {
                str = stream.DeserializeUtf8();
                return str.IsNullOrEmpty() ? default(T) : str.FromJson<T>();
            }
        }

    }
}
