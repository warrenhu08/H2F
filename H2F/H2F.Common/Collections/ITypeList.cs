﻿using System;
using System.Collections.Generic;
using System.Text;

namespace H2F.Standard .Common.Collections
{
    public interface ITypeList : ITypeList<object>
    {
    }

    public interface ITypeList<in TBaseType> : IList<Type>
    {
        void Add<T>() where T : TBaseType;

        bool Contains<T>() where T : TBaseType;

        void Remove<T>() where T : TBaseType;
    }
}
