using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NDbfReaderEx
{ // new from 1.3 version
 
  /* 
  public class DbfTablePoco<T> : DbfTable, IDisposable, IEnumerable<T> where T : class, new()                          // new from 1.3 version
  {
    #region Constructor ---------------------------------------------------------------------------------

    protected DbfTablePoco(Stream stream, Encoding encoding = null) : base(stream, encoding)
    {
      
    }

    //static DbfTable()
    //{
    //  Initialize_CodepageCodes_Encoding();
    //}
    
    #endregion

    #region IEnumerable

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    public new IEnumerator<T> GetEnumerator()
    {
      for (int i = 0; i < this.recCount; i++)
      {
        DbfRow row = this.GetRow(i);

        if (this.skipDeleted && row.deleted)
        {
          continue;
        }

        yield return DbfTable.Get<T>(row);
      }
    }

    public new IEnumerator<T> GetEnumerator(bool skipDeleted)
    {
      for (int i = 0; i < this.recCount; i++)
      {
        DbfRow row = this.GetRow(i);

        if (skipDeleted && row.deleted)
        {
          continue;
        }

        yield return DbfTable.Get<T>(row);
      }
    }
    #endregion
  }
  */

  /*
  public class DbfTable<T> : DbfTable, IDisposable, IEnumerable<T> where T : class, new()                          // new from 1.3 version
  {
    #region Constructor ---------------------------------------------------------------------------------

    protected DbfTable(Stream stream, Encoding encoding = null) : base(stream, encoding)
    {
      
    }

    //static DbfTable()
    //{
    //  Initialize_CodepageCodes_Encoding();
    //}
    
    #endregion

    #region IEnumerable

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    public new IEnumerator<T> GetEnumerator()
    {
      for (int i = 0; i < this.recCount; i++)
      {
        DbfRow row = this.GetRow(i);

        if (this.skipDeleted && row.deleted)
        {
          continue;
        }

        yield return DbfTable.Get<T>(row);
      }
    }

    public new IEnumerator<T> GetEnumerator(bool skipDeleted)
    {
      for (int i = 0; i < this.recCount; i++)
      {
        DbfRow row = this.GetRow(i);

        if (skipDeleted && row.deleted)
        {
          continue;
        }

        yield return DbfTable.Get<T>(row);
      }
    }
    #endregion
  }
  */

  public partial class DbfTable                                                     // new from 1.3 version
  {
    public IEnumerator<T> GetEnumerator<T>() where T : class, new()
    {
      for (int i = 0; i < this.recCount; i++)
      {
        DbfRow row = this.GetRow(i);

        if (this.skipDeleted && row.deleted)
        {
          continue;
        }

        yield return DbfTable.Get<T>(row);
      }
    }

    public IEnumerator<T> GetEnumerator<T>(bool skipDeleted) where T : class, new()
    {
      for (int i = 0; i < this.recCount; i++)
      {
        DbfRow row = this.GetRow(i);

        if (skipDeleted && row.deleted)
        {
          continue;
        }

        yield return DbfTable.Get<T>(row);
      }
    }

    //    

    public static T Get<T>(DbfRow row) where T : class, new()                          // new from 1.3 version
    {
      if (row == null)
      {
        return null;
      }

      T t = (T)Activator.CreateInstance(typeof(T), row);

      return t;
    }  

    public T Get<T>(int recno) where T : class, new()                                 // new from 1.3 version
    {
      DbfRow row = this.GetRow(recno);   

      T t = (T)Activator.CreateInstance(typeof(T), row);

      return t;
    }     
  }
}
