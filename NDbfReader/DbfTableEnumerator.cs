using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace NDbfReaderEx
{
  public class DbfTableRowEnumerator : IEnumerable, IEnumerable<DbfRow>
  {
    #region

    private DbfTable  table;                                      // data source
    private bool      skipDeleted;                                // table filter (only not deleted)
    private int       firstRecNo;                                 // table filter (record limit)
    private int       lastRecNo;                                  // table filter (record limit)
    
    #endregion

    public DbfTableRowEnumerator(DbfTable table, bool? skipDeleted = null, int? firstRecNo = null, int? lastRecNo = null)
    {
      this.table       = table;
      this.skipDeleted = skipDeleted ?? table.skipDeleted;
      this.firstRecNo  = firstRecNo  ?? 0;
      this.lastRecNo   = lastRecNo   ?? table.recCount - 1;

      if (this.firstRecNo < 0)
      {
        this.firstRecNo = 0;
      }

      if (this.lastRecNo >= table.recCount)
      {
        this.lastRecNo = table.recCount - 1;
      }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    public IEnumerator<DbfRow> GetEnumerator()
    {
      for (int i = firstRecNo; (i <= lastRecNo); i++)
      {
        DbfRow row = table.GetRow(i);

        if (this.skipDeleted && row.deleted)
        {
          continue;
        }

        yield return row;
      }
    }
  }

  public class DbfTablePocoEnumerator<T> : IEnumerable, IEnumerable<T> where T : class, new()
  {
    #region

    private DbfTable  table;                                      // data source
    private bool      skipDeleted;                                // table filter (only not deleted)
    private int       firstRecNo;                                 // table filter (record limit)
    private int       lastRecNo;                                  // table filter (record limit)
    
    #endregion

    public DbfTablePocoEnumerator(DbfTable table, bool? skipDeleted = null, int? firstRecNo = null, int? lastRecNo = null)
    {
      this.table       = table;
      this.skipDeleted = skipDeleted ?? table.skipDeleted;
      this.firstRecNo  = firstRecNo  ?? 0;
      this.lastRecNo   = lastRecNo   ?? table.recCount - 1;

      if (this.firstRecNo < 0)
      {
        this.firstRecNo = 0;
      }

      if (this.lastRecNo >= table.recCount)
      {
        this.lastRecNo = table.recCount - 1;
      }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    public IEnumerator<T> GetEnumerator()
    {
      for (int i = firstRecNo; (i <= lastRecNo); i++)
      {
        DbfRow row = table.GetRow(i);

        if (this.skipDeleted && row.deleted)
        {
          continue;
        }

        yield return row.Get<T>();
      }
    }
  }
}