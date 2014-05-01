using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NDbfReader
{
  /// <summary>
  /// Compatibility class for original NDbfReader class of eXavera
  /// https://github.com/eXavera/NDbfReader
  /// </summary>
  public class Reader
  { 
    private DbfTable dbfTable;
    private int      nextRecNo;
    private DbfRow   row_;

    public bool      skipDeleted = true;                                                    // leave out deleted rows from result 

    internal Reader(DbfTable dbfTable, int startRecNo = 0)
    {
      if (dbfTable == null)
      {
        throw ExceptionFactory.CreateArgumentException("dbfTable", "null parameter");
      }


      this.dbfTable  = dbfTable;
      this.nextRecNo = Math.Max(startRecNo, 0);

      this.skipDeleted = dbfTable.skipDeleted;
    }

    public bool Read()
    {
      bool skip;

      do 
      {
        row_ = dbfTable.GetRow(nextRecNo, false);                                             // Don't throw an exception, returns null if record not found

        nextRecNo++;
        skip = false;

        if (skipDeleted)
        {
          if (row_ != null)
          {
            skip = row_.deleted;
          }
        }

      } while (skip);

      return (row_ != null);
    }

    public DbfRow row
    {
      get { return row_; }
    }

    #region field read --------------------------------------------------------------------------------------
        
    public string GetString(string columnName)
    {
      if (row.IsNull(columnName))
      {
        return null;
      }

      return row.GetString(columnName);
    }

    public string GetString(IColumn column)
    {
      if (row.IsNull(column))
      {
        return null;
      }

      return row.GetString(column);
    }


    public decimal? GetDecimal(string columnName)
    {
      if (row.IsNull(columnName))
      {
        return null;
      }

      return row.GetDecimal(columnName);
    }

    public decimal? GetDecimal(IColumn column)
    {
      if (row.IsNull(column))
      {
        return null;
      }

      return row.GetDecimal(column);
    }


    public DateTime? GetDate(string columnName)
    {
      if (row.IsNull(columnName))
      {
        return null;
      }

      return row.GetDate(columnName);
    }

    public DateTime? GetDate(IColumn column)
    {
      if (row.IsNull(column))
      {
        return null;
      }

      return row.GetDate(column);
    }
    

    public bool? GetBoolean(string columnName)
    {
      if (row.IsNull(columnName))
      {
        return null;
      }

      return row.GetBoolean(columnName);
    }

    public bool? GetBoolean(IColumn column)
    {
      if (row.IsNull(column))
      {
        return null;
      }

      return row.GetBoolean(column);
    }
    

    public int GetInt32(string columnName)
    {
      return row.GetInt32(columnName);
    }

    public virtual int GetInt32(IColumn column)
    {
      return row.GetInt32(column);
    }
    #endregion
  }
}
