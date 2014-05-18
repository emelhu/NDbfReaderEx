using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

// contact:  emel@emel.hu
// If you have question or comment send me it.
// English is not my native language, please send criticism and/or correction.

namespace NDbfReaderEx
{
  /// <summary>
  /// Compatibility class for original NDbfReader class of eXavera
  /// </summary>
  public class ClipperReader
  { 
    private DbfTable _dbfTable;
    private DbfRow   _row = null;

    public bool      skipDeleted;                                                             // leave out deleted rows from result 
    public bool      recNoOverflowException = true;                                           // 

    internal ClipperReader(DbfTable dbfTable, bool skipDeleted)
    {
      if (dbfTable == null)
      {
        throw ExceptionFactory.CreateArgumentException("dbfTable", "null parameter");
      }


      this._dbfTable   = dbfTable;
      this.skipDeleted = skipDeleted;

      Top();
    }
    
    #region row positioning  ------------------------------------------------------------------------------

    /// <summary>
    /// Content of actRow has valid?
    /// </summary>
    public bool eof { get { return (_row == null); } }                                  // syntax/operating mode like dBase language :)

    /// <summary>
    /// Get or set record number of DBF rows and set right content of actual row
    /// </summary>
    public int recNo                                                                    // syntax/operating mode like dBase language :)
    {
      get
      {
        if (_row == null)
        {
          return -1;
        }

        return _row.recNo;
      }

      set
      {
        _row = _dbfTable.GetRow(value, recNoOverflowException);                      // Throws it an exception is new position invalid (too hight)?
      }
    }

    public DbfRow row
    {
      get { return _row; }
    }

    public int recCount                                                              // syntax/operating mode like dBase language :)
    {
      get
      {
        return _dbfTable.recCount;
      }
    }
    #endregion

    #region row marching ------------------------------------------------------------------------------

    public bool Next(int step = 1)
    {
      return MarchingMore(true, step);
    }

    public bool Prev(int step = 1)
    {
      return MarchingMore(false, step * -1);
    }

    public bool Top()
    {
      recNo = 0;

      if (!eof && skipDeleted && row.deleted)
      {
        MarchingOne(true);
      }
      
      return eof;
    }

    public bool Bottom()
    {
      recNo = recCount - 1;

      if (!eof && skipDeleted && row.deleted)
      {
        MarchingOne(false);
      }
      
      return eof;
    }

    #endregion

    #region private marching ------------------------------------------------------------------------------

    private bool MarchingOne(bool forward)
    {
      if (eof)
      {
        return false;
      }

      //

      int direction = forward ? 1 : -1;

      do
      {
        _row = _dbfTable.GetRow(_row.recNo + direction, false);                         // Don't throw an exception, returns null if record not found

      } while (skipDeleted && (_row != null) && _row.deleted);

      return (_row != null);
    }

    private bool MarchingMore(bool forward, int step)
    {
      if (step < 1)
      {
        throw ExceptionFactory.CreateArgumentException("MarchingMore/step", "Step parameter value must more then 0 !");
      }


      for (int i = 0; i < step; i++)
      {
        if (! MarchingOne(forward))
        {
          return false;
        }
      }

      return eof;
    }
    #endregion

    #region field read --------------------------------------------------------------------------------------

    public string GetString(string columnName)
    {
      return row.GetString(columnName);
    }

    public string GetString(IColumn column)
    {
      return row.GetString(column);
    }


    public decimal GetDecimal(string columnName)
    {
      return row.GetDecimal(columnName);
    }

    public decimal GetDecimal(IColumn column)
    {
      return row.GetDecimal(column);
    }


    public DateTime GetDate(string columnName)
    {
      return row.GetDate(columnName);
    }

    public DateTime? GetDate(IColumn column)
    {
      return row.GetDate(column);
    }
    

    public bool GetBoolean(string columnName)
    {
      return row.GetBoolean(columnName);
    }

    public bool GetBoolean(IColumn column)
    {
      return row.GetBoolean(column);
    }
    

    public int GetInt32(string columnName)
    {
      return row.GetInt32(columnName);
    }

    public int GetInt32(IColumn column)
    {
      return row.GetInt32(column);
    }


    public bool IsNull(string columnName)
    {
      return row.IsNull(columnName);
    }

    public bool IsNull(IColumn column)
    {
      return row.IsNull(column);
    }
    #endregion
  }
}
