using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace NDbfReader
{
  class DbfTableReader : DbfTable
  {
    #region Constructor / variables -----------------------------------------------------------------------
    
    private DbfRow _actRow = null;

    public bool RecNoOverflowException = true;


    protected DbfTableReader(Stream stream, Encoding encoding = null) :
      base(stream, encoding)
    {
      if (this.recCount > 0)
      {
        recNo = 0;                                                                      // go to first dbf row // operating mode like dBase language :)
      }               
    }
    #endregion

    #region row positioning  ------------------------------------------------------------------------------

    /// <summary>
    /// Content of actRow has valid?
    /// </summary>
    public bool eof { get { return _actRow == null; } }                                  // syntax/operating mode like dBase language :)

    /// <summary>
    /// Get or set record number of DBF rows and set right content of actRow
    /// </summary>
    public int recNo                                                                    // syntax/operating mode like dBase language :)
    {
      get
      {
        if (_actRow == null)
        {
          return -1;
        }

        return _actRow.recNo;
      }

      set
      {
        _actRow = GetRow(value, RecNoOverflowException);                                  // Throws it an exception is new position invalid (too hight)?
      }
    }

    #endregion
  }
}
