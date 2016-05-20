using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Text;

// contact:  emel@emel.hu
// If you have question or comment send to me it.
// English is not my native language, please send criticism and/or correction too.

// git push -u origin master
// https://github.com/emelhu/NDbfReader_eMeL_Fork.git
// https://github.com/emelhu/NDbfReaderEx.git

// Fileformat info:
// http://www.dbf2002.com/dbf-file-format.html                          -- DBF
// http://www.cs.cmu.edu/~varun/cs315p/xbase.txt                        -- DBF & DBT & NDX/NTX/etc.
// http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm              -- memo DBT
// http://msdn.microsoft.com/en-us/library/aa975374(v=vs.71).aspx       -- FoxPro memo FPT
// http://ulisse.elettra.trieste.it/services/doc/dbase/DBFstruct.htm    -- DBF, a few words from DBT
// http://www.clicketyclick.dk/databases/xbase/format/index.html        -- DBT
// http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm              -- dbase7 table

// http://tmpvar.com/markdown.html   for prewiew of *.MD files :)
// https://help.github.com/articles/github-flavored-markdown     -- help

// ftp://fship.com/pub/multisoft/flagship/docu/dbfspecs.txt
// http://www.zelczak.com/clipp_en.htm                                  --- NTX ctructure
// http://shapelib.maptools.org/codepage.html

// http://www.clicketyclick.dk/databases/xbase/format/index.html
// https://en.wikipedia.org/wiki/DBase
// http://www.johnbrown.com.au/approach/webfaq04040210.html
// http://devzone.advantagedatabase.com/dz/webhelp/advantage9.0/server1/dbf_field_types_and_specifications.htm   FoxPro memo len 4 byte
// http://msdn.microsoft.com/en-us/library/st4a0s68%28VS.80%29.aspx

namespace NDbfReaderEx
{
  /// <summary>
  /// Represents a dBASE table.  Use one of the Open static methods to create a new instance.
  /// </summary>
  /// <example>
  /// <code>
  /// using(var table = new DbfTable.Open(@"D:\Example\table.dbf", Encoding.GetEncoding(437)))
  /// {
  ///     ...
  /// }
  /// </code>
  /// </example>
  public partial class DbfTable : IDisposable, IEnumerable<DbfRow>
  {
    #region variables -------------------------------------------------------------------------------------

    private  readonly Stream        _stream;
    internal readonly IColumn[]     _columns;

    private           DbfHeader     _header;
    public  readonly  Guid          dbfTableClassID = Guid.NewGuid();

    private           MemoFileBase  _memoFile;

    private           List<IndexFileBase> _indexFiles;

    private           bool         _disposed   = false;

    public            bool         skipDeleted = true;                             // leave out deleted rows from result (Enumerate)
    public            bool         memoStreamCloseWhenDispose = true;              // detached rows can or can't read it after DbfTable closed.    
    #endregion
   
    private DbfTableParameters parameters;

    public DbfTableParametersReadOnly parametersReadOnly
    {
      get
      {
        return parameters.GetReadOnly();
      }
    }
    #region Constructor ---------------------------------------------------------------------------------

    protected DbfTable(Stream stream, DbfTableParameters parameters)
    {
      if (stream == null)
      {
        throw new ArgumentNullException("stream");
      }
      
      if (! stream.CanRead)
      {
        throw ExceptionFactory.CreateArgumentException("stream", "The stream does not allow reading (CanRead property returns false).");
      }

      if (! stream.CanSeek)
      {
        throw ExceptionFactory.CreateArgumentException("stream", "The stream does not allow reading (CanSeek property returns false).");
      }

      this._stream          = stream;
      this.parameters       = parameters;
    
      RefreshHeaderInfo();

      //      

      if (this.parameters.encoding == null)
      {
        this.parameters.encoding = ReadDbfHeader_Encoding(_header.codepageCode);

        if (this.parameters.encoding == null)
        {
          throw new Exception("DbfTable: the DBF file don't contains codepage information!");
        }
      }

      this._columns  = ReadDbfColumns(_stream, this.parameters.encoding, _header.newHeaderStructure, this.parameters.openMemo);  

      //

      int calcLen = _header.firstRecordPosition + (_header.recCount * _header.rowLength) + 1;

      if ((stream.Length < calcLen - 1) || (stream.Length > calcLen + 1))  
      { // dBase & Clipper different (There is or there isn't a 0x1F character at end of DBF data file .
        throw ExceptionFactory.CreateArgumentOutOfRangeException("DBF table", "Datafile length error! [got: {0} expected: {1}]", stream.Length, calcLen);
      }      
    }   
    #endregion

    #region Dbf file control  -------------------------------------------------------------------------------

    public void RefreshHeaderInfo()
    {
      _header = ReadDbfHeader(_stream, this.onlyDBase3Enabled);

      return;
    }

    #endregion

    #region Open/Create DBF table --------------------------------------------------------------------------

    public static DbfTable Open(string path, Encoding encoding = null, bool? openMemo = null, StrictHeader? strictHeader = null, DbfTableType tableType = DbfTableType.Undefined)
    {
      var parameters = new DbfTableParameters(encoding, openMemo, strictHeader, tableType);

      return Open(path, parameters);
    }

    public static DbfTable Open(string path, DbfTableOpenMode openMode, Encoding encoding = null, bool? openMemo = null, StrictHeader? strictHeader = null, DbfTableType tableType = DbfTableType.Undefined)
    {
      DbfTableParameters parameters = new DbfTableParameters(openMode, encoding, openMemo, strictHeader, tableType);

      return Open(path, parameters);
    }

    /// <summary>
    /// Opens a table from the specified file.
    /// </summary>
    /// <param name="path">The file to be opened.</param>
    /// <returns>A table instance.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> is <c>null</c> or empty.</exception>
    /// <exception cref="NotSupportedException">The dBASE table constains one or more columns of unsupported type.</exception>
    public static DbfTable Open(string path, DbfTableParameters parameters)
    {
      if (string.IsNullOrEmpty(path))
      {
        throw new ArgumentNullException("path");
      }

      var stream  = new FileStream(path, FileMode.Open, parameters.fileAccess, parameters.fileShare);
      var dbfFile = new DbfTable(stream, parameters);

      dbfFile.dataFileName = path;

      if (dbfFile.isExistsMemoField && parameters.openMemo)
      { // If exist a memo field and it is opened from a file, so I can find DBT/FPT/etc. memo file too.
        dbfFile.JoinMemoFile();
      }

      return dbfFile;
    }   
       
    public static DbfTable Open(Stream stream, DbfTableParameters parameters)
    {
      return new DbfTable(stream,  parameters);
    }

    /// <summary>
    /// Opens a table from the specified stream.
    /// </summary>
    /// <param name="stream">The stream of dBASE table to open. The stream is closed when the returned table instance is disposed.</param>
    /// <returns>A table instance.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="stream"/> is <c>null</c> or <paramref name="headerLoader"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="stream"/> does not allow reading.</exception>
    /// <exception cref="NotSupportedException">The dBASE table constains one or more columns of unsupported type.</exception>
    public static DbfTable Open(Stream stream, Encoding encoding = null, bool openMemo = true, StrictHeader? strictHeader = null, DbfTableType tableType = DbfTableType.Undefined)
    {
      var parameters = new DbfTableParameters(encoding, openMemo, strictHeader, tableType, null, null);   
      
      return new DbfTable(stream,  parameters);
    }

    //

    /// <summary>
    /// Create a new DBF datafile.
    /// Create() can't overwrite exists file - it's a precautionary measure.
    /// </summary>
    /// <param name="path">A not exists filename.</param>
    /// <param name="columns">Definition of columns</param>
    /// <param name="encoding">Encoding for open created file. -- It mean too, CodepageCodes of new file will OEM</param>
    /// <returns></returns>
    public static DbfTable Create(string path, IEnumerable<ColumnDefinitionForCreateTable> columns, Encoding encoding, DbfTableType tableType = DbfTableType.Undefined)
    {
      if (string.IsNullOrEmpty(path))
      {
        throw new ArgumentNullException("path");
      }

      if (columns == null)
      {
        throw new ArgumentNullException("columns");
      }

      //

      var streamDBF = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);

      DbfTable dbfTable = CreateHeader_DBF(streamDBF, columns, tableType, CodepageCodes.OEM, encoding);

      if (dbfTable.isExistsMemoField)
      {
        MemoFileType memoType = dbfTable.DefaultMemoFileFormatForDbf();

        Stream streamMemo = CreateHeader_Memo(path, memoType);
        dbfTable.JoinMemoStream(streamMemo, memoType);
      }

      return dbfTable;  
    }

    /// <summary>
    /// Create a new DBF datafile.
    /// Create() can't overwrite exists file - it's a precautionary measure.
    /// </summary>
    /// <param name="path">A not exists filename.</param>
    /// <param name="columns">Definition of columns</param>
    /// <param name="codepageCode">Stored encoding information code, allways auto create appropriate encoding for open this file.</param>
    /// <returns></returns>
    public static DbfTable Create(string path, IEnumerable<ColumnDefinitionForCreateTable> columns, CodepageCodes codepageCode, DbfTableType tableType = DbfTableType.Undefined)
    {
      if (string.IsNullOrEmpty(path))
      {
        throw new ArgumentNullException("path");
      }

      if (columns == null)
      {
        throw new ArgumentNullException("columns");
      }

      //

      var stream = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);

      return CreateHeader_DBF(stream, columns, tableType, codepageCode);                    
    }
    #endregion

    #region Interface/Read dbf info  -----------------------------------------------------------------------
    
    /// <summary>
    /// Gets a list of all columns in the table.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The table is disposed.</exception>
    public ReadOnlyCollection<IColumn> columns
    {
      get
      {
        ThrowIfDisposed();

        return new ReadOnlyCollection<IColumn>(_columns);
      }
    }

    /// <summary>
    /// Count of record/row of DBF table
    /// </summary>    
    public int          recCount  { get { return _header.recCount; } }

    public DbfHeader    dbfHeader { get { return _header; } }

    //

    public bool isExistsMemoField
    {
      get
      {
        return (parameters.openMemo && Array.Exists(_columns, (c => c.dbfType == NativeColumnType.Memo)));
      }
    }

    public bool isEnabledMemoFields
    {
      get
      {
        return (parameters.openMemo && isExistsMemoField && (_memoFile != null));      
      }
    }

    //

    public string           dataFileName  { get; internal set; }                       // Don't forget an info, if available... good for open memo stream automatically
    public string           memoFileName  { get; internal set; }                       // Don't forget an info, if available...
    
    #endregion

    #region Technical  ------------------------------------------------------------------------------------

    /// <summary>
    /// Closes the underlying stream.
    /// </summary>
    public void Dispose()
    {
      if (! _disposed)
      {
        Disposing();
      }
    }

    /// <summary>
    /// Releases the underlying stream.
    /// <remarks>
    /// The method is called only when the <see cref="Dispose"/> method is called for the first time.
    /// You MUST always call the base implementation.
    /// </remarks>
    /// </summary>
    protected virtual void Disposing()
    {
      _disposed = true;

      _stream.Dispose();

      if (memoStreamCloseWhenDispose && (_memoFile != null))
      {
        _memoFile.Dispose();
      }

      //

      if (_indexFiles != null)
      {
        foreach (var indexFile in _indexFiles)
        {
          indexFile.Dispose();
        }
      }
    }

    /// <summary>
    /// Throws a <see cref="ObjectDisposedException"/> if the table is already disposed.
    /// </summary>
    protected void ThrowIfDisposed()
    {
      if (_disposed)
      {
        throw new ObjectDisposedException(GetType().FullName);
      }
    }
    #endregion

    #region Dbf reader/enumerator -------------------------------------------------------------------------
    
    /// <summary>
    /// Compatibility behavior for original NDbfReader class of eXavera
    /// https://github.com/eXavera/NDbfReader
    /// </summary>
    public Reader OpenReader(int startRecNo = 0)
    {
      return new Reader(this, startRecNo);                                                // skipDeleted inherited from this/DbfReader
    }

    public Reader OpenReader(bool skipDeleted, int startRecNo = 0)
    {
      Reader reader = new Reader(this, startRecNo);

      reader.skipDeleted = skipDeleted;

      return reader;
    }


    public ClipperReader OpenClipperReader()
    {
      return new ClipperReader(this, this.skipDeleted);                                     
    }

    public ClipperReader OpenClipperReader(bool skipDeleted)
    {
      ClipperReader reader = new ClipperReader(this, skipDeleted);

      return reader;
    }

    //

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    public IEnumerator<DbfRow> GetEnumerator()
    {
      for (int i = 0; i < this.recCount; i++)
      {
        DbfRow row = this.GetRow(i);

        if (this.skipDeleted && row.deleted)
        {
          continue;
        }

        yield return row;
      }
    }

    public DbfTableRowEnumerator RowEnumerator()                                   // new from 1.3 version
    {
      return new DbfTableRowEnumerator(this, null, null, null);
    }

    public DbfTableRowEnumerator RowEnumerator(bool? skipDeleted, int? firstRecNo = null, int? lastRecNo = null)                                   // new from 1.3 version
    {
      return new DbfTableRowEnumerator(this, skipDeleted, firstRecNo, lastRecNo);
    }

    public DbfTableRowEnumerator RowEnumerator(int firstRecNo, bool? skipDeleted = null, int? lastRecNo = null)                                   // new from 1.3 version
    {
      return new DbfTableRowEnumerator(this, skipDeleted, firstRecNo, lastRecNo);
    }

    public DbfTableRowEnumerator RowEnumerator(int firstRecNo, int lastRecNo, bool? skipDeleted = null)                                   // new from 1.3 version
    {
      return new DbfTableRowEnumerator(this, skipDeleted, firstRecNo, lastRecNo);
    }

    public DbfTablePocoEnumerator<T> PocoEnumerator<T>() where T : class, new()                          // new from 1.3 version
    {
      return new DbfTablePocoEnumerator<T>(this, null, null, null);
    }

    public DbfTablePocoEnumerator<T> PocoEnumerator<T>(bool? skipDeleted, int? firstRecNo = null, int? lastRecNo = null) where T : class, new()                          // new from 1.3 version
    {
      return new DbfTablePocoEnumerator<T>(this, skipDeleted, firstRecNo, lastRecNo);
    }

    public DbfTablePocoEnumerator<T> PocoEnumerator<T>(int firstRecNo, bool? skipDeleted = null, int? lastRecNo = null) where T : class, new()                          // new from 1.3 version
    {
      return new DbfTablePocoEnumerator<T>(this, skipDeleted, firstRecNo, lastRecNo);
    }
    
    public DbfTablePocoEnumerator<T> PocoEnumerator<T>(int firstRecNo, int lastRecNo, bool? skipDeleted = null) where T : class, new()                          // new from 1.3 version
    {
      return new DbfTablePocoEnumerator<T>(this, skipDeleted, firstRecNo, lastRecNo);
    }
    #endregion
  }

  #region Helper struct/enum ------------------------------------------------------------------------------
  
  /// <summary>
  /// Header information of DBF file (without columns definition).
  /// </summary>
  public struct DbfHeader
  {
    //#pragma warning disable 1591                                                        // warning: "Missing XML comment for publicly visible type or member"
    public DbfTable.DbfFileTypes  type;
    public DbfTable.CodepageCodes codepageCode;
    public DateTime               lastUpdate;
    public int                    recCount;
    public int                    firstRecordPosition;
    public int                    rowLength;

    internal bool                 newHeaderStructure    // Data File Header Structure for the dBASE Version 4..7
    {
      get
      {
        return HasNewHeaderStructure(type);
      }
    }

    internal bool                 memoFlag            
    {
      get
      {
        return (((int)type & 0x80) != 0);
      }
    }

    internal static bool HasNewHeaderStructure(DbfTable.DbfFileTypes dbftype)
    {
      return (((int)dbftype & 0x04) != 0);
    }
    //#pragma warning restore 1591
  }
  
  #endregion
}
