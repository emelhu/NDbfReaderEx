using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NDbfReaderEx
{
  public struct DbfTableParameters
  {
    #region Default values
    
    public static     Encoding          defaultEncoding         = null; 
    public static     bool              defaultOpenMemo         = true;
    public static     StrictHeader      defaultStrictHeader     = StrictHeader.medium;
    public static     DbfTableType      defaultTableType        = DbfTableType.Undefined;
    public static     MemoFileType      defaultMemoType         = MemoFileType.Undefined;
    public static     IndexFileType     defaultIndexType        = IndexFileType.Undefined;
    public static     MemoTerminators   defaultMemoTerminators  = MemoTerminators.terminatorEOF;
    public static     DbfTableOpenMode  defaultOpenMode         = DbfTableOpenMode.Read;
    
    #endregion

    #region Parameter values
    private Encoding          _encoding;
    private bool              _openMemo;
    private StrictHeader      _strictHeader;
    private DbfTableType      _tableType;
    private MemoFileType      _memoType;
    private IndexFileType     _indexType;
    private MemoTerminators   _memoTerminators;
    private DbfTableOpenMode  _openMode;

    //
    
    #endregion

    #region Parameter properties

    /// <summary>
    /// if value is null, tableOpen will calculate by header codepage 
    /// </summary>
    public Encoding           encoding 
    { 
      get { return _encoding; } 
      set { _encoding = value; }                                                     
    }

    public bool               openMemo
    {
      get { return _openMemo; }

      set
      {
        _openMemo = value;
      }
    }

    public StrictHeader       strictHeader
    {
      get { return _strictHeader; }

      set
      {
        _strictHeader = value;
      }
    }

    public DbfTableType       tableType 
    { 
      get { return _tableType; } 
      
      set 
      {
        _tableType = value;

        if (_tableType == DbfTableType.Undefined)
        {
          _tableType = defaultTableType;
        }
      } 
    }

    public DbfTableType       tableTypeMainGroup
    { 
      get { return GetTableTypeMainGroup(tableType); }
    }

    public MemoFileType       memoType
    {
      get { return _memoType; }

      set
      {
        _memoType = value;

        if (_memoType == MemoFileType.Undefined)
        {
          _memoType = defaultMemoType;

          if ((_memoType == MemoFileType.Undefined) && (tableType != DbfTableType.Undefined))
          {
            switch (tableType)
            {
              case DbfTableType.DBF_Ver3_Clipper:
              case DbfTableType.DBF_Ver3_dBase:
                _memoType = MemoFileType.DBT_Ver3;
                break;
            }            
          }
        }
      }
    }

    public IndexFileType      indexType
    {
      get { return _indexType; }

      set
      {
        _indexType = value;

        if (_indexType == IndexFileType.Undefined)
        {
          switch (this.tableType)
          {
            case DbfTableType.DBF_Ver3_dBase:
              _indexType = IndexFileType.NDX;
              break;

            case DbfTableType.DBF_Ver3_Clipper:
              _indexType = IndexFileType.NTX;
              break;
          }
        }
      }
    }

    public MemoTerminators    memoTerminators
    {
      get { return _memoTerminators; }

      set
      {
        _memoTerminators = value;
      }
    }

    public DbfTableOpenMode  openMode 
    { 
      get { return _openMode; } 
      set { _openMode = value; }                                                     
    }

    #endregion

    #region get properties
    public FileAccess fileAccess
    {
      get
      {
        switch (openMode)
        {
          case DbfTableOpenMode.Exclusive:
          case DbfTableOpenMode.ReadWrite:
            return FileAccess.ReadWrite;
          case DbfTableOpenMode.Read:
            return FileAccess.Read;
          default:
            throw ExceptionFactory.CreateArgumentException("DbfTableParameters/fileAccess", "Invalid open mode parameter!");
        }
      }
    }

    public FileShare  fileShare
    {
     get
      {
        switch (openMode)
        {
          case DbfTableOpenMode.Exclusive:
            return FileShare.None;
          case DbfTableOpenMode.ReadWrite:
          case DbfTableOpenMode.Read:
            return FileShare.ReadWrite;
          default:
            throw ExceptionFactory.CreateArgumentException("DbfTableParameters/fileShare", "Invalid open mode parameter!");
        }
      }
    } 

    public bool writeable
    {
      get
      {
        switch (openMode)
        {
          case DbfTableOpenMode.Exclusive:
          case DbfTableOpenMode.ReadWrite:
            return true;
          case DbfTableOpenMode.Read:
            return false;
          default:
            throw ExceptionFactory.CreateArgumentException("DbfTableParameters/fileAccess", "Invalid open mode parameter!");
        }
      }
    }
    #endregion

    #region Constructors

    public DbfTableParameters(Encoding encoding = null, bool? openMemo = null, StrictHeader? strictHeader = null, 
                              DbfTableType? tableType = null, MemoFileType? memoType = null, IndexFileType? indexType = null)
    {
      _encoding             = null;
      _openMemo             = false;
      _strictHeader         = 0;
      _tableType            = 0;
      _memoType             = 0;
      _indexType            = 0;
      _memoTerminators      = 0;
      _openMode             = 0;

      //

      this.encoding         = encoding;                                     // null value mean "ReadDbfHeader_Encoding(_header.codepageCode)" will work
      this.openMemo         = openMemo     ?? defaultOpenMemo;
      this.strictHeader     = strictHeader ?? defaultStrictHeader;
      this.tableType        = tableType    ?? defaultTableType;
      this.memoType         = memoType     ?? defaultMemoType;
      this.indexType        = indexType    ?? defaultIndexType ;    
      
      this.memoTerminators  = defaultMemoTerminators;    
      this.openMode         = defaultOpenMode;                   
    }

    public DbfTableParameters(DbfTableOpenMode? openmode = null, Encoding encoding = null, bool? openMemo = null, StrictHeader? strictHeader = null, 
                              DbfTableType? tableType = null, MemoFileType? memoType = null, IndexFileType? indexType = null)
    {
      _encoding             = null;
      _openMemo             = false;
      _strictHeader         = 0;
      _tableType            = 0;
      _memoType             = 0;
      _indexType            = 0;
      _memoTerminators      = 0;
      _openMode             = 0;

      //

      this.encoding         = encoding;                                         // null value mean "ReadDbfHeader_Encoding(_header.codepageCode)" will work
      this.openMemo         = openMemo     ?? defaultOpenMemo;
      this.strictHeader     = strictHeader ?? defaultStrictHeader;
      this.tableType        = tableType    ?? defaultTableType;
      this.memoType         = memoType     ?? defaultMemoType;
      this.indexType        = indexType    ?? defaultIndexType ;    
      
      this.memoTerminators  = defaultMemoTerminators;    
      this.openMode         = openmode ?? defaultOpenMode;                   
    }
    #endregion

    #region Static helpers

    public static DbfTableType GetTableTypeMainGroup(DbfTableType tableType)
    {
      return (DbfTableType)((int)tableType & 0x00FF);
    }

    public DbfTableParametersReadOnly GetReadOnly()
    {
      return new DbfTableParametersReadOnly(this);
    }
    #endregion
  }

  public enum DbfTableOpenMode
  { 
    Read,
    ReadWrite,
    Exclusive
  }  

  public enum StrictHeader
  {
    none,
    weak,
    medium,
    potent,
    full
  }

  public enum MemoTerminators
  {
    both,
    terminatorEOF,
    terminatorEOS
  }

  public enum DbfTableType                                                              // format of DBF (data) -- for details of data structure
  {
    Undefined = 0x00,                                                                       
    DBF_Ver3  = 0x03,
    DBF_Ver4  = 0x04,
    DBF_Ver7  = 0x07,
    
    DBF_Ver3_dBase    = DBF_Ver3 | 0x0100,
    DBF_Ver3_Clipper  = DBF_Ver3 | 0x0200  
  }

  public enum IndexFileType                                                              // format of index file
  {
    Undefined = 0,                                                                       
    NDX,
    NTX
  }

  public enum MemoFileType                                                              // format of DBT/FPT/etc (memo) 
  {
    Undefined = 0,                                                                       
    DBT_Ver3,
    DBT_Ver4 //,
    //FPT
  }

  public struct DbfTableParametersReadOnly
  {
    #region Parameter values
    public readonly Encoding          encoding;
    public readonly bool              openMemo;
    public readonly StrictHeader      strictHeader;
    public readonly DbfTableType      tableType;
    public readonly MemoFileType      memoType;
    public readonly IndexFileType     indexType;
    public readonly MemoTerminators   memoTerminators;
    public readonly DbfTableOpenMode  openMode;
    #endregion

    internal DbfTableParametersReadOnly(DbfTableParameters parameters)
    {
      encoding        = parameters.encoding;
      openMemo        = parameters.openMemo;
      strictHeader    = parameters.strictHeader;
      tableType       = parameters.tableType;
      memoType        = parameters.memoType;
      indexType       = parameters.indexType;
      memoTerminators = parameters.memoTerminators;
      openMode        = parameters.openMode;
    }
  }
}
