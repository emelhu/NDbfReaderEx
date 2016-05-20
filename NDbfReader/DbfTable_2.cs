using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  public partial class DbfTable : IDisposable
  {
    #region variables/constants ---------------------------------------------------------------------------

    public static bool defaultOnlyDBase3Enabled = false;
    public        bool onlyDBase3Enabled        = defaultOnlyDBase3Enabled;

    private const int  codepageBytePosition     = 29;                     // http://www.dbf2002.com/dbf-file-format.html   "Code page mark"
    private const int  firstFieldSubrecordVer3  = 32;                     // http://www.dbf2002.com/dbf-file-format.html   "32 – n	: Field subrecords"
    private const int  firstFieldSubrecordVer4  = 68;                     // http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm   "Field Descriptor Array (see 1.2)."
    private const byte headerRecordTerminator   = 0x0D;                   // http://www.dbf2002.com/dbf-file-format.html   "n+1: Header record terminator (0x0D)"
    private const int  maxColumnCount           = 1024;                   // I extend it for myself: original 255  http://msdn.microsoft.com/en-us/library/3kfd3hw9(v=vs.80).aspx   
    private const byte endOfFileTerminator      = 0x1A; 

    private const int  minDbfFileLengthVer3     = 32 + 32 + 1;            // http://www.dbf2002.com/dbf-file-format.html   (header info + 1 filed + headerRecordTerminator)
    private const int  minDbfFileLengthVer4     = 68 + 48 + 1;            // http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm    (header info + 1 filed + headerRecordTerminator)                   
    #endregion

    #region enums/constans ----------------------------------------------------------------------------------
    
    /// <summary>
    /// Valid codepage bytes by standard codepage names for DBF file.
    /// information have got from http://forums.esri.com/Thread.asp?c=93&f=1170&t=197185#587982
    /// also added from           https://msdn.microsoft.com/en-us/library/8t45x02s(v=vs.80).aspx
    /// also added from           http://shapelib.maptools.org/codepage.html
    /// </summary>
    public enum CodepageCodes : byte
    {
      OEM               = 0x00,                                           // OEM = 0 
      CP_437            = 0x01,                                           // Codepage_437_US_MSDOS = &H1 
      CP_850            = 0x02,                                           // Codepage_850_International_MSDOS = &H2 
      CP_1252           = 0x03,                                           // Codepage_1252_Windows_ANSI = &H3 

      CP_10000          = 0x04,                                           // Standard Macintosh

      CP_865_Danish     = 0x08,		                                        // Danish OEM
      CP_437_Dutch      = 0x09,		                                        // Dutch OEM
      CP_850_Dutch      = 0x0A,		                                        // Dutch OEM*
      CP_437_Finnish    = 0x0B,		                                        // Finnish OEM
      CP_437_French     = 0x0D,		                                        // French OEM
      CP_850_French     = 0x0E,		                                        // French OEM*
      CP_437_German     = 0x0F,		                                        // German OEM
      CP_850_German     = 0x10,		                                        // German OEM*
      CP_437_Italian    = 0x11,		                                        // Italian OEM
      CP_850_Italian    = 0x12,		                                        // Italian OEM*
      CP_932_Japanese   = 0x13,		                                        // Japanese Shift-JIS
      CP_850_Spanish    = 0x14,		                                        // Spanish OEM*
      CP_437_Swedish    = 0x15,		                                        // Swedish OEM
      CP_850_Swedish    = 0x16,		                                        // Swedish OEM*
      CP_865_Norwegian  = 0x17,		                                        // Norwegian OEM
      CP_437_Spanish    = 0x18,		                                        // Spanish OEM
      CP_437_GB         = 0x19,		                                        // English OEM (Great Britain)
      CP_850_GB         = 0x1A,		                                        // English OEM (Great Britain)*
      CP_437_US         = 0x1B,		                                        // English OEM (US)
      CP_863            = 0x1C,		                                        // French OEM (Canada)
      CP_850_French2    = 0x1D,		                                        // French OEM*
      CP_852_Czech      = 0x1F,		                                        // Czech OEM
      CP_852_Hungarian  = 0x22,		                                        // Hungarian OEM
      CP_852_Polish     = 0x23,		                                        // Polish OEM
      CP_860            = 0x24,		                                        // Portuguese OEM
      CP_850_Portuguese = 0x25,		                                        // Portuguese OEM*
      CP_866_Russian    = 0x26,		                                        // Russian OEM
      CP_850_US         = 0x37,		                                        // English OEM (US)*
      CP_852_Romanian   = 0x40,		                                        // Romanian OEM
      CP_936_Chinese    = 0x4D,		                                        // Chinese GBK (PRC)
      CP_949_Korean     = 0x4E,		                                        // Korean (ANSI/OEM)
      CP_950_Chinese    = 0x4F,		                                        // Chinese Big5 (Taiwan)
      CP_874_Thai       = 0x50,	                                          // Thai (ANSI/OEM)

      ANSI              = 0x57,                                           // ANSI = &H57  
      
      CP_1252_WestEu    =	0x58,		                                        // Western European ANSI
	    CP_1252_Spanish   = 0x59,		                                        // Spanish ANSI
           
      CP_852            = 0x64,                                           // Codepage_852_EasernEuropean_MSDOS = &H64 
      CP_866            = 0x65,                                           // Codepage_866_Russian_MSDOS = &H65 
      CP_865            = 0x66,                                           // Codepage_865_Nordic_MSDOS = &H66 
      CP_861            = 0x67,                                           // Codepage_861_Icelandic_MSDOS = &H67      
      CP_895            = 0x68,                                           // Kamenicky (Czech) MS-DOS
      CP_620            = 0x69,                                           // Mazovia (Polish) MS-DOS 
      CP_737            = 0x6A,                                           // Codepage_737_Greek_MSDOS = &H6A 
      CP_857            = 0x6B,                                           // Codepage_857_Turkish_MSDOS = &H6B  
      
      CP_863_Canadian   = 0x6C,        	                                  // French-Canadian MS-DOS
           
      CP_950            = 0x78,                                           // Codepage_950_Chinese_Windows = &H78 
      CP_949            = 0x79,                                           // Korean Windows
      CP_936            = 0x7A,                                           // Codepage_936_Chinese_Windows = &H7A 
      CP_932            = 0x7B,                                           // Codepage_932_Japanese_Windows = &H7B 

      CP_874            = 0x7C,                                           // Thai Windows

      CP_1255           = 0x7D,                                           // Codepage_1255_Hebrew_Windows = &H7D 
      CP_1256           = 0x7E,                                           // Codepage_1256_Arabic_Windows = &H7E 

      CP_737_Greek      = 0x86,	                                          // Greek OEM
  	  CP_852_Slovenian  = 0x87,	                                          // Slovenian OEM
  	  CP_857_Turkish    = 0x88,	                                          // Turkish OEM
       
      CP_10007          = 0x96,                                           // Russian Macintosh
      CP_10029          = 0x97,                                           // Macintosh EE
      CP_10006          = 0x98,                                           // Greek Macintosh

      CP_1250           = 0xC8,                                           // Codepage_1250_Eastern_European_Windows = &HC8 
      CP_1251           = 0xC9,                                           // Codepage_1251_Russian_Windows = &HC9 
      CP_1254           = 0xCA,                                           // Codepage_1254_Turkish_Windows = &HCA 
      CP_1253           = 0xCB,                                           // Codepage_1253_Greek_Windows = &HCB 

      CP_1257           = 0xCC	                                          // Baltic Windows
    };

    public static int GetEncodingCodePageFromCodepageCodes(CodepageCodes codepageCode)
    {
      string codepageName = codepageCode.ToString();

      if (codepageName.StartsWith("CP_"))
      {
        string[] nameParts = codepageName.Split('_');

        return int.Parse(nameParts[1]);;
      }

      switch (codepageCode)
      { // Name isn't started "CP_"
        case CodepageCodes.OEM:
          return CultureInfo.CurrentCulture.TextInfo.OEMCodePage;           
        case CodepageCodes.ANSI:
          return CultureInfo.CurrentCulture.TextInfo.ANSICodePage;          
      }

      return int.MinValue;
    }

    /// <summary>
    /// http://www.dbf2002.com/dbf-file-format.html
    /// </summary>
    public enum DbfFileTypes : byte
    {
      FoxBASE       = 0x02,                                     // FoxBASE
      DBase3        = 0x03,                                     // FoxBASE+/Dbase III plus, no memo
      DBase7        = 0x04,                                     // NEW *** added by eMeL
      VisualFoxPro  = 0x30,                                     // Visual FoxPro
      VisualFoxPro2 = 0x31,                                     // Visual FoxPro, autoincrement enabled
      VisualFoxPro3 = 0x32,                                     // Visual FoxPro with field type Varchar or Varbinary
      DBase4Sql     = 0x43,                                     // dBASE IV SQL table files, no memo
      DBase4SqlSys  = 0x63,                                     // dBASE IV SQL system files, no memo
      DBase3M       = 0x83,                                     // FoxBASE+/dBASE III PLUS, with memo
      DBase7M       = 0x84,                                     // NEW *** added by eMeL / with memo
      DBase4M       = 0x8B,                                     // dBASE IV with memo
      DBase4SqlM    = 0xCB,                                     // dBASE IV SQL table files, with memo
      FoxPro2M      = 0xF5,                                     // FoxPro 2.x (or earlier) with memo
      HiperSix      = 0xE5,                                     // HiPer-Six format with SMT memo file
      FoxBASE2      = 0xFB                                      // FoxBASE
    };
    #endregion

    #region ReadDbfHeader_* ---------------------------------------------------------------------------------

    public static DbfHeader ReadDbfHeader(Stream stream, bool? onlyDBase3Enabled = null)
    {
      DbfHeader header;

      stream.Position = 0;                                                      // start from first byte

      if (stream.Length < minDbfFileLengthVer3)
      {
        throw new IOException("Not a DBF file! ('DBF File length' is too short!) [< " + minDbfFileLengthVer3 + "]");
      }

      BinaryReader reader = new BinaryReader(stream);                           // don't use using '(BinaryReader reader...' because 'using' dispose 'stream' too!
      {        
        header.type = (DbfFileTypes)(reader.ReadByte());                        // pos: 0          -- DBF File type: 

        if (! DbfHeader.HasNewHeaderStructure(header.type))
        {
          if (Array.IndexOf(Enum.GetValues(typeof(DbfFileTypes)), header.type) < 0)
          {
            if (onlyDBase3Enabled ?? defaultOnlyDBase3Enabled)
            {
              throw new IOException("Not a DBF file! ('DBF File type' is not valid!) [dBase3]");
            }
          }
        }

        int minDbfFileLength = DbfHeader.HasNewHeaderStructure(header.type) ? minDbfFileLengthVer4 : minDbfFileLengthVer3;
        
        if (stream.Length < minDbfFileLength)
        {
          throw new IOException("Not a DBF file! ('DBF File length' is too short!) [< " + minDbfFileLength + "]");
        }  
        
        //

        int lastUpdateYear  = reader.ReadByte();                                // pos: 1,2,3      -- Last update (YYMMDD)
        int lastUpdateMonth = reader.ReadByte();
        int lastUpdateDay   = reader.ReadByte();

        int maxYear = (DateTime.Today.Year - 2000) + 1;                         // DBase III / Clipper date

        bool correctedYear = false;

        if (lastUpdateYear >= 100)
        { // If not a dBaseIII or Clipper file, correct it            
          // because dBaseIII+ (and earlier) has a Year 2000 bug. It stores the year as simply the last two digits of the actual year. 
          maxYear += 100;
          correctedYear = true;
        }

        if ((lastUpdateYear  < 0) || (lastUpdateYear  > maxYear) ||             // !WARNING: only next year is valid
            (lastUpdateMonth < 1) || (lastUpdateMonth > 12)      ||
            (lastUpdateDay   < 1) || (lastUpdateDay   > 31))
        {
          throw new IOException("Not a DBF file! ('Last update' (YYMMDD) error!)");
        }

        header.lastUpdate = new DateTime((correctedYear ? 1900 : 2000) + lastUpdateYear, lastUpdateMonth, lastUpdateDay);       

        //

        header.recCount            = reader.ReadInt32();
        header.firstRecordPosition = reader.ReadInt16();
        header.rowLength           = reader.ReadInt16();

        if (header.recCount < 0)                
        {
          throw new IOException("Not a DBF file! (Number of records in file error!)");
        }

        //

        if (DbfHeader.HasNewHeaderStructure(header.type))
        {
          int maxLengthOfFieldPropertiesStructure;  // it's too complex, we use only a fabricated number. http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm

          maxLengthOfFieldPropertiesStructure = 64 * 1024;

          if ((header.firstRecordPosition < minDbfFileLengthVer4) || 
              (header.firstRecordPosition > (68 + (48 * maxColumnCount) + 1 + maxLengthOfFieldPropertiesStructure)) ||
              (header.firstRecordPosition > stream.Length))                     // !WARNING: I don't know valid limit
          {
            throw new IOException("Not a DBF file! (firstRecordPosition) [dBase4-7]");
          }
        }
        else
        {
          if ((header.firstRecordPosition < minDbfFileLengthVer3) || 
              (header.firstRecordPosition > (32 + (32 * maxColumnCount) + 1 + 2 + 264)) ||
              (header.firstRecordPosition > stream.Length))                     // !WARNING: I don't know valid limit
          {
            throw new IOException("Not a DBF file! (firstRecordPosition) [dBase3]");
          }
        }

        //

        stream.Position = codepageBytePosition;                                // pos: http://www.dbf2002.com/dbf-file-format.html

        header.codepageCode = (CodepageCodes)(reader.ReadByte());

        if (Array.IndexOf(Enum.GetValues(typeof(CodepageCodes)), header.codepageCode) < 0)
        {
          throw new IOException("Not a DBF file! ('Code page mark' is not valid!)");
        }
      }
      
      return header;   
    }

    public static Encoding ReadDbfHeader_Encoding(CodepageCodes code, bool throwException = false)
    {
      int codepage = GetEncodingCodePageFromCodepageCodes(code);
        
      if (codepage < 0)
      { // not found  
        if (throwException)
        {
          throw new Exception(String.Format("ReadDbfHeader_Encoding(): '{0}' codepage code invalid!", code));
        }
        else
        {
          return null;
        }
      }

      return Encoding.GetEncoding(codepage);
    }

    public static IColumn[] ReadDbfColumns(Stream stream, Encoding encoding, bool newHeaderStructure, bool openMemo)
    {
      List<IColumn> columns = new List<IColumn>();

      stream.Position = newHeaderStructure ? firstFieldSubrecordVer4 : firstFieldSubrecordVer3;

      int calcOffset = 0;

      for (int i = 0; (i < maxColumnCount); i++)
      {
        IColumn column = GetNextColumnDefinition(stream, encoding, calcOffset, newHeaderStructure);

        if (column == null)
        { // No more column definition
          break;
        }
        else
        { // Store column definition
          if (column.dbfType == NativeColumnType.Memo)
          {
            if (openMemo)
            {
              columns.Add(column);
            }
          }
          else
          {
            columns.Add(column);
          }
          
          calcOffset += column.size;
        }
      }

      if (columns.Count < 1)
      {
        throw new Exception("DBF file format error! (At least one column definition required!)");
      }

      return columns.ToArray();                                                            
    }

    private static Column GetNextColumnDefinition(Stream stream, Encoding encoding, int calcOffset, bool newHeaderStructure)
    { // If there isn't more column definition return null.
      string            columnName;
      NativeColumnType  columnType;
      int               columnOffset;                                           // isn't stored by dBase3/Clipper, must calculate by 'calcOffset'
      short             columnSize;
      short             columnDec;

      //

      int    bufferLen = newHeaderStructure ? 32 : 11;                          // http://www.dbf2002.com/dbf-file-format.html "Field name with a maximum of 10 characters. If less than 10, it is padded with null characters (0x00)."
                                                                                // http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm  
      byte[] buffer = new byte[bufferLen]; 
                            
      int readed = stream.Read(buffer, 0, buffer.Length);

      if ((readed > 0) && (buffer[0] == headerRecordTerminator))
      { // found Header record terminator (0x0D)
        return null;                                                            // there isn't more column definition record
      }   


      if (readed != buffer.Length)
      {
        throw new Exception("DBF file format error! (don't found 'Header record terminator' (0x0D)!)");
      }

      { // http://www.dbf2002.com/dbf-file-format.html "If less than 10, it is padded with null characters (0x00)." - NO! It can contains garbage bytes after 0x00! 
        int nameLen = newHeaderStructure ? 32 : 10;

        for (int i = 0; (i < nameLen); i++)
        {
          if ((buffer[i] == 0x00) || (buffer[i] == 0x20))                                     // name is closed and/or trimmed blanks (can closed with blank in real world)
          {
            nameLen = i;
            break;
          }
        }

        if (nameLen == 0)
        {
          throw new Exception("DBF file format error! (don't found column name!)");
        }

        columnName = Encoding.ASCII.GetString(buffer, 0, nameLen);    
      }

      //

      BinaryReader reader = new BinaryReader(stream);                                 // don't use 'using (BinaryReader reader...' because 'using' dispose 'stream' too.

      if (newHeaderStructure)
      {
        columnType = (NativeColumnType)reader.ReadByte();
        columnSize = reader.ReadByte();
        columnDec  = reader.ReadByte();

        reader.ReadBytes(13); 
      }
      else
      {
        columnType = (NativeColumnType)reader.ReadByte();
        columnOffset = reader.ReadInt32();                                              // fake in dBase3 header
        columnSize = reader.ReadByte();
        columnDec = reader.ReadByte();

        reader.ReadBytes(14);                                                           // skip don't used and reserved caharacters
      }

      reader = null;

      //

      columnOffset = calcOffset;                                                      // isn't stored in header by dBase/Clipper, must calculate by 'calcOffset'                                                  

      if ((char)columnType == '+')                                                    // TODO: set column to ReadOnly
      { // Because store as same format and only read field data 
        columnType = NativeColumnType.Long;
      }

      switch (columnType)
      {
        case NativeColumnType.Char:
          return new StringColumn(columnName, columnType, columnOffset, columnSize, columnDec, encoding);

        case NativeColumnType.Memo:
          return new MemoColumn(columnName, columnType, columnOffset, columnSize, columnDec, encoding);

        case NativeColumnType.Date:
          return new DateTimeColumn(columnName, columnType, columnOffset);

        case NativeColumnType.Long:
          Debug.Assert(columnSize == 4);
          return new Int32Column(columnName, columnType, columnOffset);

        case NativeColumnType.Logical:
          Debug.Assert(columnSize == 1);
          return new BooleanColumn(columnName, columnType, columnOffset);

        case NativeColumnType.Numeric:
        case NativeColumnType.Float:
          return new DecimalColumn(columnName, columnType, columnOffset, columnSize, columnDec);

        case NativeColumnType.Double:
          Debug.Assert(columnSize == 8);
          return new DoubleColumn(columnName, columnType, columnOffset);

        default:
          throw ExceptionFactory.CreateNotSupportedException("The {0} column's type '{1}' is not supported.", columnName, columnType);
      }
    }

    #endregion

    #region row read/write ----------------------------------------------------------------------------------

    public DbfRow GetRow(int recNo, bool throwException = false)
    {
      ThrowIfDisposed();

      if ((recNo < 0) || (recNo >= recCount))
      {
        if (throwException)
        {
          throw new Exception(String.Format("DbfTable.GetRow({0}): invalid record number! [count of records: {1}]", recNo, recCount));
        }
        else
        {
          return null;
        }  
      }

      int    offset = _header.firstRecordPosition + (recNo * _header.rowLength);
      byte[] buffer = new byte[_header.rowLength];

      _stream.Position = offset;
      int readed = _stream.Read(buffer, 0, _header.rowLength);

      if (readed != _header.rowLength)
      {
        throw new IOException(String.Format("DBF file is corrupted! [Haven't enought bytes for {0}. row!]", recNo));
      }

      return new DbfRow(recNo, buffer, _columns, dbfTableClassID);
    }

    /// <summary>
    /// Update a dbf record by a DbfRow.
    /// 
    /// 
    /// </summary>
    /// <param name="row">Recno in DbfRow points record to update.</param>
    /// <param name="throwException"></param>
    /// <param name="updateRecNo">If DbfRow is a foreign/orphan row you must signal it explicit with updateRecNo value.</param>
    /// <returns>A boolean value or exception for success & DbfRow atached to this table if it was a foreign/orphan row.</returns>
    public bool UpdateRow(DbfRow row, bool throwException = true, int updateRecNo = int.MaxValue)
    {
      ThrowIfDisposed();

      if (row == null)
      {
        throw ExceptionFactory.CreateArgumentException("UpdateRow(row)", "Null parameter invalid!");
      }

      if (row.dbfTableClassID != this.dbfTableClassID)
      {
        if (updateRecNo != int.MaxValue)
        { // explicit signal for update a foreign/orphan row into this dbf file 
          if ((updateRecNo < 0) || (updateRecNo >= recCount))
          {
            if (throwException)
            {
              throw new Exception(String.Format("DbfTable.UpdateRow(updateRecNo:{0}): invalid record number! [count of records: {1}]", updateRecNo, recCount));
            }
            else
            {
              return false;
            }
          }

          row.AtachedToAnotherTable(this, updateRecNo);
        }
        else if (throwException)
        {
          throw new Exception(String.Format("DbfTable.UpdateRow(): DbfRow parameter is a foreign/orphan value!"));
        }
        else
        {
          return false;
        }
      }

      


      if ((row.recNo < 0) || (row.recNo >= recCount))
      {
        if (throwException)
        {
          throw new Exception(String.Format("DbfTable.UpdateRow({0}): invalid record number! [count of records: {1}]", row.recNo, recCount));
        }
        else
        {
          return false;
        }
      }


      if (row._buffer.Length != _header.rowLength)
      {
        throw ExceptionFactory.CreateArgumentException("UpdateRow(row)", "Length of buffer in row different then file rowlength!");
      }


      int offset = _header.firstRecordPosition + (row.recNo * _header.rowLength);

      try
      {
        _stream.Write(row._buffer, offset, row._buffer.Length);
      }
      catch (Exception e)
      {
        throw new IOException(String.Format("DBF file is corrupted! [Write error at {0}. row!]", row.recNo), e);
      }

      row._modified = false;

      return true;       
    }

    /// <summary>
    /// Insert a row and return success.
    /// If error happends return false (or throw an exception) 
    /// Set row._recNo to record position of new record.
    /// </summary>
    /// <param name="row"></param>
    /// <param name="throwException"></param>
    /// <returns></returns>
    public bool InsertRow(DbfRow row, bool throwException = true, bool foreignRowEnabled = false)
    {
      ThrowIfDisposed();

      if (row == null)
      {
        throw ExceptionFactory.CreateArgumentException("InsertRow(row)", "Null parameter invalid!");
      }

      if (row.dbfTableClassID != this.dbfTableClassID)
      {
        if (foreignRowEnabled)
        { // explicit signal for insert a foreign/orphan row into this dbf file 
          row.AtachedToAnotherTable(this, DbfRow.forInsert_recNoValue);
        }
        else if (throwException)
        {
          throw new Exception(String.Format("DbfTable.UpdateRow(): DbfRow parameter is a foreign/orphan value!"));
        }
        else
        {
          return false;
        }
      }

      row._recNo    = 99999999;         // TODO: !!!!!!!!!!!!!!!!!!!!!!
      row._modified = false;

      throw new NotImplementedException("InsertRow");
    }

    public bool WriteRow(DbfRow row, bool throwException = true)
    {
      if (row == null)
      {
        throw ExceptionFactory.CreateArgumentException("WriteRow(row)", "Null parameter invalid!");
      }

      if (row.dbfTableClassID != this.dbfTableClassID)
      {
        if (throwException)
        {
          throw new Exception(String.Format("DbfTable.WriteRow(): DbfRow parameter is a foreign/orphan value!"));
        }
        else
        {
          return false;
        }
      }

      if (row.recNo == DbfRow.forInsert_recNoValue)
      {
        return InsertRow(row, throwException);
      }
      else
      {
        return UpdateRow(row, throwException);
      }
    }

    public void DetachRow(DbfRow row, bool throwException = true)
    {
      row.AtachedToAnotherTable(null, DbfRow.forInsert_recNoValue);
    }

    #endregion

    #region row data read -----------------------------------------------------------------------------------
    #endregion

    #region CreateFile --------------------------------------------------------------------------------------

    private static DbfTable CreateHeader_DBF(Stream stream, 
                                         IEnumerable<IColumnBase> columns,                                  // ColumnDefinitionForCreateTable --> IColumnBase
                                         DbfTableType tableType     = DbfTableType.Undefined,
                                         CodepageCodes codepageCode = CodepageCodes.OEM,
                                         Encoding encoding = null)
    {
      if (tableType == DbfTableType.Undefined)
      { // Crawling columns definition and choose one
        // TODO:!!!
        // tableType = *****;
      }

      var par = new DbfTableParameters(encoding, true, StrictHeader.full, tableType, null, null);

      //    

      switch (par.tableTypeMainGroup)
      {
        case DbfTableType.DBF_Ver3:
        case DbfTableType.DBF_Ver4:
        case DbfTableType.DBF_Ver7:
          CreateHeader_dBase(stream, columns, par.tableType, codepageCode);
          break;

        case DbfTableType.Undefined:
          throw ExceptionFactory.CreateArgumentException("tableType", "Undefined table type for CreateHeader_DBF() !");
        default:
          throw ExceptionFactory.CreateArgumentException("tableType", "Invalid table type for CreateHeader_DBF() !");
      }

      if (encoding == null)
      {
        Debug.Assert(codepageCode != CodepageCodes.OEM);
      }
      else
      {
        Debug.Assert(codepageCode == CodepageCodes.OEM);
      }
      

      DbfTable ret = new DbfTable(stream, par);

      return ret;
    }

    private static void CreateHeader_dBase(Stream stream, IEnumerable<IColumnBase> columns, 
                                            DbfTableType tableType     = DbfTableType.Undefined,
                                            CodepageCodes codepageCode = CodepageCodes.OEM)
    {
      if (stream == null)
      {
        throw new ArgumentNullException("stream");
      }

      if (!stream.CanRead)
      {
        throw ExceptionFactory.CreateArgumentException("stream", "The stream does not allow creating (CanRead property returns false).");
      }

      if (!stream.CanWrite)
      {
        throw ExceptionFactory.CreateArgumentException("stream", "The stream does not allow creating (CanWrite property returns false).");
      }

      if (!stream.CanSeek)
      {
        throw ExceptionFactory.CreateArgumentException("stream", "The stream does not allow creating (CanSeek property returns false).");
      }

      //

      var columnDefs = new List<IColumnBase>(columns);

      if (columnDefs.Count < 1)
      {
        throw ExceptionFactory.CreateArgumentException("columns", "Header definition is empty!");
      }

      //

      bool headerVer7;

      switch (DbfTableParameters.GetTableTypeMainGroup(tableType))
      {
        case DbfTableType.DBF_Ver3:
        case DbfTableType.DBF_Ver4:
          headerVer7 = false;
          break;

        case DbfTableType.DBF_Ver7:
          headerVer7 = true;
          break;

        default:
          throw ExceptionFactory.CreateArgumentException("tableType", "Invalid dbf type! [" + tableType + "]");
      }

      //

      DbfFileTypes dbfFileType = headerVer7 ? DbfFileTypes.DBase7 : DbfFileTypes.DBase3;

      short        lengthOfDataRecords = 1;                                         // 1 for 'delete flag'

      foreach (IColumnBase col in columnDefs)
      {
        if (col.dbfType == NativeColumnType.Memo)
        {
          dbfFileType = headerVer7 ? DbfFileTypes.DBase7M : DbfFileTypes.DBase3M;
        }

        lengthOfDataRecords += col.size;      
      }

      //

      short positionOfFirstDataRecord;  
      
      if (headerVer7)
      {
        throw new NotImplementedException();    // TODO: !!!
      }
      else
      {
        positionOfFirstDataRecord = (short)(32 + (columnDefs.Count * 32) + 1); 
      }

      //

      byte[] lastUpdate = new byte[3];

      {
        int year = DateTime.Now.Year - 2000;
        int month = DateTime.Now.Month;
        int day = DateTime.Now.Day;

        lastUpdate[0] = (byte)year;
        lastUpdate[1] = (byte)month;
        lastUpdate[2] = (byte)day;
      }

      //

      stream.SetLength(0);
      BinaryWriter writer = new BinaryWriter(stream);                           // don't use using '(BinaryWriter reader...' because 'using' dispose 'stream' too!

      writer.Write((byte)dbfFileType);                                          // http://www.dbf2002.com/dbf-file-format.html
      writer.Write(lastUpdate);
      writer.Write((Int32)0);                                                   // "Number of records in file"                             
      writer.Write(positionOfFirstDataRecord);
      writer.Write(lengthOfDataRecords);
      writer.Write(new byte[16]);
      writer.Write((byte)0);                                                    // "Table flags"
      writer.Write((byte)codepageCode);                                         // 29. byte
      writer.Write(new byte[2]);                                                // reserved

      foreach (IColumnBase col in columnDefs)
      {
        byte[] name = Encoding.ASCII.GetBytes(col.name);
        Array.Resize<byte>(ref name, 11);
        writer.Write(name);

        writer.Write((byte)col.dbfType);

        writer.Write((Int32)0);                                                 // "Displacement of field in record"  

        writer.Write((byte)col.size);
        writer.Write((byte)col.dec);

        writer.Write(new byte[14]);                                             // reserved
      }

      writer.Write(headerRecordTerminator);                                     // "Header record terminator" 

      //

      if (tableType == DbfTableType.DBF_Ver3_Clipper)
      {
        writer.Write((byte)0x00);                                               // Optional: Extra byte for Clipper (only after header) befor EOF
      }
      
      writer.Write(endOfFileTerminator);
    }
        
    private static Stream CreateHeader_Memo(string dbfFileName, MemoFileType memoType)
    {
      switch (memoType)
      {
        case MemoFileType.DBT_Ver3:
        case MemoFileType.DBT_Ver4:
          return CreateHeader_MemoDBT(dbfFileName, memoType);

        case MemoFileType.Undefined:         
          throw ExceptionFactory.CreateArgumentException("memoType", "Undefined memo type for CreateHeader_Memo() !");

        default:
          throw ExceptionFactory.CreateArgumentException("memoType", "Invalid memo type for CreateHeader_Memo() !");
      }
    }

    private static Stream CreateHeader_MemoDBT(string dbfFileName, MemoFileType memoFileType, int memoBlockSize = 0)
    {      
      string dbtFileName = Path.Combine(Path.GetDirectoryName(dbfFileName), Path.GetFileNameWithoutExtension(dbfFileName) + ".dbt");
      
      if (memoBlockSize < 1)
      { // Set default
        memoBlockSize = 512;
      }

      switch (memoFileType)
      {
        case MemoFileType.Undefined:
          throw new Exception("CreateHeader_MemoDBT: Don't know MemoFileType parameter value !");
        case MemoFileType.DBT_Ver3:
          if (memoBlockSize != 512)
          {
            throw new Exception("CreateHeader_MemoDBT: DBT version 3 and (memoBlockSize != 512) !");
          }
          break;
        case MemoFileType.DBT_Ver4:
          if ((memoBlockSize < 64) || (memoBlockSize < 32 * 1024))
          {
            throw new Exception("CreateHeader_MemoDBT(" + memoBlockSize + "): DBT version 4 - valid memoBlockSize values are 64..32768 !");
          }

          if ((memoBlockSize % 64) != 0)
          {
            throw new Exception("CreateHeader_MemoDBT(" + memoBlockSize + "): DBT version 4 - valid memoBlockSize is a multiple of 64 !");
          }
          break;
        default:
          throw new Exception("CreateHeader_MemoDBT: Don't handle MemoFileType parameter value !");
      }

      Stream stream = new FileStream(dbtFileName, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
      
      {
        //stream.SetLength(512);                                                // Header block / If the stream is expanded, the contents of the stream between the old and the new length are not defined.
        byte[] headerBlock = new byte[memoBlockSize];                           // Header block is filled with zero  

        stream.Write(headerBlock, 0, headerBlock.Length);
      }

      stream.Position = 0;

      BinaryWriter writer = new BinaryWriter(stream);                           // don't use 'using (BinaryWriter writer...' because 'using' dispose 'stream' too!

      writer.Write((Int32)1);                                                   // "Number of next available block for appending data"  http://www.clicketyclick.dk/databases/xbase/format/dbt.html#DBT_STRUCT

      switch (memoFileType)
      {
        case MemoFileType.DBT_Ver3:
          writer.BaseStream.Position = 16;                                      // position of "Version no."                            --''--
          writer.Write((byte)0x03);  
          break;

        case MemoFileType.DBT_Ver4:
          writer.BaseStream.Position = 8;                                       // position of "filename"                            --''--
          {
            string fileName  = Path.GetFileNameWithoutExtension(dbtFileName);
            byte[] fileBytes = Encoding.Default.GetBytes(fileName);

            writer.Write(fileBytes, 0, Math.Min(8, fileBytes.Length));
          }
          writer.BaseStream.Position = 16;                                      // position of "Version no."                            --''--
          writer.Write((byte)0x00);  
          break;
      }
      
      writer.BaseStream.Position = 20;
      writer.Write((Int16)memoBlockSize);                        

      return stream;
    }
    
    #endregion

    #region MemoFile ----------------------------------------------------------------------------------------
    
    public void JoinMemoStream(Stream fileStream, MemoFileType memoType = MemoFileType.Undefined)
    {
      if (memoType == MemoFileType.Undefined)
      {
        memoType = parameters.memoType;

        if (memoType == MemoFileType.Undefined)
        { // Scan memo stream for detect type...
          // There aren't any signature in DBT or FPT file, so we can't detect type of it correctly.

          memoType = DefaultMemoFileFormatForDbf();
        }
      }

      if (parameters.encoding == null)
      {
        parameters.encoding = ReadDbfHeader_Encoding(dbfHeader.codepageCode, true);
      }

      switch (memoType)
      {
        case MemoFileType.DBT_Ver3:
        case MemoFileType.DBT_Ver4:
          this._memoFile = new MemoFileDBT(fileStream, parameters.encoding, memoType, parameters.strictHeader);
          break;
        case MemoFileType.Undefined:
          throw ExceptionFactory.CreateArgumentException("memoType", "There isn't information of format of the memory stream!");
        default:
          throw new NotImplementedException();
      }

      //

      var memoFields = from col in _columns
                       where col.dbfType == NativeColumnType.Memo
                       select col;

      foreach (var colDef in memoFields)
      {
        MemoColumn memoColumn = colDef as MemoColumn;

        Debug.Assert(memoColumn != null);

        memoColumn.memoFile = this._memoFile;
      }
    }

    private void JoinMemoFile()
    {
      if (! this.isExistsMemoField)
      {
        Trace.TraceWarning("DbfTable/JoinMemoFile/! isExistsMemoField");
        return;
      }

      if (! parameters.openMemo)
      {
        Trace.TraceWarning("DbfTable/JoinMemoFile/! openMemo");
        return;
      }


      if (parameters.memoType == MemoFileType.Undefined)
      {
        parameters.memoType = DefaultMemoFileFormatForDbf();                          // calculate this from DBF header data
      }   

      switch (parameters.memoType)
      {
        case MemoFileType.Undefined:
          {
            string testFilename = Path.ChangeExtension(dataFileName, ".DBT");

            if (File.Exists(testFilename))
            {
              memoFileName = testFilename; 
            }
          }
          break;

        case MemoFileType.DBT_Ver3:
        case MemoFileType.DBT_Ver4:
          memoFileName = Path.ChangeExtension(dataFileName, ".DBT");
          break;

        default:
          throw new Exception(String.Format("JoinMemoFile(): untreated MemoFileType! [{0}]", parameters.memoType));
      }      


      if (String.IsNullOrWhiteSpace(memoFileName))
      {      
        throw new Exception(String.Format("Don't be known the format or name of the memo/blob file for '{0}' DBF file!", this.dataFileName));
      }
      else if (File.Exists(memoFileName))
      {
        var stream = new FileStream(memoFileName, FileMode.Open, parameters.fileAccess, parameters.fileShare);

        JoinMemoStream(stream, parameters.memoType);
      }
      else
      {
        throw new Exception(String.Format("Don't found the '{0}' memo/blob file for '{1}' DBF file!", memoFileName, this.dataFileName));
      }
    }

    #endregion

    #region IndexFile ----------------------------------------------------------------------------------------

    [CLSCompliant(false)]
    public IIndexFile JoinIndexStream(Stream fileStream, bool? skipDeleted = null, IndexFileType indexType = IndexFileType.Undefined)
    {
      IndexFileBase indexFile = null;

      if (indexType == IndexFileType.Undefined)
      {
        indexType = parameters.indexType;
      }
  
      switch (indexType)
      {
        case IndexFileType.NTX:  
          indexFile = new IndexFileNTX(fileStream, this, skipDeleted ?? this.skipDeleted);
          break;                                             
        case IndexFileType.NDX:
          indexFile = new IndexFileNDX(fileStream, this, skipDeleted ?? this.skipDeleted);
          break;
        case IndexFileType.Undefined:
          throw new Exception("JoinIndexStream(): Don't known the format of the index stream!");
        default:
          throw new Exception("JoinIndexStream(): Don't handle the format of the index stream!");
      }

      if (this._indexFiles == null)
      {
        this._indexFiles = new List<IndexFileBase>();
      }

      this._indexFiles.Add(indexFile);

      return indexFile;
    }

    [CLSCompliant(false)]
    public IIndexFile JoinIndexFile(IndexFileType indexType, string indexFileID = null, bool? skipDeleted = null)
    {
      return JoinIndexFile(indexType, null, indexFileID, skipDeleted);
    }

    [CLSCompliant(false)]
    public IIndexFile JoinIndexFile(IndexFileType indexType = IndexFileType.Undefined, char? separatorChar = null, string indexFileID = null, bool? skipDeleted = null)
    {
      string fileName = Path.Combine(Path.GetDirectoryName(this.dataFileName), Path.GetFileNameWithoutExtension(this.dataFileName));

      if (separatorChar != null)
      {
        fileName += separatorChar; 
      }

      if (! String.IsNullOrWhiteSpace(indexFileID))
      {
        fileName += indexFileID;
      }

      if (indexType == IndexFileType.Undefined)
      {
        indexType = parameters.indexType;
      }     

      switch (indexType)
      {
        case IndexFileType.Undefined:
          throw new Exception("JoinIndexFile(): IndexFileType.Undefined parameter!");
        case IndexFileType.NDX:
          fileName += ".ndx";
          break;
        case IndexFileType.NTX:
          fileName += ".ntx";
          break;
        default:
          throw new Exception("JoinIndexFile(" + indexType + "): Don't handled parameter value !");
      }

      return JoinIndexFile(fileName, skipDeleted);
    }

    [CLSCompliant(false)]
    public IIndexFile JoinIndexFile(string indexFileName, bool? skipDeleted = null)
    {
      string fileName = Path.Combine(Path.GetDirectoryName(indexFileName), Path.GetFileNameWithoutExtension(indexFileName) + ".");
      string fileExt  = Path.GetExtension(indexFileName);

      IndexFileType indexType = parameters.indexType;

      if (fileExt.ToUpper() == ".NDX")
      {
        indexType = IndexFileType.NDX;
      }
      else if (fileExt.ToUpper() == ".NTX")
      {
        indexType = IndexFileType.NTX;
      }
      // .... others too
      else if (indexType == IndexFileType.Undefined)
      { // Scan extension of files for detect type...
        if (File.Exists(fileName + "NDX"))
        {
          indexType = IndexFileType.NDX;
        }
        else if (File.Exists(fileName + "NTX"))
        {
          indexType = IndexFileType.NTX;
        }
        // .... others too
      }


      switch (indexType)
      {
        case IndexFileType.NTX:  
          fileName += "NTX";
          break;                                   
        case IndexFileType.NDX:
          fileName += "NDX";
          break;
        case IndexFileType.Undefined:
          throw new Exception("JoinIndexFile(): Don't know the format of index file!");
        default:
          throw new Exception("JoinIndexFile(): Don't handle the format of index file!");
      }


      if (File.Exists(fileName))
      {
        var stream = new FileStream(fileName, FileMode.Open, parameters.fileAccess, parameters.fileShare);

        return JoinIndexStream(stream, skipDeleted, indexType);
      }
      else
      {
        throw ExceptionFactory.CreateArgumentException("indexFileName", "Index file does not found! [{0}/{1}]", indexFileName, fileName);
      }
    }

    #endregion

    #region technical ---------------------------------------------------------------------------------------

    private static string GetIndexExtension(DbfTableType tableType)
    {
      string ret = null;

      switch (tableType)
      {
        case DbfTableType.DBF_Ver3_Clipper:
          ret = ".ntx";
          break;

        case DbfTableType.DBF_Ver3_dBase:
          ret = ".ndx";
          break;

        case DbfTableType.Undefined:
          ret = null;
          break;

        default:
          Debug.Fail("GetIndexExtension(): invalid 'DbfTableType' parameter value!");
          break;
      }

      return ret;
    }


    private void StoreTableType(DbfTableType tableType)
    {
      if (parameters.tableType == DbfTableType.Undefined)
      {
        parameters.tableType = tableType; 
      }
      else if (tableType == DbfTableType.Undefined)
      { // Do nothing: Don't forget a better this.tableType value
      }
      else if (parameters.tableType != tableType)
      {
        throw ExceptionFactory.CreateArgumentException("tableType", "Different dbf table type parameter value!");
      }
    }

    public MemoFileType DefaultMemoFileFormatForDbf()
    {
      MemoFileType ret = MemoFileType.Undefined;

      if (_header.newHeaderStructure)
      {
        if (isExistsMemoField)
        {
          ret = MemoFileType.DBT_Ver4;
        }
      }
      else
      {
        switch (_header.type)
        {
          case DbfFileTypes.FoxBASE:
            break;
          case DbfFileTypes.DBase3:
            if (isExistsMemoField)
            {
              ret = MemoFileType.DBT_Ver3;
            }
            break;
          case DbfFileTypes.VisualFoxPro:
            break;
          case DbfFileTypes.VisualFoxPro2:
            break;
          case DbfFileTypes.VisualFoxPro3:
            break;
          case DbfFileTypes.DBase4Sql:
            if (isExistsMemoField)
            {
              ret = MemoFileType.DBT_Ver4;
            }
            break;
          case DbfFileTypes.DBase4SqlSys:
            if (isExistsMemoField)
            {
              ret = MemoFileType.DBT_Ver4;
            }
            break;
          case DbfFileTypes.DBase3M:
             ret = MemoFileType.DBT_Ver3;
            break;
          case DbfFileTypes.DBase4M:
             ret = MemoFileType.DBT_Ver4;
            break;
          case DbfFileTypes.DBase4SqlM:
             ret = MemoFileType.DBT_Ver4;
            break;
          case DbfFileTypes.FoxPro2M:
            break;
          case DbfFileTypes.HiperSix:
            break;
          case DbfFileTypes.FoxBASE2:
            break;
          default:
            ret = MemoFileType.Undefined;
            break;
        }
      }

      return ret;
    }   
    #endregion  
  }
}
