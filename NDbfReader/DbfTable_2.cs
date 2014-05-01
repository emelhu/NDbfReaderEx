using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace NDbfReader
{
  public partial class DbfTable : IDisposable
  {
    #region variables/constants ---------------------------------------------------------------------------

    private const int  codepageBytePosition   = 29;                     // http://www.dbf2002.com/dbf-file-format.html   "Code page mark"
    private const int  firstFieldSubrecord    = 32;                     // http://www.dbf2002.com/dbf-file-format.html   "32 – n	: Field subrecords"
    private const byte headerRecordTerminator = 0x0D;                   // http://www.dbf2002.com/dbf-file-format.html   "n+1: Header record terminator (0x0D)"
    private const int  maxColumnCount         = 1024;                   // I extend it for myself: original 255  http://msdn.microsoft.com/en-us/library/3kfd3hw9(v=vs.80).aspx
    private const int  minDbfFileLength       = 32 + 32 + 1;            // http://www.dbf2002.com/dbf-file-format.html   (header info + 1 filed + headerRecordTerminator)

    #endregion

    #region enums/constans ----------------------------------------------------------------------------------
    
    /// <summary>
    /// Valid codepage bytes by standard codepage names for DBF file.
    /// information have got from http://forums.esri.com/Thread.asp?c=93&f=1170&t=197185#587982
    /// </summary>
    public enum CodepageCodes : byte
    {
      OEM     = 0x00,                                           // OEM = 0 
      CP_437  = 0x01,                                           // Codepage_437_US_MSDOS = &H1 
      CP_850  = 0x02,                                           // Codepage_850_International_MSDOS = &H2 
      CP_1252 = 0x03,                                           // Codepage_1252_Windows_ANSI = &H3 
      ANSI    = 0x57,                                           // ANSI = &H57 
      CP_737  = 0x6A,                                           // Codepage_737_Greek_MSDOS = &H6A 
      CP_852  = 0x64,                                           // Codepage_852_EasernEuropean_MSDOS = &H64 
      CP_857  = 0x6B,                                           // Codepage_857_Turkish_MSDOS = &H6B 
      CP_861  = 0x67,                                           // Codepage_861_Icelandic_MSDOS = &H67 
      CP_865  = 0x66,                                           // Codepage_865_Nordic_MSDOS = &H66 
      CP_866  = 0x65,                                           // Codepage_866_Russian_MSDOS = &H65 
      CP_950  = 0x78,                                           // Codepage_950_Chinese_Windows = &H78 
      CP_936  = 0x7A,                                           // Codepage_936_Chinese_Windows = &H7A 
      CP_932  = 0x7B,                                           // Codepage_932_Japanese_Windows = &H7B 
      CP_1255 = 0x7D,                                           // Codepage_1255_Hebrew_Windows = &H7D 
      CP_1256 = 0x7E,                                           // Codepage_1256_Arabic_Windows = &H7E 
      CP_1250 = 0xC8,                                           // Codepage_1250_Eastern_European_Windows = &HC8 
      CP_1251 = 0xC9,                                           // Codepage_1251_Russian_Windows = &HC9 
      CP_1254 = 0xCA,                                           // Codepage_1254_Turkish_Windows = &HCA 
      CP_1253 = 0xCB                                            // Codepage_1253_Greek_Windows = &HCB 
    };

    internal struct CodepageCodes_Encoding
    {
      internal CodepageCodes code;
      internal int           codepage;
    };

    private static CodepageCodes_Encoding[] codepageCodes_Encodings;

    private static void Initialize_CodepageCodes_Encoding()
    {
      codepageCodes_Encodings = new CodepageCodes_Encoding[20];                 // !Warning: "Magic number" (equals as count of enum CodepageCodes items)

      codepageCodes_Encodings[0]  = new CodepageCodes_Encoding() { code = CodepageCodes.OEM,     codepage = 0    };
      codepageCodes_Encodings[1]  = new CodepageCodes_Encoding() { code = CodepageCodes.CP_437,  codepage = 437  };
      codepageCodes_Encodings[2]  = new CodepageCodes_Encoding() { code = CodepageCodes.CP_850,  codepage = 850  };
      codepageCodes_Encodings[3]  = new CodepageCodes_Encoding() { code = CodepageCodes.CP_1252, codepage = 1252 };
      codepageCodes_Encodings[4]  = new CodepageCodes_Encoding() { code = CodepageCodes.CP_737,  codepage = 737  };
      codepageCodes_Encodings[5]  = new CodepageCodes_Encoding() { code = CodepageCodes.CP_852,  codepage = 852  };
      codepageCodes_Encodings[6]  = new CodepageCodes_Encoding() { code = CodepageCodes.CP_857,  codepage = 857  };
      codepageCodes_Encodings[7]  = new CodepageCodes_Encoding() { code = CodepageCodes.CP_861,  codepage = 861  };
      codepageCodes_Encodings[8]  = new CodepageCodes_Encoding() { code = CodepageCodes.CP_865,  codepage = 865  };
      codepageCodes_Encodings[9]  = new CodepageCodes_Encoding() { code = CodepageCodes.CP_866,  codepage = 866  };
      codepageCodes_Encodings[10] = new CodepageCodes_Encoding() { code = CodepageCodes.CP_950,  codepage = 950  };
      codepageCodes_Encodings[11] = new CodepageCodes_Encoding() { code = CodepageCodes.CP_936,  codepage = 936  };
      codepageCodes_Encodings[12] = new CodepageCodes_Encoding() { code = CodepageCodes.CP_932,  codepage = 932  };
      codepageCodes_Encodings[13] = new CodepageCodes_Encoding() { code = CodepageCodes.CP_1255, codepage = 1255 };
      codepageCodes_Encodings[14] = new CodepageCodes_Encoding() { code = CodepageCodes.CP_1256, codepage = 1256 };
      codepageCodes_Encodings[15] = new CodepageCodes_Encoding() { code = CodepageCodes.CP_1250, codepage = 1250 };
      codepageCodes_Encodings[16] = new CodepageCodes_Encoding() { code = CodepageCodes.CP_1251, codepage = 1251 };
      codepageCodes_Encodings[17] = new CodepageCodes_Encoding() { code = CodepageCodes.CP_1254, codepage = 1254 };
      codepageCodes_Encodings[18] = new CodepageCodes_Encoding() { code = CodepageCodes.CP_1253, codepage = 1253 };
      codepageCodes_Encodings[19] = new CodepageCodes_Encoding() { code = CodepageCodes.ANSI,    codepage = 0    };      
    }


    /// <summary>
    /// http://www.dbf2002.com/dbf-file-format.html
    /// </summary>
    public enum DbfFileTypes : byte
    {
      FoxBASE       = 0x02,                                     // FoxBASE
      DBase3        = 0x03,                                     // FoxBASE+/Dbase III plus, no memo
      VisualFoxPro  = 0x30,                                     // Visual FoxPro
      VisualFoxPro2 = 0x31,                                     // Visual FoxPro, autoincrement enabled
      VisualFoxPro3 = 0x32,                                     // Visual FoxPro with field type Varchar or Varbinary
      DBase4Sql     = 0x43,                                     // dBASE IV SQL table files, no memo
      DBase4SqlSys  = 0x63,                                     // dBASE IV SQL system files, no memo
      DBase3M       = 0x83,                                     // FoxBASE+/dBASE III PLUS, with memo
      DBase4M       = 0x8B,                                     // dBASE IV with memo
      DBase4SqlM    = 0xCB,                                     // dBASE IV SQL table files, with memo
      FoxPro2M      = 0xF5,                                     // FoxPro 2.x (or earlier) with memo
      HiperSix      = 0xE5,                                     // HiPer-Six format with SMT memo file
      FoxBASE2      = 0xFB                                      // FoxBASE
    };
    #endregion

    #region ReadDbfHeader_* ---------------------------------------------------------------------------------

    public static DbfHeader ReadDbfHeader(Stream stream)
    {
      DbfHeader header;

      stream.Position = 0;                                                      // start from first byte

      if (stream.Length < minDbfFileLength)
      {
        throw new IOException("Not a DBF file! ('DBF File length' is too short!)");
      }

      BinaryReader reader = new BinaryReader(stream);                           // don't use using '(BinaryReader reader...' because 'using' dispose 'stream' too!
      {        
        header.type = (DbfFileTypes)(reader.ReadByte());                        // pos: 0          -- DBF File type: 

        if (Array.IndexOf(Enum.GetValues(typeof(DbfFileTypes)), header.type) < 0)
        {
          throw new IOException("Not a DBF file! ('DBF File type' is not valid!)");
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
        header.rowLength             = reader.ReadInt16();

        if (header.recCount < 0)                
        {
          throw new IOException("Not a DBF file! (Number of records in file error!)");
        }

        if ((header.firstRecordPosition < 65) || (header.firstRecordPosition > 64 + 32 * maxColumnCount + 1 + 264) ||
            (header.firstRecordPosition >= stream.Length))                    // !WARNING: I don't know valid limit
        {
          throw new IOException("Not a DBF file! (firstRecordPosition)");
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
      int codepage = (from cpi in codepageCodes_Encodings
                      where cpi.code == code
                      select cpi.codepage).FirstOrDefault();

      if (codepage == 0)
      { // not found or stored by 0
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

    public static IColumn[] ReadDbfColumns(Stream stream, Encoding encoding)
    {
      List<IColumn> columns = new List<IColumn>();

      stream.Position = firstFieldSubrecord;

      int calcOffset = 0;

      for (int i = 0; (i < maxColumnCount); i++)
      {
        IColumn column = GetNextColumnDefinition(stream, encoding, calcOffset);

        if (column == null)
        { // No more column definition
          break;
        }
        else
        { // Store column definition
          columns.Add(column);
          calcOffset += column.size;
        }
      }

      if (columns.Count < 1)
      {
        throw new Exception("DBF file format error! (At least one column definition required!)");
      }

      return columns.ToArray();                                                            
    }

    private static Column GetNextColumnDefinition(Stream stream, Encoding encoding, int calcOffset)
    { // If there isn't more column definition return null.
      string            columnName;
      NativeColumnType  columnType;
      int               columnOffset;                                           // isn't stored by dBase/Clipper, must calculate by 'calcOffset'
      short             columnSize;
      short             columnDec;

      //

      byte[] buffer = new byte[11];                                             // http://www.dbf2002.com/dbf-file-format.html "Field name with a maximum of 10 characters. If less than 10, it is padded with null characters (0x00)."

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
        int nameLen = buffer.Length;

        for (int i = 0; (i < buffer.Length); i++)
        {
          if ((buffer[i] == 0x00) || (buffer[i] == 0x20))                                     // name is closed and/or trimmed blanks
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

      columnType   = (NativeColumnType)reader.ReadByte();                                                 
      columnOffset = reader.ReadInt32();
      columnSize   = reader.ReadByte();
      columnDec    = reader.ReadByte();
        
      reader.ReadBytes(14);                                                           // skip don't used and reserved caharacters
      reader = null;

      //

      columnOffset = calcOffset;                                                      // isn't stored in header by dBase/Clipper, must calculate by 'calcOffset'                                                  

      switch (columnType)
      {
        case NativeColumnType.Char:
          return new StringColumn(columnName, columnType, columnOffset, columnSize, columnDec, encoding);
        case NativeColumnType.Date:
          return new DateTimeColumn(columnName, columnType, columnOffset);
        case NativeColumnType.Long:
          return new Int32Column(columnName, columnType, columnOffset);
        case NativeColumnType.Logical:
          return new BooleanColumn(columnName, columnType, columnOffset);
        case NativeColumnType.Numeric:
        case NativeColumnType.Float:
          return new DecimalColumn(columnName, columnType, columnOffset, columnSize, columnDec);
        default:
          throw ExceptionFactory.CreateNotSupportedException("The {0} column's type '{1}' is not supported.", columnName, columnType);
      }
    }

    #endregion

    #region row read/write ----------------------------------------------------------------------------------

    public DbfRow GetRow(int recNo, bool throwException = false)
    {
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

      return new DbfRow(recNo, buffer, _columns);
    }

    public bool UpdateRow(DbfRow row, bool throwException = true)
    {
      if (row == null)
      {
        throw ExceptionFactory.CreateArgumentException("UpdateRow(row)", "Null parameter invalid!");
      }


      if ((row.recNo < 0) || (row.recNo >= recCount))
      {
        if (throwException)
        {
          throw new Exception(String.Format("DbfTable.GetRow({0}): invalid record number! [count of records: {1}]", recNo, recCount));
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

    public bool InsertRow(DbfRow row, bool throwException = true)
    {
      if (row == null)
      {
        throw ExceptionFactory.CreateArgumentException("InsertRow(row)", "Null parameter invalid!");
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

      if (row.recNo == int.MinValue)
      {
        return InsertRow(row, throwException);
      }
      else
      {
        return UpdateRow(row, throwException);
      }
    }

    #endregion

    #region row data read -----------------------------------------------------------------------------------
    #endregion


  }
}
