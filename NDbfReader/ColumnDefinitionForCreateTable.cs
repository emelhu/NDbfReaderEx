using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  public class ColumnDefinitionForCreateTable : IColumnBase
  {
    protected readonly string name_;
    protected readonly short  size_;
    protected readonly short  dec_;
    protected readonly NativeColumnType dbfType_;

    public static bool onlyUppercaseNameValid = true;
    public static bool dBase3columnDefault    = true;

    private ColumnDefinitionForCreateTable(string name, NativeColumnType dbfType, short size, short dec = 0, bool? dBase3column = null)
    {
      bool dbf3col = dBase3column ?? dBase3columnDefault;

      if (! IsValidName(name, onlyUppercaseNameValid, dbf3col))
      {
        throw new ArgumentOutOfRangeException("name", "Invalid name (character set, length, etc.) !");
      }

      if (size < 1)
      {
        throw new ArgumentOutOfRangeException("size");
      }

      if (! IsValidType(dbfType, dbf3col))
      {
        throw new ArgumentOutOfRangeException("name", "Invalid type for dBaseIII! [" + dbfType.ToString() + "]");
      }      

      this.name_    = name;
      this.dbfType_ = dbfType;
      this.size_    = size;
      this.dec_     = dec;
    }

    public static ColumnDefinitionForCreateTable StringField(string name, short size, bool? dBase3column = null)
    {
      return new ColumnDefinitionForCreateTable(name, NativeColumnType.Char, size, 0, dBase3column);
    }

    public static ColumnDefinitionForCreateTable NumericField(string name, short size, short dec = 0, bool? dBase3column = null)
    {
      return new ColumnDefinitionForCreateTable(name, NativeColumnType.Numeric, size, dec, dBase3column);
    }

    public static ColumnDefinitionForCreateTable NumericField(string name, short size, bool? dBase3column = null)
    {
      return new ColumnDefinitionForCreateTable(name, NativeColumnType.Numeric, size, 0, dBase3column);
    }

    public static ColumnDefinitionForCreateTable LogicalField(string name, bool? dBase3column = null)
    {
      return new ColumnDefinitionForCreateTable(name, NativeColumnType.Logical, 1, 0, dBase3column);
    }

    public static ColumnDefinitionForCreateTable DateField(string name, bool? dBase3column = null)
    {
      return new ColumnDefinitionForCreateTable(name, NativeColumnType.Date, 8, 0, dBase3column);
    }

    public static ColumnDefinitionForCreateTable MemoField(string name, bool? dBase3column = null)
    {
      return new ColumnDefinitionForCreateTable(name, NativeColumnType.Memo, 10, 0, dBase3column);
    }

    // because only dBase3/Clipper DBF file format enabled for create DbfTable, 'I' and 'F' types are not possible.

    /// <summary>
    /// Gets the column name.
    /// </summary>
    public string name
    {
      get
      {
        return name_;
      }
    }

    /// <summary>
    /// Gets the column offset in a row in bytes.
    /// </summary>
    //public int offset
    //{
    //  get
    //  {
    //    return 0;                                                                     // not relevant
    //  }
    //}

    /// <summary>
    /// Gets the column size in bytes.
    /// </summary>
    public short size
    {
      get
      {
        return size_;
      }
    }

    /// <summary>
    /// Gets the number of decimal places in bytes/characters.
    /// </summary>
    public short dec
    {
      get
      {
        return dec_;
      }
    }

    /// <summary>
    /// Gets the <c>DBF</c> type of a column value.
    /// </summary>
    public NativeColumnType dbfType
    {
      get
      {
        return dbfType_;
      }
    }

    public static bool IsValidType(NativeColumnType dbfType, bool? dBase3column = null)
    {
      bool dbf3col = dBase3column ?? dBase3columnDefault;

      if (dbf3col)
      {
        if ((dbfType == NativeColumnType.Long) || (dbfType == NativeColumnType.Float))
        {
          return false;
        }
      }

      return true;
    }
      
    public static bool IsValidName(string name, bool? onlyUppercaseValid = null, bool? dBase3column = null)
    {
      bool dbf3col = dBase3column ?? dBase3columnDefault;

      if (String.IsNullOrWhiteSpace(name))
      {
        return false;
      }

      if (name.Length > (dbf3col ? 10 : 32))
      {
        return false;
      }

        

      for (int i = 0; i < name.Length; i++)
      {
        char c = name[i];

        if ((c == '_') || char.IsDigit(c))
        { // It it is the first character invalid, otherwise correct.
          if (i == 0)
          {
            return false;
          }
        }
        else if ((c >= 'A') && (c <= 'Z'))
        { // OK
        }
        else if ((c >= 'a') && (c <= 'z'))
        {
          if (onlyUppercaseValid ?? onlyUppercaseNameValid)                 // parameter value or static default if null received
          {
            return false;
          }
        }
        else
        { // Invalid character found
          return false;
        }
      }

      return true;
    }
  }
}
