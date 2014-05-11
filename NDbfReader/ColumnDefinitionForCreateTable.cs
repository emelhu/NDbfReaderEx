using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  public class ColumnDefinitionForCreateTable
  {
      protected readonly string name_;
      protected readonly short  size_;
      protected readonly short  dec_;
      protected readonly NativeColumnType dbfType_;

      private ColumnDefinitionForCreateTable(string name, NativeColumnType dbfType, short size, short dec = 0)
      {
        if (string.IsNullOrEmpty(name))
        {
          throw new ArgumentNullException("name");
        }

        if (size < 1)
        {
          throw new ArgumentOutOfRangeException("size");
        }

        this.name_    = name;
        this.dbfType_ = dbfType;
        this.size_    = size;
        this.dec_     = dec;
      }

      public static ColumnDefinitionForCreateTable StringField(string name, short size)
      {
        return new ColumnDefinitionForCreateTable(name, NativeColumnType.Char, size);
      }

      public static ColumnDefinitionForCreateTable NumericField(string name, short size, short dec = 0)
      {
        return new ColumnDefinitionForCreateTable(name, NativeColumnType.Numeric, size, dec);
      }

      public static ColumnDefinitionForCreateTable LogicalField(string name)
      {
        return new ColumnDefinitionForCreateTable(name, NativeColumnType.Logical, 1);
      }

      public static ColumnDefinitionForCreateTable DateField(string name)
      {
        return new ColumnDefinitionForCreateTable(name, NativeColumnType.Date, 8);
      }

      public static ColumnDefinitionForCreateTable MemoField(string name)
      {
        return new ColumnDefinitionForCreateTable(name, NativeColumnType.Memo, 10);
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

      /// <summary>
      /// Gets the width to display of a column value.
      /// If column type is memo, returns 0 because it is variable width.
      /// </summary>
      public int displayWidth
      {
        get
        {
          return 0;                                                                     // not relevant
        }
      }


      /// <summary>
      /// Better side to display of a column value.
      /// </summary>
      public bool leftSideDisplay
      {
        get
        {
          return true;                                                                    // not relevant
        }
      }

      /// <summary>
      /// Gets the <c>CLR</c> type of a column value.
      /// </summary>
      // public Type type { get { return typeof(void); } }                                   // not relevant      
    }

}
