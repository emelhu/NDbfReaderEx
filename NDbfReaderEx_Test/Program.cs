using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// git push -u origin master

namespace NDbfReaderEx_Test
{
  using System.IO;
  using NDbfReaderEx;

  class Program
  {
    #region Start test --------------------------------------------------------------------------------------

    static void Main(string[] args)
    {
      Console.WriteLine("*******************************************************************************");
      Console.WriteLine("************************* NDbfReaderEx_Test by eMeL ***************************");
      Console.WriteLine("*******************************************************************************");
      Console.WriteLine();


      char operation;

      if (args.Length > 1)
      { // first parameter [index: 0] is the name of program
        operation = args[1][0];
      }
      else
      {
        Console.WriteLine("Select an NDbfReaderEx_Test operation:\n");
        DisplayHelp(false);

        Console.Write("\nPlease press the key of operation: ");

        operation = (char)Console.ReadKey().KeyChar;

        Console.WriteLine();
      }

      //

      var  start            = DateTime.Now;
      bool dispEllaptedTime = false;

      switch (operation)
      {
        case '1':
          TestDbaseAndClipperFiles();                                                    
          dispEllaptedTime = false;
          break;

        case '2':
          CreateTableAndWriteRows();
          dispEllaptedTime = true;
          break;

        case '3':
          ShowRawDataTo();
          dispEllaptedTime = false;
          break;

        case '4':
          DisplayMemo(true);
          dispEllaptedTime = false;
          break;

        case '5':
          OpenNDX();
          dispEllaptedTime = false;
          break;

        case '6':
          OpenNTX();
          dispEllaptedTime = false;
          break;

        case '7':
          ReadMultipleDbfDbtStruct();                                                    
          dispEllaptedTime = false;
          break;

        case '8':
          DisplayMemo(false);
          dispEllaptedTime = false;
          break;

        default:
          if (args.Length > 1)
          {
            DisplayHelp(true);
          }
          else
          {
            Console.WriteLine("!!!! invalid operation code !!!!");
          }
          dispEllaptedTime = false;
          break;
      }

      //

      if (dispEllaptedTime)
      {
        var stop = DateTime.Now;

        TimeSpan elapsed = stop.Subtract(start);

        Console.Write(" Elapsed time: ");
        Console.WriteLine(elapsed.ToString());
      }         

      Console.WriteLine();
      Console.WriteLine("...press 'enter' to close window.");
      Console.ReadLine();      
    }

    public static void DisplayHelp(bool fullText)
    {
      if (fullText)
      {
        Console.WriteLine("usage NDbfReaderEx_Test <operation code>");
        Console.WriteLine("  operation codes:");
      }

      Console.WriteLine("  '1' : Test read/content of dBbaseIII and Clipper files.");
      Console.WriteLine("  '2' : Create test file and write rows/content.");
      Console.WriteLine("  '3' : Show raw data too.");
      Console.WriteLine("  '4' : Show memo field content dBase3.");
      Console.WriteLine("  '5' : Open NDX index.");
      Console.WriteLine("  '6' : Open NTX index.");
      Console.WriteLine("  '7' : Show fields/memo from 3/4/7 version of dbf/dbt structure.");
      Console.WriteLine("  '8' : Show memo field content dBase4.");
    }
    #endregion

    #region Test '1' --------------------------------------------------------------------------------------

    static void TestDbaseAndClipperFiles()
    {
      string[] dbfFiles = new string[6];

      dbfFiles[0] = "Test1.dbf";                                    // empty dBase
      dbfFiles[1] = "Test1C.dbf";                                   // empty Clipper
      dbfFiles[2] = "Test2.dbf";                                    // one null row dBase
      dbfFiles[3] = "Test2C.dbf";                                   // one null row Clipper
      dbfFiles[4] = "Test3.dbf";                                    // rows dBase
      dbfFiles[5] = "Test3C.dbf";                                   // rows Clipper

      foreach (string filename in dbfFiles)
      {
        DisplayHeader(filename);
        More();
      }

      foreach (string filename in dbfFiles)
      {
        DisplayRows(filename, false);
        More();
      }      
    }

    // Test '7'
    static void ReadMultipleDbfDbtStruct()
    {
      string[] dbfFiles = new string[6];

      dbfFiles[0] = "BDE_TEST_3_512.dbf";
      dbfFiles[1] = "BDE_TEST_4_512.dbf";
      dbfFiles[2] = "BDE_TEST_4_64.dbf";
      dbfFiles[3] = "BDE_TEST_7_1024.dbf";
      dbfFiles[4] = "BDE_TEST_7_2048.dbf";
      dbfFiles[5] = "BDE_TEST_7_64.dbf";
      
      DisplayHeader(dbfFiles[0]);
      More();

      foreach (string filename in dbfFiles)
      {
        DisplayRows(filename, false);
        More();
      }      
    }

    static void DisplayHeader(string filename)
    {
      Console.WriteLine("'" + filename + "':");

      using (DbfTable test = DbfTable.Open(filename, Encoding.GetEncoding(437)))
      {
        foreach (var column in test.columns)
        {
          string name = "'" + column.name + "'";
          char   type = Convert.ToChar(column.dbfType);

          int    offset = ((Column)column).offset;

          Console.WriteLine("{0,-12} {1} {2,4}.{3,-2} {4} [{5}]", name, type, column.size, column.dec, column.type.ToString(), offset);
        }
      }

      Console.WriteLine("===============================================================================");
    }

    static void DisplayRows(string filename, bool rawDataToo = false)
    {
      Console.WriteLine("'" + filename + "':");

      bool nullFound = false;

      using (DbfTable test = DbfTable.Open(filename, null))                         // null: test codepage byte from DBF header
      {
      }

      using (DbfTable test = DbfTable.Open(filename, Encoding.GetEncoding(437)))
      {
        int[] columnWidth = new int[test.columns.Count];

        for (int i = 0; (i < test.columns.Count); i++)
        {
          int width = test.columns[i].displayWidth;

          if (width == 0)
          {
            width = 20;                                                                   // variable length data, for example Memo
          }

          columnWidth[i] = Math.Max(width, test.columns[i].name.Length);

          if (test.columns[i].type == typeof(bool))
          {
            if (columnWidth[i] < 5)
            {
              columnWidth[i] = 5;
            }
          }
        }


        string line = String.Empty;

        for (int i = 0; (i < test.columns.Count); i++)
        {
          string format = "|{0," + (test.columns[i].leftSideDisplay ? "-" : "") + columnWidth[i] + "}";
          line += String.Format(format, test.columns[i].name);
        }

        line += "|";
        Console.WriteLine(line);


        foreach (DbfRow row in test)
        {
          line = String.Empty;

          for (int i = 0; (i < test.columns.Count); i++)
          {
            IColumn col = test.columns[i];

            string format = "|{0," + (col.leftSideDisplay ? "-" : "") + columnWidth[i] + "}";

            if (col.type == typeof(DateTime))
            {
              format = "|{0:yyyy-MM-dd}";
            }

            object output = row.GetValue(col);

            if (row.IsNull(col))
            {
              output = "¤";

              if (col.type == typeof(DateTime))
              {
                format = "|{0,10}";
              }

              nullFound = true;
            }

            line += String.Format(format, output);
          }

          line += "|";
          Console.WriteLine(line);

          //

          if (rawDataToo)
          {
            line = String.Empty;

            for (int i = 0; (i < test.columns.Count); i++)
            {
              string format = "|{0,-" + columnWidth[i] + "}";

              string output = row.GetRawString(test.columns[i]);

              if (output.Length < columnWidth[i])
              {
                output += "˘";
              }

              line += String.Format(format, output);
            }

            line += "| <RAW";
            Console.WriteLine(line);
          }
        }

        if (nullFound)
        {
          Console.WriteLine("['¤' means null field state]");
        }
      }

      Console.WriteLine("===============================================================================");
    }

    static void More()
    {
      Console.WriteLine("More...");
      Console.ReadLine();
    }
    #endregion

    #region Test '2' --------------------------------------------------------------------------------------

    private static void CreateTableAndWriteRows()
    {
      {
        string fileName1 = "createdTest1.dbf";
        string fileName2 = "createdTest2.dbf";

        File.Delete(fileName1);                                                // DbfTable.Create can't overwrite exists file - it's a precautionary measure.
        File.Delete(fileName2);                                                // according to plan --''--
        File.Delete("createdTest2.dbt");                                       // according to plan: CreateHeader_Memo can't overwrites existing DBT file. 

        var columns = new List<ColumnDefinitionForCreateTable>();

        columns.Add(ColumnDefinitionForCreateTable.StringField("AAA", 10));
        columns.Add(ColumnDefinitionForCreateTable.NumericField("BBB", 8, 3));
        columns.Add(ColumnDefinitionForCreateTable.DateField("CCC"));
        columns.Add(ColumnDefinitionForCreateTable.LogicalField("DDD"));

        using (var dbfTable = DbfTable.Create(fileName1, columns, DbfTable.CodepageCodes.CP_852, DbfTableType.DBF_Ver3_dBase))
        {
          Console.WriteLine("{0} file created.", Path.GetFullPath(fileName1));
        }

        columns.Add(ColumnDefinitionForCreateTable.MemoField("EEE"));

        using (var dbfTable = DbfTable.Create(fileName2, columns, Encoding.GetEncoding(852), DbfTableType.DBF_Ver3))
        {
          Console.WriteLine("{0} file created.", Path.GetFullPath(fileName2));
        }

        //--- There isn't write code to add rows... yet
      }

      // TODO: !!!

      //{
      //  string fileName3 = "createdTest3.dbf";
      //  string fileName4 = "createdTest4.dbf";

      //  File.Delete(fileName3);                                                // DbfTable.Create can't overwrite exists file - it's a precautionary measure.
      //  File.Delete(fileName4);                                                // according to plan --''--
      //  File.Delete("createdTest4.dbt");                                       // according to plan: CreateHeader_Memo can't overwrites existing DBT file. 

      //  var columns = new List<ColumnDefinitionForCreateTable>();

      //  columns.Add(ColumnDefinitionForCreateTable.StringField("AAAAAAAAAAAAAAA", 10, false));         // Name length more then 10
      //  columns.Add(ColumnDefinitionForCreateTable.NumericField("BBBBBBBBBBBBBBB", 8, 3, false));
      //  columns.Add(ColumnDefinitionForCreateTable.DateField("CCCCCCCCCCCCCCC", false));
      //  columns.Add(ColumnDefinitionForCreateTable.LogicalField("DDDDDDDDDDDDDDD", false));

      //  using (var dbfTable = DbfTable.Create(fileName3, columns, DbfTable.CodepageCodes.CP_852, DbfTableType.DBF_Ver7))
      //  {

      //  }

      //  columns.Add(ColumnDefinitionForCreateTable.MemoField("EEEEEEEEEEEEEEE", false));
      //  columns.Add(ColumnDefinitionForCreateTable.NumericField("FFFFFFFFFFFFFFF", 4, false));

      //  using (var dbfTable = DbfTable.Create(fileName4, columns, Encoding.GetEncoding(852), DbfTableType.DBF_Ver7))
      //  {

      //  }

      //  //--- There isn't write code to add rows... yet
      //}
    }
    #endregion

    #region Test '3' ----------------------------------------------------------------------------------------

    private static void ShowRawDataTo()
    {
      DisplayRows("Test3.dbf", true);
      More();

      DisplayRows("Test3C.dbf", true);
    }
    #endregion

    #region Test '4' ----------------------------------------------------------------------------------------

    private static void DisplayMemo(bool dBase3File)
    {
      string dbfFile;          

      if (dBase3File)
      {
        dbfFile = "BDE_TEST_3_512.dbf";                                 // created by DBF Viewer Plus (by http://www.alexnolan.net/)
      }
      else
      {
        dbfFile = "BDE_TEST_4_512.dbf";                            // created by BDE (Borland Database Engine)
      }

      

      DisplayHeader(dbfFile);
      More();

      DisplayRows(dbfFile, false);
      More();

      DisplayRows(dbfFile, true);
      More();

      List<DbfRow> rows = new List<DbfRow>();

      if (dBase3File)
      {
        using (DbfTable test = DbfTable.Open(dbfFile, Encoding.GetEncoding(437)))
        {
          foreach (DbfRow row in test)
          { // Store for detached mode
            rows.Add(row);
          }

          DisplayTest4DFields(rows);
          More();
        }

        Console.WriteLine("Detached rows:");
        DisplayTest4DFields(rows);
      }      
    }

    static void DisplayTest4DFields(DbfTable table)
    {
      foreach (DbfRow row in table)
      {
        DisplayTest4DField(row);
      }
    }

    static void DisplayTest4DFields(List<DbfRow> rows)
    {
      foreach (DbfRow row in rows)
      {
        DisplayTest4DField(row);
      }
    }

    static void DisplayTest4DField(DbfRow row)
    {
      Console.WriteLine();
      Console.WriteLine("C10: " + row.GetString("C10"));
      Console.WriteLine("D:   " + row.GetDate("D").ToShortDateString());
      Console.WriteLine("L:   " + row.GetBoolean("L")); 
      Console.WriteLine("I:   " + row.GetDecimal("I"));                                       // row.GetInt32("I") + " | " + 
      Console.WriteLine("N5:  " + row.GetDecimal("N5"));
      Console.WriteLine("N10: " + row.GetDecimal("N10"));
      Console.WriteLine("B:   " + row.GetString("B"));
    }
    #endregion

    #region Test '5' ----------------------------------------------------------------------------------------

    private static void OpenNDX()
    {
      Console.WriteLine("Isn't realized fully yet!");

      string dbfName = "test_ix.dbf";
      string ndxName = "test_ix.ndx";

      using (DbfTable test = DbfTable.Open(dbfName, Encoding.GetEncoding(437)))
      {
        Console.WriteLine();
        Console.WriteLine("Open index: " + ndxName);

        IIndexFile ndx = test.JoinIndexFile(ndxName);

        Console.WriteLine("Index key : " + ndx.KeyExpression);
        Console.WriteLine();

        var row = ndx.Top();

        Console.WriteLine("----------------------------- 'AAA' field --------------------------------------");
        Console.WriteLine("Top()   : '" + row.GetString("AAA") + "'");

        row = ndx.Bottom();

        Console.WriteLine();
        Console.WriteLine("Bottom(): '" + row.GetString("AAA") + "'");

        Console.WriteLine("All rows:");
        row = ndx.Top();                                                    // dBase/Clipper style read

        int rowCount = 0;

        while (! ndx.eof)
        {
          rowCount++;

          Console.WriteLine(rowCount.ToString("00000") + ": '" + row.GetString("AAA") + "'");
          row = ndx.Next();         
        }

        Console.WriteLine("---eof---");
      }
    }
    #endregion

    #region Test '6' ----------------------------------------------------------------------------------------

    private static void OpenNTX()
    {
      Console.WriteLine("Isn't realized yet!");
    }
    #endregion
  }
}
