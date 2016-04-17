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
          DisplayMemo();
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
      Console.WriteLine("  '4' : Show memo field content.");
      Console.WriteLine("  '5' : Open NDX index.");
      Console.WriteLine("  '6' : Open NTX index.");
    }
    #endregion

    #region Test '1' --------------------------------------------------------------------------------------

    private static string[] dbfFiles = new string[6];

    static void TestDbaseAndClipperFiles()
    {
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

      using (var dbfTable = DbfTable.Create(fileName1, columns, DbfTable.CodepageCodes.CP_852))
      {

      }

      columns.Add(ColumnDefinitionForCreateTable.MemoField("EEE"));

      using (var dbfTable = DbfTable.Create(fileName2, columns, Encoding.GetEncoding(852), DbfTableType.Clipper))
      {

      }
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

    private static void DisplayMemo()
    {
      string dbfFile = "Test4D.dbf";          // created by DBF Viewer Plus (by http://www.alexnolan.net/)

      DisplayHeader(dbfFile);
      More();

      DisplayRows(dbfFile, false);
      More();

      DisplayRows(dbfFile, true);
      More();

      List<DbfRow> rows = new List<DbfRow>();

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
      Console.WriteLine("AAA: " + row.GetString("AAA"));
      Console.WriteLine("BBB: " + row.GetDecimal("BBB"));
      Console.WriteLine("CCC: " + row.GetDate("CCC")); 
      Console.WriteLine("DDD: " + row.GetBoolean("DDD")); 
      Console.WriteLine("EEE: " + row.GetString("EEE"));
    }
    #endregion

    #region Test '5' ----------------------------------------------------------------------------------------

    private static void OpenNDX()
    {
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
      Console.WriteLine("Don't realized yet!");
    }
    #endregion
  }
}
