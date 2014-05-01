using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// git push -u origin master

namespace NDbfReaderEx_Test
{
  using NDbfReader;

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

        //case '3':
          //DisplayTreeClass();
          //break;    

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
      Console.WriteLine("  '3' : ........");
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
        DisplayRows(filename);
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

    static void DisplayRows(string filename)
    {
      Console.WriteLine("'" + filename + "':");

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
        }


        string line = String.Empty;

        for (int i = 0; (i < test.columns.Count); i++)
        {
          string format = " {0," + (test.columns[i].leftSideDisplay ? "-" : "") + columnWidth[i] + "}";
          line += String.Format(format, test.columns[i].name);
        }

        Console.WriteLine(line);


        foreach (DbfRow row in test)
        {
          line = String.Empty;

          for (int i = 0; (i < test.columns.Count); i++)
          {
            string format = " {0," + (test.columns[i].leftSideDisplay ? "-" : "") + columnWidth[i] + "}";

            if (test.columns[i].type == typeof(DateTime))
            {
              format = " {0:yyyy-MM-dd}";
            }

            
            line += String.Format(format, row.GetValue(test.columns[i]));
          }

          Console.WriteLine(line);
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

    }
    #endregion
  }
}
