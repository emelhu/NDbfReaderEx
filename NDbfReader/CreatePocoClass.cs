using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

// -- Example of use: --
// string dir = Path.Combine(CreatePocoClass.GetSourceFileDirectory(), @"Data");
// Debug.Assert(Directory.Exists(dir));
// var pocoCreator = new CreatePocoClass(CreatePocoClass.PocoType.SoftWired, dir, "MyAppNamespace.Data", true);
// string dbfName = Path.Combine(dir, "MyAppData.dbf");
// Debug.Assert(File.Exists(dbfName));
// DbfTable dbfTable = DbfTable.Open(dbfName, Encoding.GetEncoding(852), DbfTableType.Clipper);
// pocoCreator.CreateCS("MyAppDataDBF", dbfTable.columns);

namespace NDbfReaderEx
{
  public class CreatePocoClass                                              // new from 1.3 version
  {
    #region const/variables/enum -------------------------------------------------------------------------
    private const string fieldNamesText   = @"/*FIELDNAMES*/";
    private const string fieldPropsText   = @"/*FIELDPROPS*/";
    private const string fieldAssignsText = @"/*FIELDASSIGNS*/";
    private const string namespaceText    = @"NDbfReaderEx.Template";
    private const string namespaceDefault = @"ImportedDBF";   
    private const string dropLineSignal   = @"/*DROP_LINE*/";                     

    public enum PocoType { HardWired, SoftWired, Dynamic };

    private PocoType  pocoType;
    private string    outputDirectory;   
    private string    outputNameSpace;
    private bool      readOnlyProps;
    private bool      nullableProps;

    #endregion

    public CreatePocoClass(PocoType pocoType = PocoType.SoftWired, string outputDirectory = null, string outputNameSpace = null, 
                           bool readOnlyProps = true, bool nullableProps = true)
    {
      this.pocoType         = pocoType; 
      this.outputDirectory  = outputDirectory;
      this.outputNameSpace  = outputNameSpace;
      this.readOnlyProps    = readOnlyProps;
      this.nullableProps    = nullableProps;

      if (String.IsNullOrWhiteSpace(this.outputDirectory))
      {
        this.outputDirectory = @".\";                                     // current directory
      }

      if (String.IsNullOrWhiteSpace(this.outputNameSpace))
      {
        this.outputNameSpace = namespaceDefault;                                      
      }
    }

    public void CreateCS(string className, IEnumerable<IColumn> columns)   // IColumn[] columns
    {
      Debug.Assert((columns != null), "CreatePocoClass.CreateCS(): Null columns parameter!");

      string    fileName      = Path.Combine(outputDirectory, className + ".cs");
      string[]  templateText;      
      string    classNameText;
      
      Directory.CreateDirectory(outputDirectory);
      if (File.Exists(fileName))
      {
        File.Copy(fileName, fileName + "__" + DateTime.Now.ToString("yyyyMMdd-HHmmss"));
        File.Delete(fileName);
      }   

      switch (pocoType)
      {
        case PocoType.HardWired:
          templateText  = SplitLines(NDbfReaderEx.Properties.Resources.CreatePocoClassHardWiredTemplate);
          classNameText = "CreatePocoClassHardWiredTemplate";
          break;
        case PocoType.SoftWired:
          templateText  = SplitLines(NDbfReaderEx.Properties.Resources.CreatePocoClassSoftWiredTemplate);
          classNameText = "CreatePocoClassSoftWiredTemplate";
          break;
        case PocoType.Dynamic:
          templateText  = SplitLines(NDbfReaderEx.Properties.Resources.CreatePocoClassDynamicTemplate);
          classNameText = "CreatePocoClassDynamicTemplate";
          break;
        default:
          Debug.Fail("CreatePocoClass.CreateCS(" + pocoType.ToString() + "): Invalid PocoType parameter!");
          return;
      }


      // Change namespace text
      for (int i = 0; (i < templateText.Length); i++)
      { 
        if (!IsRemarkedLine(templateText[i]) && templateText[i].Contains(namespaceText))
        {
          templateText[i] = templateText[i].Replace(namespaceText, outputNameSpace);
          break;
        }
      }

      // Change class name text
      for (int i = 0; (i < templateText.Length); i++)
      { 
        if (!IsRemarkedLine(templateText[i]) && templateText[i].Contains(classNameText))
        {
          templateText[i] = templateText[i].Replace(classNameText, className); 
          // break;   --- don't break, because we can found more then one class name line 
        }
      }

      // Insert fieldnames and they position definitions inside enum definition
      for (int i = 0; (i < templateText.Length); i++)
      { 
        if (!IsRemarkedLine(templateText[i]) && templateText[i].Contains(fieldNamesText))
        {
          templateText[i] = templateText[i].Replace(fieldNamesText, GetFieldNamesText(columns));          
          break;
        }
      }

      // Insert property definitions inside class definition
      for (int i = 0; (i < templateText.Length); i++)
      { 
        if (!IsRemarkedLine(templateText[i]) && templateText[i].Contains(fieldPropsText))
        {
          int    margin   = templateText[i].IndexOf(fieldPropsText);
          templateText[i] = GetFieldProps(columns, margin);             
          break;
        }
      }      

      // Insert property definitions inside class definition
      for (int i = 0; (i < templateText.Length); i++)
      { 
        if (!IsRemarkedLine(templateText[i]) && templateText[i].Contains(fieldAssignsText))
        {
          int    margin   = templateText[i].IndexOf(fieldAssignsText);
          templateText[i] = GetFieldAssigns(columns, margin);             
          break;
        }
      }      

      //

      string outFile = Path.Combine(outputDirectory, className + ".cs");

      var templateTextFiltered = templateText.Where(a => ! IsDropLineSignaledLine(a));

      File.WriteAllLines(outFile, templateTextFiltered, Encoding.UTF8);
    }    

    #region 

    private string GetFieldNamesText(IEnumerable<IColumn> columns)
    {
      string names = "!!!";

      switch (pocoType)
      {
        case PocoType.HardWired:
          {
            var namesList = from p in columns 
                             select p.name; 
            names = string.Join(",", namesList);
          }      
          
          break;

        case PocoType.SoftWired:
          {
            var namesList = from p in columns 
                             select p.name; 
            names = string.Join(",", namesList);
          }      
          
          break;

        case PocoType.Dynamic:
          {
            var namesList = from p in columns 
                             select p.name; 
            names = string.Join(",", namesList);
          }          
          break;
      }

      return names;
    }

    private string GetFieldProps(IEnumerable<IColumn> columns, int margin)
    {
      StringBuilder ret       = new StringBuilder();
      string        marginStr = new String(' ', margin);

      foreach (var column in columns)
      {
        ret.AppendFormat("{0}public {1,-20}{2,-12}", marginStr, GetTemplateString(column, TemplateStringType.type), column.name);

        if (readOnlyProps)
        {
          ret.Append("{ get; set; }");
        }
        else
        {
          ret.Append("{ get; private set; }");
        }

        ret.Append(Environment.NewLine);
      }

      return ret.ToString();
    }

    private string GetFieldAssigns(IEnumerable<IColumn> columns, int margin)
    {
      StringBuilder ret       = new StringBuilder();
      string        marginStr = new String(' ', margin);

      switch (pocoType)
      {
        case PocoType.Dynamic:
        case PocoType.SoftWired:
          //column = row.FindColumnByName("FIELDNAME");         // template for generated code  --- Dynamic |OR|
          //column = fieldColumns[(int)FieldNames.FIELDNAME];   // template for generated code  --- SoftWired
          //if ((column != null) && ! row.IsNull(column))       // template for generated code  --- SoftWired/Dynamic
          //{                                                   // template for generated code  --- SoftWired/Dynamic
          //  FIELDNAME = row.GetInt32(column);                 // template for generated code  --- SoftWired/Dynamic
          //}    
          foreach (var column in columns)
          {
            if (pocoType == PocoType.Dynamic)
            {
              ret.AppendFormat("{0}column = row.FindColumnByName(\"{1}\", partiallyFilledEnable);{2}", marginStr, column.name, Environment.NewLine);
            }
            else
            {
              ret.AppendFormat("{0}column = fieldColumns[(int)FieldNames.{1}];{2}", marginStr, column.name, Environment.NewLine);
            }

            if (nullableProps)
            {
              ret.AppendFormat("{0}if ((column != null) && ! row.IsNull(column)){1}", marginStr, Environment.NewLine);
            }
            else
            {
              ret.AppendFormat("{0}if (column != null){1}", marginStr, Environment.NewLine);
            }

            ret.AppendFormat("{0}{1}{2}", marginStr, '{', Environment.NewLine);
            ret.AppendFormat("{0}  {1} = {2}row.{3}(column);{4}", marginStr, column.name, GetTemplateString(column, TemplateStringType.conversion), GetTemplateString(column, TemplateStringType.readFunc), Environment.NewLine);
            ret.AppendFormat("{0}{1}{2}", marginStr, '}', Environment.NewLine);
            ret.Append(Environment.NewLine);
          }
          break;

        case PocoType.HardWired:
          //FIELDNAME = row.GetInt32(row.columns[(int)FieldNames.FIELDNAME]);   // template for generated code  --- HardWired
          foreach (var column in columns)
          {
            ret.AppendFormat("{0}{1} = row.{2}(row.column[(int)FieldNames.{1}]);{3}", marginStr, column.name, GetTemplateString(column, TemplateStringType.readFunc), Environment.NewLine);
            ret.Append(Environment.NewLine);
          }
          break;

        default:
          ret.Append("!!!ERROR!!!");
          break;
      }

      return ret.ToString();
    }

    #endregion

    #region Helper functions ==============================================================================

    /// <summary>
    /// Split string to lines at CR/LF characters.
    /// </summary>
    /// <param name="source">source of splitting</param>
    /// <param name="opt">optional split option</param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string[] SplitLines(string source, StringSplitOptions opt = StringSplitOptions.None) 
    {
      if (source == null)
      {
        return new string[0];
      }

      return source.Split(new string[] { "\r\n", "\n" }, opt);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsRemarkedLine(string line)
    {
      line = line.TrimStart(' ', '\t');

      return (line.StartsWith("//"));      
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsDropLineSignaledLine(string line)
    {
      return (line.Contains(dropLineSignal));      
    }

    private enum TemplateStringType {type, readFunc, conversion};

    private string GetTemplateString(IColumn column, TemplateStringType type)
    {
      string[] keywords;

      switch (type) 
      {
        case TemplateStringType.type:
          //                        0            1            2           3           4             5             6            7             8 32bit
          keywords = new string[] {"string",    "string",    "DateTime", "int",      "bool",       "decimal",    "double",     "Int64",      "Int32"};
          break;
        case TemplateStringType.readFunc:
          keywords = new string[] {"GetString", "GetString", "GetDate",  "GetInt32", "GetBoolean", "GetDecimal", "GetDecimal", "GetDecimal", "GetDecimal"};
          break;
        case TemplateStringType.conversion: 
          keywords = new string[] {"",          "",          "",         "",         "",           "",           "(double)",   "(Int64)",    "(Int32)"};
          break;
        default:
          throw new Exception("GetTemplateString(type='" + ((int)type).ToString() + "'): Invalid 'TemplateStringType' parameter!");;
      }

      switch (column.dbfType)
      {
        case NativeColumnType.Char:
          return keywords[0];
        case NativeColumnType.Memo:
          return keywords[1];
        case NativeColumnType.Date:
          return keywords[2];
        case NativeColumnType.Long:
          return keywords[3];
        case NativeColumnType.Logical:
          return keywords[4];
        case NativeColumnType.Numeric:
          if ((column.size < 10) && (column.dec == 0))
          { // Reduce to integer (32 bit)
            return keywords[8];
          }
          else if ((column.size < 19) && (column.dec == 0))
          { // Reduce to long (64 bit)
            return keywords[7];
          }
          return keywords[5];
        case NativeColumnType.Float:
          return keywords[6];
        default:
          throw new Exception("Invalid 'dbfType' at '" + column.name + "' column!");
      }
    }

    public static string GetSourceFileDirectory([CallerFilePath] string sourceFilePath = "")
    {
      return Path.GetDirectoryName(sourceFilePath);      
    }

    #endregion
  }
}
