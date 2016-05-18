using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

// template source is CreatePocoClassSoftWiredTemplate.cs
// If any line contains /*DROP_LINE*/ text, it will not copy to target

namespace NDbfReaderEx.Template
{
  using NDbfReaderEx;

  public class CreatePocoClassSoftWiredTemplate
  { 
    public const  CreatePocoClass.PocoType type = CreatePocoClass.PocoType.SoftWired;
    public static bool partiallyFilledEnable { get; private set; }

    // enum definition for fields of DBF table
    // '/*FIELDNAMES*/' changed to properties definition, do not delete next line!  /*DROP_LINE*/
    private enum   FieldNames 
                   { 
                      FIELDNAME         /*DROP_LINE*/     // syntax check/template for generated code                      
                      /*FIELDNAMES*/ 
                   };
    private static IColumn[] fieldColumns = null;

    [CLSCompliant(false)]  
    public int _recNo_ { get; private set; }

    // properties for fields of DBF table (you can change type to nullable (with add '?' character if "row.IsNull()" exists in 'assignment' code))
    // '/*FIELDPROPS*/' changed to properties definition, do not delete next line!  /*DROP_LINE*/
    /*FIELDPROPS*/
    public int FIELDNAME { get; private set; }      /*DROP_LINE*/     // syntax check/template for generated code

    public CreatePocoClassSoftWiredTemplate()
    {
      _recNo_ = int.MinValue;
      Reinitialize();
    }

    public CreatePocoClassSoftWiredTemplate(DbfRow row)
    {
      if (fieldColumns == null)
      { // First/Reinitialized use of this class (WARNING! DbfRow.IColumn[] definition must same in all call or use Reinitialize() smartly.
        string[] names = Enum.GetNames(typeof(FieldNames));
        fieldColumns = new IColumn[names.Length];

        for (int i = 0; i < names.Length; i++)
        {
          fieldColumns[i] = row.FindColumnByName(names[i], partiallyFilledEnable);      // if ! partiallyFilledEnable and not found a field, it throw an exeption
        }
      }

      //

      _recNo_ = row.recNo;

      IColumn column;

      // assign properties with values of fields of DBF table
      // '/*FIELDASSIGNS*/' changed to makes target text, do not delete next line!      /*DROP_LINE*/
      /*FIELDASSIGNS*/
      column = fieldColumns[(int)FieldNames.FIELDNAME];                           /*DROP_LINE*/     // syntax check/template for generated code
      if ((column != null) && ! row.IsNull(column))                               /*DROP_LINE*/     // syntax check/template for generated code
      {                                                                           /*DROP_LINE*/     // syntax check/template for generated code
        FIELDNAME = row.GetInt32(column);                                         /*DROP_LINE*/     // syntax check/template for generated code
      }                                                                           /*DROP_LINE*/     // syntax check/template for generated code
    }

    public static void Reinitialize(bool newPartiallyFilledEnable = false)
    {
      fieldColumns          = null;
      partiallyFilledEnable = newPartiallyFilledEnable;
    }
  }
}
