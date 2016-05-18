using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

// template source is CreatePocoClassHardWiredTemplate.cs
// If any line contains /*DROP_LINE*/ text, it will not copy to target

namespace NDbfReaderEx.Template
{
  public class CreatePocoClassHardWiredTemplate
  { 
    public const CreatePocoClass.PocoType type = CreatePocoClass.PocoType.HardWired;

    // enum definition for fields of DBF table
    //! '/*FIELDNAMES*/' changed to properties definition, do not delete next line!
    private enum  FieldNames 
                  { 
                    FIELDNAME         /*DROP_LINE*/     // syntax check/template for generated code                      
                    /*FIELDNAMES*/ 
                  };
    
    [CLSCompliant(false)]  
    public int _recNo_ { get; private set; }

    // properties for fields of DBF table
    //! '/*FIELDPROPS*/' changed to properties definition, do not delete next line!
    /*FIELDPROPS*/
    public int FIELDNAME { get; private set; }      /*DROP_LINE*/     // syntax check/template for generated code

    public CreatePocoClassHardWiredTemplate()
    {
      _recNo_ = int.MinValue;
    }

    public CreatePocoClassHardWiredTemplate(DbfRow row)
    {
      _recNo_ = row.recNo;

      // assign properties with values of fields of DBF table
      // '/*FIELDASSIGNS*/' changed to makes target text, do not delete next line!      /*DROP_LINE*/
      /*FIELDASSIGNS*/
      FIELDNAME = row.GetInt32(row.columns[(int)FieldNames.FIELDNAME]);               /*DROP_LINE*/     // syntax check/template for generated code                
    }
  }
}
