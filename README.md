NDbfReaderEx
============
```
NDbfReaderEx is a .NET library for **reading** dBASE (.dbf) files. 
The library is simple, extensible and without any external dependencies.
```
This code forked from https://github.com/eXavera/NDbfReader

_Original code by Stanislav Fajfr ( eXavera )_

```
Original code was forked because I found some fatal error when my code read dBase3/Clipper tables.
...and I wanted to use a **lot of extra features** too :)

```
New NDbfReaderEx changed to positioning inside dbf file, you can reread previously readed records too 
and can read any record by record count in datafile. You can read records direct or enumerate it.

Others can insert/modify dbf records while you read it by NDbfReaderEx; you can refresh header 
information (record count and last update date) because other program maybe wrote it 
after you opened the dbf file.

[Other program can't change column info or shrink datafile by 'zip/pack' command while you read it. 
These are dBase/Clipper methods - and need exclusive file open for do it.]

NDbfReaderEx records can work with detached mode too. 
If you have already readed necessary rows of dbf file you can close DbfTable, and you can use theese stored records after it.
 
You have got a dBase/Clipper syntax/operating mode like extension for DbfTable, called DbfTableReader.

``` 
...and: A lot of things complettely redesigned and reimplemented for extendable code 
and simpler and flexible usage since original NDbfReader.
```

## Example

### You can enumerate records of DBF table

```csharp
using (DbfTable table = DbfTable.Open(filename, Encoding.GetEncoding(437)))
{
  foreach (DbfRow row in table)
  {
    Console.WriteLine();
    Console.WriteLine("AAA: " + row.GetString("AAA"));
    Console.WriteLine("BBB: " + row.GetDecimal("BBB"));
    Console.WriteLine("CCC: " + row.GetDate("CCC")); 
    Console.WriteLine("DDD: " + row.GetBoolean("DDD")); 
    Console.WriteLine("EEE: " + row.GetString("EEE"));   // MEMO!
  }
  
  // but can read direct too
  
  DbfRow rowFirst = table.GetRow(0);                    // first record of table
  DbfRow rowLast  = table.GetRow(table.recCount - 1);   // last  record of table
}
```

###A dbf table can be readed by reader as original eXavera/NDbfReader:

```csharp
using (var table = Table.Open("D:\\foo.dbf"), Encoding.GetEncoding(1250))
{   
  var reader = table.OpenReader();

  while(reader.Read())
  {
    var name = reader.GetString("NAME");
    //...
  }
}
```

###A dbf table can be readed by reader like Clipper did it:
It good to take an old Clipper .prg source code and refactor it. 

```csharp
using (var table = Table.Open("D:\\foo.dbf"))         // without Encoding: use 'Code page mark' byte from Dbf header! Gooood! (not only English World)
{   
  ClipperReader reader = table.OpenClipperReader(true);     // skip deleted ON

  while(! reader.eof)
  {
    var name = reader.GetString("NAME");
    //...
    
    reader.Next();
  }
}
```

###An entire table can be loaded into a `DataTable` as original eXavera/NDbfReader:

```csharp
using (var table = Table.Open("D:\\foo.dbf"))
{
  return table.AsDataTable();
}
```

You can use more reader, enumerate, or direct record (by recNo) simulta even in more thread.

Non-seekable (forward-only) streams are **NOT** supported _already_ (as original NDbfReader did it). 

## PoCo class

There are a possibility to use **PoCo class** for read (after version 1.3).
You can create *.cs source file (contains PoCo class that simultaneous of dbf) from a DBF file header information and if you add this class to your project, you can fill it from dbf's row or you can enumerate dbf table and get rows in PoCo class.

_Create Poco Class from DBF table:_
```csharp
#if DEBUG -- called from application if cs file not found, create *.cs directly to target dictionary of project
internal static void CreateCS()
{    
    string dir         = Path.Combine(CreatePocoClass.GetSourceFileDirectory(), @"Data");
    var    pocoCreator = new CreatePocoClass(CreatePocoClass.PocoType.Dynamic, dir, "Namespace.Data", true);  
    string dbfName     = Path.Combine(dir, "TESTDATA.dbf");

    DbfTable dbfTable  = DbfTable.Open(dbfName, Encoding.GetEncoding(852), DbfTableType.Clipper);
    
    pocoCreator.CreateCS("DBF_TESTDATA", dbfTable.columns);
}
#endif
```

_Read DBF rows directly on the created Poco Class:_
```csharp
using (var table = DbfTable.Open(dbfTestDataFileName, Encoding.GetEncoding(852)))
{
    ...
    var rec = table.Get<DBF_TESTDATA>(recNo);
    ...   
    foreach (var rec in table.PocoEnumerator<DBF_TESTDATA>(startRecNo))
    {          
    }  
    ...
    foreach (DbfRow row in table)
    { 
        var rec = table.Get<DBF_TESTDATA>(row);
        --or--
        var rec = row.Get<DBF_TESTDATA>();
    }         
}
```

## NuGet

```
Install-Package NDbfReaderEx
```

## Source

Clone the repository and run `build.cmd`. 
Openning the solution requires Visual Studio 2012 or newer (including Express editions).


## History

Version 1.1
  Read DBF & DBT structure version 3
  
Version 1.3
  Read table data with PoCo class
  
Version 1.4  
  Read DBF (data) structure version 4..7 too
  Read DBT (memo) structure version 4 too
  
Version 1.4.1 
  You can read double typed dbf field (type byte is 'O' in dbf header)
  
  **WARNING! Because the value hasn't a stabdard IEEE double bit structure only positive value is valid!**
  
  *[If anyone can correct this code/situation please help me!]*   


## Tests & Examples

You can see the supplied source of NDbfReaderEx_Test.exe

There isn't any test program yet for PoCo class use (after version 1.3) but I have tested it inside a production app.

## License

[MIT](https://github.com/emelhu/NDbfReaderEx/blob/master/LICENSE.md)

## Contact

If you have any question or comment send it to me.

Sorry, English is not my native language, please send criticism and/or correction too.

You can contact to me directly:  emel@emel.hu 

Laszlo Moravecz / [www.emel.hu](http://www.emel.hu)
