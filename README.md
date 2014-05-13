NDbfReaderEx
============
```
NDbfReaderEx is a .NET library for reading dBASE (.dbf) files. 
The library is simple, extensible and without any external dependencies.
```
This code forked from https://github.com/eXavera/NDbfReader

Original code by Stanislav Fajfr ( eXavera )

```
Original code was forked because I found some fatal error when my code read dBase3/Clipper tables.
...and I want a lot of extra too :)

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
...and: A lot of things complettely redesigned and reimplemented for extendable code and simpler and flexible usage since original NDbfReader
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

###An entire table can be loaded into a `DataTable` as original eXavera/NDbfReader:

```csharp
using (var table = Table.Open("D:\\foo.dbf"))
{
  return table.AsDataTable();
}
```

Non-seekable (forward-only) streams are **NOT** supported _already_ (as original NDbfReader did it). 

## NuGet

```
Install-Package NDbfReaderEx
```

## Source

Clone the repository and run `build.cmd`. 
Openning the solution requires Visual Studio 2012 or newer (including Express editions).


## Tests & Examples

You can see the supplied source of NDbfReaderEx_Test.exe

## License

[MIT](https://github.com/emelhu/NDbfReaderEx/blob/master/LICENSE.md)

## Contact

If you have any question or comment send it to me.

Sorry, English is not my native language, please send criticism and/or correction too.

You can contact to me directly:  emel@emel.hu 

Laszlo Moravecz / [www.emel.hu](http://www.emel.hu)
