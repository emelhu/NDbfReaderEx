***************************************************************************************************

https://github.com/emelhu/NDbfReaderEx

***************************************************************************************************

This code forked from https://github.com/eXavera/NDbfReader

Original code by Stanislav Fajfr ( eXavera )

A lot of things complettely redesigned and reimplemented for extendable code 
and simpler and flexible usage since original NDbfReader.

***************************************************************************************************

If you have question or comment send me it.

English is not my native language, please send criticism and/or correction.


Laszlo Moravecz / www.emel.hu  emel@emel.hu

***************************************************************************************************

Plan: add a simple row update and insert feature

***************************************************************************************************

History:

1.1 --- read only dBaseIII and Clipper DBFs.  (DBF version 3)

Added with 1.3 version:  (Read PoCo class from DBF table, and help for create PoCo class definition)

  class CreatePocoClass                 (CreatePocoClass.cs)
  func  Reader/Read<T>()                (Reader.cs)
  func  Reader/Get<T>()                 (Reader.cs)
  func  DbfTable/GetPocoEnumerator<T>() (DbfTable.cs)

Added with 1.4 version:  (Read PoCo class from DBF table, and help for create PoCo class definition)
  class DbfTableParameters              (DbfTableParameters.cs)

  Read DBF version 3 & 4..7 too, and read DBT (memo) version 3 & 4 too.

Added with 1.4.1 version: double dbf field (type byte is 'O' in dbf header)
  WARNING! Because the value hasn't a stabderd IEEE double bit structure only positive value is valid!
  [If anyone can correct this code/situation please help me!] 

***************************************************************************************************
