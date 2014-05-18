using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  public interface IIndexFile
  {
    bool    eof   { get; }
    DbfRow  row   { get; }

    bool    Next(int step = 1);
    bool    Prev(int step = 1);
    
    bool    Top();
    bool    Bottom();

    bool    Seek(params Object[] keys);
    bool    SoftSeek(params Object[] keys);

    //

    string  KeyExpression {get;}

    bool    IsStreamValid(bool throwException = false);
  }
}
