using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  public interface IMemoFile
  {
    string ReadMemoText(int blockNo);

    int    WriteMemoText(string memoText, int oldBlockNo = 0);    

    bool   disposed {get;}
  }
}
