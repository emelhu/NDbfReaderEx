using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  public interface IMemoFile
  {
    byte[] ReadMemoBytes(int blockNo);

    int WriteMemoBytes(byte[] newBytes, int oldBlockNo = 0);

    bool   disposed {get;}
  }
}
