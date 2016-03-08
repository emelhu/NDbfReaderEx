﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  internal class IndexPageCache
  {
    private UInt32 _pageCount = 0;
    private Dictionary<int, IndexPageCacheItem> items = null;

    public const uint MAXCACHESIZE = 100000;

    public UInt32 pageCount 
    {
      get { return _pageCount; }
    
      set
      {
        Resize(value);
      }
    }

    private void Resize(UInt32 newPageCount)
    {
      if (newPageCount > MAXCACHESIZE)
      {
        newPageCount = MAXCACHESIZE;
      }

      if (items.Count > newPageCount)
      { // Shrink size, remove oldest/not-used items
        RemovePages(items.Count - (int)newPageCount);
      }

      _pageCount = newPageCount;      
    }

    private void RemovePages(int count)
    {
      var pageNoList = 
        from item in items.Values 
        orderby item
        select item.indexPageNo;

      foreach (var pageNo in pageNoList)
      {
        if (count < 1)
        {
          break;
        }

        items.Remove(pageNo);
      }
    }

    public void Clear()
    {
      items.Clear();
    }

    public IndexPageCache(UInt32 pageCount)
    {
      _pageCount = pageCount;

      items = new Dictionary<int,IndexPageCacheItem>();
    }

    public object GetPage(int pageNo)
    {
      try
      {
        IndexPageCacheItem item = items[pageNo];

        return item.indexPageData;                                // auto: item.reads++; item.readed = DateTime.Now;
      }
      catch  
      {
        //
      }
      
      return null;
    }

    public void SetPage(int pageNo, object pageInfo)
    {
      IndexPageCacheItem item = new IndexPageCacheItem(pageNo, pageInfo);

      items[pageNo] = item;
    }
  }

  //*******************************************************************************************************

  internal class IndexPageCacheItem : IComparable, IComparable<IndexPageCacheItem>

  {
    private static long createCounter = 0;
    private static long readCounter   = 0;

    private object  _indexPageData;

    public int      indexPageNo   { get; private set;}

    public DateTime created       { get; private set;}
    public DateTime readed        { get; private set;}
    public long     reads         { get; private set;}
    public long     readCount     { get; private set;}
    public long     createCount   { get; private set;}

    public object   indexPageData 
    { 
      get          { reads++; readed = DateTime.Now; return _indexPageData;} 
      private set  { _indexPageData = value; }
    }

    public IndexPageCacheItem (int pageNo, object data)
    {
      Debug.Assert(pageNo > 0);

      indexPageNo = pageNo;
      
      created     = DateTime.Now;     
      readed      = DateTime.MinValue;      
      reads       = 0;     
      readCount   = readCounter; 
      createCount = createCounter;
 
      indexPageData = data;
    }

    #region IComparable Members

    public int CompareTo(object obj)
    {
      if (obj == null)
      {
        throw new ArgumentException("Object is not a IndexPageCacheItem");
      }

      IndexPageCacheItem ipci = obj as IndexPageCacheItem;
      if (ipci != null)
      {
        return CompareTo(ipci);
      }
      else
      {
        throw new ArgumentException("Object is not a IndexPageCacheItem");
      }
    }

    public int CompareTo(IndexPageCacheItem other)
    {
      int comp = this.reads.CompareTo(other.reads);

      if (comp == 0)
      {
        comp = this.readed.CompareTo(other.readed);
      }
        
      return comp;
    }

    #endregion
  }
}
