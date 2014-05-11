using System;
using System.Data;
using System.Globalization;
using System.Text;

namespace NDbfReaderEx
{
  /// <summary>
  /// Extensions for for the <see cref="DbfTable"/> class.
  /// </summary>
  public static class TableExtensions
  {
    /// <summary>
    /// Loads the DBF table into a <see cref="DataTable"/> with the default ASCII encoding.
    /// </summary>
    /// <param name="table">The DBF table to load.</param>
    /// <returns>A <see cref="DataTable"/> loaded from the DBF table.</returns>
    /// <exception cref="InvalidOperationException">Another reader of the DBF table is opened.</exception>
    /// <exception cref="ObjectDisposedException">The DBF table is disposed.</exception>
    public static DataTable AsDataTable(this DbfTable table)
    {
      if (table == null)
      {
        throw new ArgumentNullException("table");
      }

      var dataTable = CreateDataTable(table);

      FillData(table, dataTable);

      return dataTable;
    }


    private static DataTable CreateDataTable(DbfTable table)
    {
      var dataTable = new DataTable()
      {
        Locale = CultureInfo.CurrentCulture
      };

      foreach (var column in table.columns)
      {
        var columnType = Nullable.GetUnderlyingType(column.type) ?? column.type;
        dataTable.Columns.Add(column.name, columnType);
      }

      return dataTable;
    }

    private static void FillData(DbfTable table, DataTable dataTable)
    {
      for (int i = 0; i < table.recCount; i++)
      {
        var rowDT  = dataTable.NewRow();
        var recDBF = table.GetRow(i);

        foreach (var column in table.columns)
        {
          rowDT[column.name] = recDBF.GetValue(column) ?? DBNull.Value;
        }

        dataTable.Rows.Add(rowDT);
      }
    }
  }
}
