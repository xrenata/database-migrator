namespace DbMigrator.CLI;

/// <summary>
/// Two-line live progress display:
///   Line 1 – overall  [████░░░] 52%  152/293  00:02:14  Errors: 2
///   Line 2 – current  dbo.tbl_orders  [███░░░░] 40%  45,230/113,000  12,450/s
/// Errors/warnings are printed above the bar; the bar floats at the bottom.
/// Safe when output is redirected (falls back to plain Console.WriteLine).
/// </summary>
internal sealed class ProgressUI : IDisposable
{
    // ── state ──────────────────────────────────────────────────────────
    private int    _total;
    private int    _done;
    private int    _errors;
    private string _currentItem  = "";
    private long   _itemRows;
    private long   _itemTotal;
    private long   _rowsPerSec;
    private readonly DateTime _start = DateTime.UtcNow;

    // cursor tracking
    private int  _progressRow = -1;
    private readonly bool _tty;

    // ── ctor ───────────────────────────────────────────────────────────
    public ProgressUI()
    {
        try   { _tty = !Console.IsOutputRedirected; }
        catch { _tty = false; }
    }

    // ── public API ─────────────────────────────────────────────────────

    /// <summary>Reserve the two progress lines and draw initial state.</summary>
    public void Start(int total)
    {
        _total = total;
        if (!_tty) return;
        try { Console.CursorVisible = false; } catch { }
        Console.WriteLine();          // blank separator above bar
        _progressRow = Console.CursorTop;
        Console.WriteLine();          // reserve line 1
        Console.WriteLine();          // reserve line 2
        Render();
    }

    /// <summary>Switch the "current" item (table being uploaded).</summary>
    public void SetCurrentTable(string name, long rowCount)
    {
        _currentItem = name;
        _itemRows    = 0;
        _itemTotal   = rowCount;
        _rowsPerSec  = 0;
        Render();
    }

    /// <summary>Update the per-table row progress.</summary>
    public void UpdateTableProgress(long rows, long rowsPerSec)
    {
        _itemRows   = rows;
        _rowsPerSec = rowsPerSec;
        Render();
    }

    /// <summary>Mark the current table as finished and advance overall counter.</summary>
    public void CompleteTable(bool success)
    {
        _done++;
        if (!success) _errors++;
        _currentItem = "";
        Render();
    }

    /// <summary>
    /// Print a line ABOVE the progress bar (errors, warnings, info messages).
    /// The bar is then re-drawn below.
    /// </summary>
    public void PrintLine(string message)
    {
        if (!_tty)
        {
            Console.WriteLine(message);
            return;
        }
        try
        {
            // Erase the two progress lines
            for (int i = 0; i < 2; i++)
            {
                Console.SetCursorPosition(0, _progressRow + i);
                Console.Write(new string(' ', SafeWidth() - 1));
            }
            // Print the message at the old progress start row
            Console.SetCursorPosition(0, _progressRow);
            Console.WriteLine(message);
            // Reserve new progress lines below the message
            _progressRow = Console.CursorTop;
            Console.WriteLine();
            Console.WriteLine();
        }
        catch
        {
            Console.WriteLine(message);
        }
        Render();
    }

    /// <summary>Restore the cursor after the progress area.</summary>
    public void Stop()
    {
        if (!_tty) return;
        try
        {
            if (_progressRow >= 0)
                Console.SetCursorPosition(0, _progressRow + 2);
            Console.CursorVisible = true;
        }
        catch { }
    }

    public void Dispose() => Stop();

    // ── rendering ─────────────────────────────────────────────────────

    private void Render()
    {
        if (!_tty || _progressRow < 0) return;

        var elapsed  = DateTime.UtcNow - _start;
        var barWidth = Math.Clamp(SafeWidth() - 52, 12, 38);

        try
        {
            // ── Line 1: Overall ─────────────────────────────────────────
            Console.SetCursorPosition(0, _progressRow);
            var overallPct = _total > 0 ? (double)_done / _total : 0;
            var errPart    = _errors > 0 ? $"  Errors: {_errors}" : "";
            var line1 = $" Overall  {Bar(overallPct, barWidth)} {overallPct * 100,3:F0}%  {_done}/{_total}  {elapsed:hh\\:mm\\:ss}{errPart}";
            Console.Write(Fit(line1));

            // ── Line 2: Current table ────────────────────────────────────
            Console.SetCursorPosition(0, _progressRow + 1);
            string line2;
            if (string.IsNullOrEmpty(_currentItem))
            {
                line2 = " Current  —";
            }
            else
            {
                var tblPct = _itemTotal > 0 ? (double)_itemRows / _itemTotal : 0;
                line2 = $" Current  {_currentItem}  {Bar(tblPct, barWidth)} {tblPct * 100,3:F0}%  {_itemRows:N0}/{_itemTotal:N0}  {_rowsPerSec:N0}/s";
            }
            Console.Write(Fit(line2));
        }
        catch { /* terminal too narrow or redirected mid-run */ }
    }

    // ── helpers ───────────────────────────────────────────────────────

    private static string Bar(double pct, int width)
    {
        pct = Math.Clamp(pct, 0, 1);
        var filled = (int)(pct * width);
        return "[" + new string('█', filled) + new string('░', width - filled) + "]";
    }

    private string Fit(string s)
    {
        var w = SafeWidth() - 1;
        if (w <= 0) w = 79;
        return s.Length >= w ? s[..w] : s + new string(' ', w - s.Length);
    }

    private static int SafeWidth()
    {
        try   { return Console.WindowWidth; }
        catch { return 80; }
    }
}
