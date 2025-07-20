USE RegFlow;
GO
ALTER PROCEDURE dbo.sp_QC_CheckTER
    @Threshold DECIMAL(5,2) = 5.00   -- bps (0.05 if you multiply later)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO QC_Log (FundID, ValDate, Delta_bp, [Rule])
    SELECT
        f.FundID,
        f.ValDate,
        f.VendorTER_bp - f.CalcTER_bp            AS Delta_bp,
        CONCAT('TER variance > ', @Threshold, ' bps') AS [Rule]
    FROM FactCosts f
    WHERE ABS(f.VendorTER_bp - f.CalcTER_bp) * 100 > @Threshold  -- *100 if using 5 bps
      AND NOT EXISTS (                                          -- skip duplicates
            SELECT 1
            FROM QC_Log q
            WHERE q.FundID = f.FundID
              AND q.ValDate = f.ValDate
              AND q.[Rule]  = CONCAT('TER variance > ', @Threshold, ' bps')
      );

    -- return rows added in this run (optional)
    SELECT *
    FROM   QC_Log
    WHERE  QCID >= SCOPE_IDENTITY() - @@ROWCOUNT + 1;
END;
GO
