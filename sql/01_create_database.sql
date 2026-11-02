/* =====================================================================
   01 - Database
   Creates the serving database for the Accounts Receivable Gold layer.
   Run against: master
   ===================================================================== */

IF DB_ID(N'ARAnalytics') IS NULL
BEGIN
    CREATE DATABASE ARAnalytics;
END
GO
