using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using log4net;
using log4net.Config;
using System.IO;

namespace MSSQLCopy
{
    class Program
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(Program));

        public static void Main(string[] args)
        {

            XmlConfigurator.Configure(new System.IO.FileInfo("log4net.xml"));

            log.Info("start...");

            SqlConnection SourceConnection = new SqlConnection(ConfigurationManager.AppSettings["SourceDbConString"]);
            SqlConnection TargetConnection = new SqlConnection(ConfigurationManager.AppSettings["TargetDbConString"]);

            
            bool bRet = createBackupToDisk(SourceConnection, ConfigurationManager.AppSettings["SourceDbName"], ConfigurationManager.AppSettings["DbBackupFilePath"]);
            if (bRet)
            {
                log.Info("...start restore...");
                restoreBackupFromDisk(TargetConnection, ConfigurationManager.AppSettings["TargetDbName"], ConfigurationManager.AppSettings["DbBackupFilePath"]);
                //Delete backup file
                try
                {
                    log.Debug("Try to delete backupfile.");
                    File.Delete(ConfigurationManager.AppSettings["DbBackupFilePath"]);
                }
                catch (Exception e)
                {
                    log.Error("Delete backupfile: " + e.ToString());
                }

            }
            log.Info("...end");
            
        }

        private static void restoreBackupFromDisk(SqlConnection con, string DatabaseName, string BackupFilePath)
        {
            try
            {
                SqlCommand myCommand = new SqlCommand("RESTORE DATABASE [" + DatabaseName + "] FROM DISK = '" + BackupFilePath + "'", con);
                con.Open();
                log.Info("Start restore of database: " + DatabaseName + " from file: " + BackupFilePath);
                int ret = myCommand.ExecuteNonQuery();
                log.Info("lines: " + ret);
            }
            catch (Exception e)
            {
                log.Error(e.ToString());
            }
            finally
            {
                if (con.State != ConnectionState.Closed)
                {
                    con.Close();
                }

            }
        }

        private static bool createBackupToDisk(SqlConnection con, string DatabaseName, string BackupFilePath)
        {
            bool bRet = true;
            try
            {
                SqlCommand myCommand = new SqlCommand("BACKUP DATABASE [" + DatabaseName + "] TO DISK = '" + BackupFilePath + "'", con);
                con.Open();
                log.Info("Start restore of database: " + DatabaseName + " from file: " + BackupFilePath);
                int ret = myCommand.ExecuteNonQuery();
                log.Info("lines: " + ret);
            }
            catch (Exception e)
            {
                bRet = false;
                log.Error(e.ToString());

            }
            finally
            {
                if (con.State != ConnectionState.Closed)
                {
                    con.Close();
                }
                
            }
            return bRet;
        }
    }
}
