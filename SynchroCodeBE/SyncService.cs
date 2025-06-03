using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace SynchroCodeBE
{
    public partial class SyncService : ServiceBase
    {
        private Timer syncTimer;
        private SyncManager syncManager;
        private readonly string logFile = @"C:\Logs\DatabaseSync.log";
        public SyncService()
        {
            InitializeComponent();
            ServiceName = "SynchroCodeBE";
        }

        protected override void OnStart(string[] args)
        {
            WriteLog("Service démarré");

            syncManager = new SyncManager();

            // Timer pour synchronisation automatique toutes les 30 secondes
            syncTimer = new Timer(30000);
            syncTimer.Elapsed += OnSyncTimer;
            syncTimer.Start();
        }

        protected override void OnStop()
        {
            WriteLog("Service arrêté");

            syncTimer?.Stop();
            syncTimer?.Dispose();
            syncManager?.Dispose();
        }

        private void OnSyncTimer(object sender, ElapsedEventArgs e)
        {
            try
            {
                syncManager.PerformSync();
            }
            catch (Exception ex)
            {
                WriteLog($"Erreur lors de la synchronisation: {ex.Message}");
            }
        }

        private void WriteLog(string message)
        {
            try
            {
                string logDir = Path.GetDirectoryName(logFile);
                if (!Directory.Exists(logDir))
                    Directory.CreateDirectory(logDir);

                File.AppendAllText(logFile, $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}\n");
            }
            catch { /* Ignore logging errors */ }
        }
    }
}
