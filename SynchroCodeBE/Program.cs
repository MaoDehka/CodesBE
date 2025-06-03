using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace SynchroCodeBE
{
    internal static class Program
    {
        /// <summary>
        /// Point d'entrée principal de l'application.
        /// </summary>
        static void Main(string[] args)
        {
            if (args.Length > 0)
            {
                switch (args[0].ToLower())
                {
                    case "/install":
                        InstallService();
                        break;
                    case "/uninstall":
                        UninstallService();
                        break;
                    case "/console":
                        RunAsConsole();
                        break;
                    default:
                        Console.WriteLine("Usage: SynchroCodeBE.exe [/install|/uninstall|/console]");
                        break;
                }
            }
            else
            {
                ServiceBase.Run(new SyncService());
            }
        }

        private static void InstallService()
        {
            try
            {
                System.Configuration.Install.ManagedInstallerClass.InstallHelper(
                    new string[] { System.Reflection.Assembly.GetExecutingAssembly().Location });
                Console.WriteLine("Service installé avec succès.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erreur lors de l'installation: {ex.Message}");
            }
        }

        private static void UninstallService()
        {
            try
            {
                System.Configuration.Install.ManagedInstallerClass.InstallHelper(
                    new string[] { "/u", System.Reflection.Assembly.GetExecutingAssembly().Location });
                Console.WriteLine("Service désinstallé avec succès.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erreur lors de la désinstallation: {ex.Message}");
            }
        }

        private static void RunAsConsole()
        {
            Console.WriteLine("Mode console - Appuyez sur une touche pour arrêter...");

            var syncManager = new SyncManager();
            var timer = new System.Timers.Timer(30000);

            timer.Elapsed += (sender, e) =>
            {
                try
                {
                    Console.WriteLine($"{DateTime.Now}: Synchronisation en cours...");
                    syncManager.PerformSync();
                    Console.WriteLine($"{DateTime.Now}: Synchronisation terminée.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{DateTime.Now}: Erreur - {ex.Message}");
                }
            };

            timer.Start();
            Console.ReadKey();
            timer.Stop();
            syncManager.Dispose();
        }
    }
}
