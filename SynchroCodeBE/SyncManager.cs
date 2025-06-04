using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SynchroCodeBE
{
    public class SyncManager : IDisposable
    {
        private readonly string sqlServerConnectionString;
        private readonly string accessConnectionString;
        private bool disposed = false;

        public Action<string> LogAction { get; set; }

        private void LogInfo(string message)
        {
            LogAction?.Invoke($"INFO: {message}");
        }

        private void LogError(string message)
        {
            LogAction?.Invoke($"ERREUR: {message}");
        }

        public SyncManager()
        {
            try
            {
                // Vérifier que les chaînes de connexion existent
                var sqlServerConfig = ConfigurationManager.ConnectionStrings["SqlServer"];
                var accessConfig = ConfigurationManager.ConnectionStrings["Access"];

                if (sqlServerConfig == null)
                    throw new InvalidOperationException("Chaîne de connexion 'SqlServer' non trouvée dans App.config");

                if (accessConfig == null)
                    throw new InvalidOperationException("Chaîne de connexion 'Access' non trouvée dans App.config");

                sqlServerConnectionString = sqlServerConfig.ConnectionString;
                accessConnectionString = accessConfig.ConnectionString;

                // Log pour diagnostic
                System.Diagnostics.EventLog.WriteEntry("SynchroCodeBE",
                    $"Constructeur SyncManager - SQL: {!string.IsNullOrEmpty(sqlServerConnectionString)}, Access: {!string.IsNullOrEmpty(accessConnectionString)}",
                    System.Diagnostics.EventLogEntryType.Information);
            }
            catch (Exception ex)
            {
                System.Diagnostics.EventLog.WriteEntry("SynchroCodeBE",
                    $"Erreur constructeur SyncManager: {ex.Message}",
                    System.Diagnostics.EventLogEntryType.Error);
                throw;
            }
        }

        public void PerformSync()
        {
            try
            {
                LogInfo("Début PerformSync");

                using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
                {
                    sqlConnection.Open();
                    LogInfo("Connexion SQL ouverte pour synchronisation");

                    // Synchroniser SQL Server vers Access
                    LogInfo("=== Début sync SQL Server → Access ===");
                    SyncSqlServerToAccess(sqlConnection);
                    LogInfo("=== Fin sync SQL Server → Access ===");

                    // Synchroniser Access vers SQL Server
                    LogInfo("=== Début sync Access → SQL Server ===");
                    SyncAccessToSqlServer(sqlConnection);
                    LogInfo("=== Fin sync Access → SQL Server ===");
                }

                LogInfo("Fin PerformSync - Succès");
            }
            catch (Exception ex)
            {
                LogError($"Erreur PerformSync: {ex.Message}");
                LogError($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        private void SyncSqlServerToAccess(SqlConnection sqlConnection)
        {
            const string query = @"
        SELECT ID, TableName, Operation, KeyValues, NewValues, OldValues 
        FROM SyncLog 
        WHERE Synchronized = 0 
        AND CreatedBy NOT LIKE 'ACCESS_%'
        AND CreatedBy != 'WINDOWS_SERVICE'
        ORDER BY DateModification";

            using (var command = new SqlCommand(query, sqlConnection))
            using (var reader = command.ExecuteReader())
            {
                var logsToProcess = new List<SyncLogEntry>();

                while (reader.Read())
                {
                    logsToProcess.Add(new SyncLogEntry
                    {
                        ID = (int)reader["ID"],
                        TableName = (string)reader["TableName"],
                        Operation = (string)reader["Operation"],
                        KeyValues = (string)reader["KeyValues"],
                        NewValues = reader["NewValues"] == DBNull.Value ? null : (string)reader["NewValues"],
                        OldValues = reader["OldValues"] == DBNull.Value ? null : (string)reader["OldValues"]
                    });
                }

                reader.Close();

                LogInfo($"Trouvé {logsToProcess.Count} enregistrements SQL Server à synchroniser vers Access");

                // Traiter chaque entrée de log
                foreach (var logEntry in logsToProcess)
                {
                    try
                    {
                        LogInfo($"Traitement {logEntry.Operation} sur {logEntry.TableName} (ID: {logEntry.ID})");
                        ApplyChangeToAccess(logEntry);
                        MarkAsSynchronized(sqlConnection, logEntry.ID, true);
                        LogInfo($"Succès synchronisation ID {logEntry.ID}");
                    }
                    catch (Exception ex)
                    {
                        LogError($"Erreur synchronisation ID {logEntry.ID}: {ex.Message}");
                        MarkAsSynchronized(sqlConnection, logEntry.ID, false, ex.Message);
                    }
                }
            }
        }

        private void SyncAccessToSqlServer(SqlConnection sqlConnection)
        {
            const string query = @"
        SELECT ID, TableName, Operation, KeyValues, NewValues, OldValues 
        FROM SyncLog 
        WHERE Synchronized = 0 
        AND CreatedBy LIKE 'ACCESS_%'
        ORDER BY DateModification";

            using (var command = new SqlCommand(query, sqlConnection))
            using (var reader = command.ExecuteReader())
            {
                var logsToProcess = new List<SyncLogEntry>();

                while (reader.Read())
                {
                    logsToProcess.Add(new SyncLogEntry
                    {
                        ID = (int)reader["ID"],
                        TableName = (string)reader["TableName"],
                        Operation = (string)reader["Operation"],
                        KeyValues = (string)reader["KeyValues"],
                        NewValues = reader["NewValues"] == DBNull.Value ? null : (string)reader["NewValues"],
                        OldValues = reader["OldValues"] == DBNull.Value ? null : (string)reader["OldValues"]
                    });
                }

                reader.Close();

                LogInfo($"Trouvé {logsToProcess.Count} enregistrements Access à synchroniser vers SQL Server");

                // Traiter chaque entrée de log
                foreach (var logEntry in logsToProcess)
                {
                    try
                    {
                        LogInfo($"Traitement {logEntry.Operation} sur {logEntry.TableName} (ID: {logEntry.ID})");
                        ApplyChangeToSqlServer(sqlConnection, logEntry);
                        MarkAsSynchronized(sqlConnection, logEntry.ID, true);
                        LogInfo($"Succès synchronisation ID {logEntry.ID}");
                    }
                    catch (Exception ex)
                    {
                        LogError($"Erreur synchronisation ID {logEntry.ID}: {ex.Message}");
                        MarkAsSynchronized(sqlConnection, logEntry.ID, false, ex.Message);
                    }
                }
            }
        }

        private void ApplyChangeToAccess(SyncLogEntry logEntry)
        {
            using (var accessConnection = new OleDbConnection(accessConnectionString))
            {
                accessConnection.Open();

                switch (logEntry.TableName)
                {
                    case "BloDemande":
                        ApplyBloDemandChangeToAccess(accessConnection, logEntry);
                        break;
                    case "BloModificationsFM":
                        ApplyBloModificationsFMChangeToAccess(accessConnection, logEntry);
                        break;
                }
            }
        }

        private void ApplyChangeToSqlServer(SqlConnection sqlConnection, SyncLogEntry logEntry)
        {
            try
            {
                DisableTriggers(sqlConnection, GetTargetTable(logEntry.TableName));

                switch (logEntry.TableName)
                {
                    case "Produits":
                        ApplyProduitsChangeToSqlServer(sqlConnection, logEntry);
                        break;
                    case "Modifications":
                        ApplyModificationsChangeToSqlServer(sqlConnection, logEntry);
                        break;
                }
            }
            finally
            {
                EnableTriggers(sqlConnection, GetTargetTable(logEntry.TableName));
            }
        }

        private string GetTargetTable(string sourceTable)
        {
            switch (sourceTable)
            {
                case "Produits": return "BloDemande";
                case "Modifications": return "BloModificationsFM";
                default: return sourceTable;
            }
        }

        private void DisableTriggers(SqlConnection connection, string tableName)
        {
            try
            {
                var sql = $"ALTER TABLE {tableName} DISABLE TRIGGER ALL";
                using (var cmd = new SqlCommand(sql, connection))
                {
                    cmd.ExecuteNonQuery();
                    LogInfo($"Triggers désactivés sur {tableName}");
                }
            }
            catch (Exception ex)
            {
                LogError($"Impossible de désactiver les triggers sur {tableName}: {ex.Message}");
                // Ne pas arrêter le processus - continuer
            }
        }

        private void EnableTriggers(SqlConnection connection, string tableName)
        {
            try
            {
                var sql = $"ALTER TABLE {tableName} ENABLE TRIGGER ALL";
                using (var cmd = new SqlCommand(sql, connection))
                {
                    cmd.ExecuteNonQuery();
                    LogInfo($"Triggers réactivés sur {tableName}");
                }
            }
            catch (Exception ex)
            {
                LogError($"CRITIQUE: Impossible de réactiver les triggers sur {tableName}: {ex.Message}");
            }
        }

        private void ApplyBloDemandChangeToAccess(OleDbConnection connection, SyncLogEntry logEntry)
        {
            var keyData = JsonConvert.DeserializeObject<Dictionary<string, object>>(logEntry.KeyValues);

            string atelier = keyData["Atelier"].ToString();
            DateTime dateDemande = DateTime.Parse(keyData["DateDemande"].ToString());
            string refBE = keyData["RefBE"].ToString();

            switch (logEntry.Operation)
            {
                case "INSERT":
                    InsertIntoProduitsFromSqlServer(connection, logEntry.NewValues, atelier, dateDemande, refBE);
                    break;
                case "UPDATE":
                    UpdateProduitsFromSqlServer(connection, logEntry.NewValues, atelier, dateDemande, refBE);
                    break;
                case "DELETE":
                    DeleteFromProduits(connection, atelier, dateDemande, refBE);
                    break;
            }
        }

        private void ApplyBloModificationsFMChangeToAccess(OleDbConnection connection, SyncLogEntry logEntry)
        {
            var keyData = JsonConvert.DeserializeObject<Dictionary<string, object>>(logEntry.KeyValues);

            string codeBE = keyData["CodeBE"].ToString();
            DateTime dateSaisie = DateTime.Parse(keyData["DateSaisie"].ToString());

            switch (logEntry.Operation)
            {
                case "INSERT":
                    InsertIntoModificationsFromSqlServer(connection, logEntry.NewValues, codeBE, dateSaisie);
                    break;
                case "UPDATE":
                    UpdateModificationsFromSqlServer(connection, logEntry.NewValues, codeBE, dateSaisie);
                    break;
                case "DELETE":
                    DeleteFromModifications(connection, codeBE, dateSaisie);
                    break;
            }
        }

        private void ApplyProduitsChangeToSqlServer(SqlConnection connection, SyncLogEntry logEntry)
        {
            var keyData = JsonConvert.DeserializeObject<Dictionary<string, object>>(logEntry.KeyValues);

            string atelier = keyData["Atelier"].ToString();
            DateTime dateDemande = DateTime.Parse(keyData["DateDemande"].ToString());
            string refBE = keyData["RefBE"].ToString();

            switch (logEntry.Operation)
            {
                case "INSERT":
                    InsertIntoBloDemandeFromAccess(connection, logEntry.NewValues, atelier, dateDemande, refBE);
                    break;
                case "UPDATE":
                    UpdateBloDemandeFromAccess(connection, logEntry.NewValues, atelier, dateDemande, refBE);
                    break;
                case "DELETE":
                    DeleteFromBloDemande(connection, atelier, dateDemande, refBE);
                    break;
            }
        }

        private void ApplyModificationsChangeToSqlServer(SqlConnection connection, SyncLogEntry logEntry)
        {
            var keyData = JsonConvert.DeserializeObject<Dictionary<string, object>>(logEntry.KeyValues);

            // CORRECTION 1: Gestion robuste des clés
            string codeBE = GetKeyValue(keyData, new[] { "Code_Be", "CodeBE" });
            DateTime dateSaisie = GetKeyDateValue(keyData, new[] { "Dat_Sai", "DateSaisie" });

            LogInfo($"Traitement Modifications: CodeBE='{codeBE}', DateSaisie='{dateSaisie:yyyy-MM-dd HH:mm:ss}'");

            switch (logEntry.Operation)
            {
                case "INSERT":
                    InsertIntoBloModificationsFMFromAccess(connection, logEntry.NewValues, codeBE, dateSaisie);
                    break;
                case "UPDATE":
                    UpdateBloModificationsFMFromAccess(connection, logEntry.NewValues, codeBE, dateSaisie);
                    break;
                case "DELETE":
                    DeleteFromBloModificationsFM(connection, codeBE, dateSaisie);
                    break;
            }
        }

        // =============================================================================
        // MÉTHODES D'INSERTION ET MISE À JOUR - PRODUITS
        // =============================================================================

        private void InsertIntoProduitsFromSqlServer(OleDbConnection connection, string jsonValues, string atelier, DateTime dateDemande, string refBE)
        {
            var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonValues);

            const string sql = @"
        INSERT INTO Produits ([Atelier], [Date de la demande], [ref BE], 
        [Origine de la modification], [type d'erreur], [Commentaire], 
        [Date de mise à jour], [Réponse FAB/BE], [Refus]) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

            using (var command = new OleDbCommand(sql, connection))
            {
                // CORRECTION: Spécifier explicitement les types OleDb
                command.Parameters.Add("@atelier", OleDbType.VarChar, 50).Value = atelier ?? "";
                command.Parameters.Add("@dateDemande", OleDbType.Date).Value = dateDemande;
                command.Parameters.Add("@refBE", OleDbType.VarChar, 50).Value = refBE ?? "";
                command.Parameters.Add("@origineModif", OleDbType.VarChar, 255).Value = GetStringValue(data, "OrigineModif") ?? "";
                command.Parameters.Add("@typeErreur", OleDbType.VarChar, 255).Value = GetStringValue(data, "TypeErreur") ?? "";
                command.Parameters.Add("@commentaire", OleDbType.VarChar, 255).Value = GetStringValue(data, "Commentaire") ?? "";

                // Gestion spéciale des dates NULL
                var dateModif = GetDateValue(data, "DateModif");
                if (dateModif.HasValue)
                    command.Parameters.Add("@dateModif", OleDbType.Date).Value = dateModif.Value;
                else
                    command.Parameters.Add("@dateModif", OleDbType.Date).Value = DBNull.Value;

                // CORRECTION: Réponse ne peut pas être vide - utiliser NULL si pas de valeur
                var reponseValue = GetStringValue(data, "Reponse");
                if (string.IsNullOrEmpty(reponseValue))
                    command.Parameters.Add("@reponse", OleDbType.VarChar, 255).Value = DBNull.Value;
                else
                    command.Parameters.Add("@reponse", OleDbType.VarChar, 255).Value = reponseValue;

                // CORRECTION: Conversion boolean explicite
                bool refusValue = GetIntValue(data, "Statut") == 5;
                command.Parameters.Add("@refus", OleDbType.Boolean).Value = refusValue;

                LogInfo($"INSERT Access: Atelier={atelier}, Date={dateDemande:yyyy-MM-dd}, RefBE={refBE}, Refus={refusValue}");
                command.ExecuteNonQuery();
            }
        }

        // AUSSI corriger UpdateProduitsFromSqlServer
        private void UpdateProduitsFromSqlServer(OleDbConnection connection, string jsonValues, string atelier, DateTime dateDemande, string refBE)
        {
            var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonValues);

            const string sql = @"
        UPDATE Produits SET 
        [Origine de la modification] = ?, [type d'erreur] = ?, [Commentaire] = ?, 
        [Date de mise à jour] = ?, [Réponse FAB/BE] = ?, [Refus] = ?
        WHERE [Atelier] = ? AND [Date de la demande] = ? AND [ref BE] = ?";

            using (var command = new OleDbCommand(sql, connection))
            {
                // Paramètres SET
                command.Parameters.Add("@origineModif", OleDbType.VarChar, 255).Value = GetStringValue(data, "OrigineModif") ?? "";
                command.Parameters.Add("@typeErreur", OleDbType.VarChar, 255).Value = GetStringValue(data, "TypeErreur") ?? "";
                command.Parameters.Add("@commentaire", OleDbType.VarChar, 255).Value = GetStringValue(data, "Commentaire") ?? "";

                var dateModif = GetDateValue(data, "DateModif");
                if (dateModif.HasValue)
                    command.Parameters.Add("@dateModif", OleDbType.Date).Value = dateModif.Value;
                else
                    command.Parameters.Add("@dateModif", OleDbType.Date).Value = DBNull.Value;

                // CORRECTION: Réponse ne peut pas être vide - utiliser NULL si pas de valeur
                var reponseValue = GetStringValue(data, "Reponse");
                if (string.IsNullOrEmpty(reponseValue))
                    command.Parameters.Add("@reponse", OleDbType.VarChar, 255).Value = DBNull.Value;
                else
                    command.Parameters.Add("@reponse", OleDbType.VarChar, 255).Value = reponseValue;
                command.Parameters.Add("@refus", OleDbType.Boolean).Value = GetIntValue(data, "Statut") == 5;

                // Paramètres WHERE
                command.Parameters.Add("@atelierWhere", OleDbType.VarChar, 50).Value = atelier ?? "";
                command.Parameters.Add("@dateDemandeWhere", OleDbType.Date).Value = dateDemande;
                command.Parameters.Add("@refBEWhere", OleDbType.VarChar, 50).Value = refBE ?? "";

                LogInfo($"UPDATE Access WHERE Atelier={atelier}, Date={dateDemande:yyyy-MM-dd}, RefBE={refBE}");
                command.ExecuteNonQuery();
            }
        }

        private void InsertIntoBloDemandeFromAccess(SqlConnection connection, string jsonValues, string atelier, DateTime dateDemande, string refBE)
        {
            var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonValues);

            const string sql = @"
                INSERT INTO BloDemande (Atelier, DateDemande, RefBE, OrigineModif, TypeErreur, 
                Commentaire, DateModif, Reponse, Statut) 
                VALUES (@atelier, @dateDemande, @refBE, @origineModif, @typeErreur, 
                @commentaire, @dateModif, @reponse, @statut)";

            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.AddWithValue("@atelier", atelier);
                command.Parameters.AddWithValue("@dateDemande", dateDemande);
                command.Parameters.AddWithValue("@refBE", refBE);
                command.Parameters.AddWithValue("@origineModif", GetStringValue(data, "OrigineModif") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@typeErreur", GetStringValue(data, "TypeErreur") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@commentaire", GetStringValue(data, "Commentaire") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateModif", GetDateValue(data, "DateModif") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@reponse", GetStringValue(data, "Reponse") ?? (object)DBNull.Value);
                // Conversion Refus -> Statut : Refus=True -> Statut=5, sinon Statut=1 par défaut
                command.Parameters.AddWithValue("@statut", GetBoolValue(data, "Refus") ? 5 : 1);

                command.ExecuteNonQuery();
            }
        }

        private void UpdateBloDemandeFromAccess(SqlConnection connection, string jsonValues, string atelier, DateTime dateDemande, string refBE)
        {
            var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonValues);

            const string sql = @"
                UPDATE BloDemande SET 
                OrigineModif = @origineModif, TypeErreur = @typeErreur, Commentaire = @commentaire, 
                DateModif = @dateModif, Reponse = @reponse, Statut = @statut
                WHERE Atelier = @atelier AND DateDemande = @dateDemande AND RefBE = @refBE";

            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.AddWithValue("@origineModif", GetStringValue(data, "OrigineModif") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@typeErreur", GetStringValue(data, "TypeErreur") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@commentaire", GetStringValue(data, "Commentaire") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateModif", GetDateValue(data, "DateModif") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@reponse", GetStringValue(data, "Reponse") ?? (object)DBNull.Value);
                // Conversion Refus -> Statut
                command.Parameters.AddWithValue("@statut", GetBoolValue(data, "Refus") ? 5 : 1);
                command.Parameters.AddWithValue("@atelier", atelier);
                command.Parameters.AddWithValue("@dateDemande", dateDemande);
                command.Parameters.AddWithValue("@refBE", refBE);

                command.ExecuteNonQuery();
            }
        }

        private void DeleteFromProduits(OleDbConnection connection, string atelier, DateTime dateDemande, string refBE)
        {
            const string sql = "DELETE FROM Produits WHERE [Atelier] = ? AND [Date de la demande] = ? AND [ref BE] = ?";

            using (var command = new OleDbCommand(sql, connection))
            {
                command.Parameters.Add("@atelier", OleDbType.VarChar, 50).Value = atelier ?? "";
                command.Parameters.Add("@dateDemande", OleDbType.Date).Value = dateDemande;
                command.Parameters.Add("@refBE", OleDbType.VarChar, 50).Value = refBE ?? "";

                LogInfo($"DELETE Access WHERE Atelier={atelier}, Date={dateDemande:yyyy-MM-dd}, RefBE={refBE}");
                command.ExecuteNonQuery();
            }
        }

        private void DeleteFromBloDemande(SqlConnection connection, string atelier, DateTime dateDemande, string refBE)
        {
            const string sql = "DELETE FROM BloDemande WHERE Atelier = @atelier AND DateDemande = @dateDemande AND RefBE = @refBE";

            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.AddWithValue("@atelier", atelier);
                command.Parameters.AddWithValue("@dateDemande", dateDemande);
                command.Parameters.AddWithValue("@refBE", refBE);
                command.ExecuteNonQuery();
            }
        }

        // =============================================================================
        // MÉTHODES D'INSERTION ET MISE À JOUR - MODIFICATIONS
        // =============================================================================

        private void InsertIntoModificationsFromSqlServer(OleDbConnection connection, string jsonValues, string codeBE, DateTime dateSaisie)
        {
            var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonValues);

            const string sql = @"
        INSERT INTO Modifications ([CodeBE], [Saisie], [DesMod], [FaiQui], [FaiDat], 
        [TolOui], [TolQui], [TolDat], [CodeBEOui], [CodeBEQui], [CodeBEDat]) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            using (var command = new OleDbCommand(sql, connection))
            {
                // CORRECTION: Paramètres POSITIONNELS pour OleDB (pas de noms)
                command.Parameters.Add("p1", OleDbType.VarChar, 50).Value = codeBE ?? "";
                command.Parameters.Add("p2", OleDbType.Date).Value = dateSaisie;
                command.Parameters.Add("p3", OleDbType.VarChar, 255).Value = GetStringValue(data, "Description") ?? "";
                command.Parameters.Add("p4", OleDbType.VarChar, 255).Value = GetStringValue(data, "Realisateur") ?? "";

                var dateRealisation = GetDateValue(data, "DateRealisation");
                command.Parameters.Add("p5", OleDbType.Date).Value = dateRealisation ?? (object)DBNull.Value;

                // TolOui/TolQui : booléen + personne
                command.Parameters.Add("p6", OleDbType.Boolean).Value = GetBoolValue(data, "ModifTole");
                command.Parameters.Add("p7", OleDbType.VarChar, 255).Value = GetStringValue(data, "RealisateurTole") ?? "";

                var dateModifTole = GetDateValue(data, "DateModifTole");
                command.Parameters.Add("p8", OleDbType.Date).Value = dateModifTole ?? (object)DBNull.Value;

                // CodeBEOui/CodeBEQui : booléen + personne
                command.Parameters.Add("p9", OleDbType.Boolean).Value = GetBoolValue(data, "ModifCodeBE");
                command.Parameters.Add("p10", OleDbType.VarChar, 255).Value = GetStringValue(data, "RealisateurCodeBE") ?? "";

                var dateModifCodeBE = GetDateValue(data, "DateModifCodeBE");
                command.Parameters.Add("p11", OleDbType.Date).Value = dateModifCodeBE ?? (object)DBNull.Value;

                LogInfo($"INSERT Modifications: CodeBE={codeBE}, Saisie={dateSaisie:yyyy-MM-dd HH:mm:ss}");
                command.ExecuteNonQuery();
            }
        }

        private void UpdateModificationsFromSqlServer(OleDbConnection connection, string jsonValues, string codeBE, DateTime dateSaisie)
        {
            var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonValues);

            const string sql = @"
        UPDATE Modifications SET 
        [DesMod] = ?, [FaiQui] = ?, [FaiDat] = ?, [TolOui] = ?, [TolQui] = ?, [TolDat] = ?, 
        [CodeBEOui] = ?, [CodeBEQui] = ?, [CodeBEDat] = ?
        WHERE [CodeBE] = ? AND [Saisie] = ?";

            using (var command = new OleDbCommand(sql, connection))
            {
                // Paramètres SET POSITIONNELS (ordre important!)
                command.Parameters.Add("p1", OleDbType.VarChar, 255).Value = GetStringValue(data, "Description") ?? "";
                command.Parameters.Add("p2", OleDbType.VarChar, 255).Value = GetStringValue(data, "Realisateur") ?? "";

                var dateRealisation = GetDateValue(data, "DateRealisation");
                command.Parameters.Add("p3", OleDbType.Date).Value = dateRealisation ?? (object)DBNull.Value;

                command.Parameters.Add("p4", OleDbType.Boolean).Value = GetBoolValue(data, "ModifTole");
                command.Parameters.Add("p5", OleDbType.VarChar, 255).Value = GetStringValue(data, "RealisateurTole") ?? "";

                var dateModifTole = GetDateValue(data, "DateModifTole");
                command.Parameters.Add("p6", OleDbType.Date).Value = dateModifTole ?? (object)DBNull.Value;

                command.Parameters.Add("p7", OleDbType.Boolean).Value = GetBoolValue(data, "ModifCodeBE");
                command.Parameters.Add("p8", OleDbType.VarChar, 255).Value = GetStringValue(data, "RealisateurCodeBE") ?? "";

                var dateModifCodeBE = GetDateValue(data, "DateModifCodeBE");
                command.Parameters.Add("p9", OleDbType.Date).Value = dateModifCodeBE ?? (object)DBNull.Value;

                // Paramètres WHERE
                command.Parameters.Add("p10", OleDbType.VarChar, 50).Value = codeBE ?? "";
                command.Parameters.Add("p11", OleDbType.Date).Value = dateSaisie;

                LogInfo($"UPDATE Modifications WHERE CodeBE={codeBE}, Saisie={dateSaisie:yyyy-MM-dd HH:mm:ss}");
                int rowsAffected = command.ExecuteNonQuery();
                LogInfo($"Lignes affectées: {rowsAffected}");
            }
        }

        private void InsertIntoBloModificationsFMFromAccess(SqlConnection connection, string jsonValues, string codeBE, DateTime dateSaisie)
        {
            var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonValues);

            const string sql = @"
        INSERT INTO BloModificationsFM (CodeBE, DateSaisie, Description, Realisateur, DateRealisation, 
        ModifTole, RealisateurTole, DateModifTole, ModifCodeBE, RealisateurCodeBE, DateModifCodeBE, CausesBlocage) 
        VALUES (@codeBE, @dateSaisie, @description, @realisateur, @dateRealisation, 
        @modifTole, @realisateurTole, @dateModifTole, @modifCodeBE, @realisateurCodeBE, @dateModifCodeBE, @causesBlocage)";

            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.AddWithValue("@codeBE", codeBE);
                command.Parameters.AddWithValue("@dateSaisie", dateSaisie);

                // CORRECTION 3: Mapping Access → SQL Server avec noms corrects
                command.Parameters.AddWithValue("@description", GetStringValueAccess(data, "Des_Mod") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@realisateur", GetStringValueAccess(data, "Fai_Qui") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateRealisation", GetDateValueAccess(data, "Fai_Dat") ?? (object)DBNull.Value);

                // ModifTole = TolOui d'Access
                command.Parameters.AddWithValue("@modifTole", GetBoolValueAccess(data, "Tol_Oui"));
                command.Parameters.AddWithValue("@realisateurTole", GetStringValueAccess(data, "Tol_Qui") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateModifTole", GetDateValueAccess(data, "Tol_Dat") ?? (object)DBNull.Value);

                // ModifCodeBE = CodeBEOui d'Access
                command.Parameters.AddWithValue("@modifCodeBE", GetBoolValueAccess(data, "CodeBE_Oui"));
                command.Parameters.AddWithValue("@realisateurCodeBE", GetStringValueAccess(data, "CodeBE_Qui") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateModifCodeBE", GetDateValueAccess(data, "CodeBE_Dat") ?? (object)DBNull.Value);

                // CausesBlocage n'existe pas dans Access
                command.Parameters.AddWithValue("@causesBlocage", DBNull.Value);

                LogInfo($"INSERT BloModificationsFM: CodeBE={codeBE}, DateSaisie={dateSaisie:yyyy-MM-dd HH:mm:ss}");
                command.ExecuteNonQuery();
            }
        }

        private void UpdateBloModificationsFMFromAccess(SqlConnection connection, string jsonValues, string codeBE, DateTime dateSaisie)
        {
            var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonValues);

            const string sql = @"
        UPDATE BloModificationsFM SET 
        Description = @description, Realisateur = @realisateur, DateRealisation = @dateRealisation,
        ModifTole = @modifTole, RealisateurTole = @realisateurTole, DateModifTole = @dateModifTole,
        ModifCodeBE = @modifCodeBE, RealisateurCodeBE = @realisateurCodeBE, DateModifCodeBE = @dateModifCodeBE
        WHERE CodeBE = @codeBE AND DateSaisie = @dateSaisie";

            using (var command = new SqlCommand(sql, connection))
            {
                // Mapping Access → SQL Server
                command.Parameters.AddWithValue("@description", GetStringValueAccess(data, "Des_Mod") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@realisateur", GetStringValueAccess(data, "Fai_Qui") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateRealisation", GetDateValueAccess(data, "Fai_Dat") ?? (object)DBNull.Value);

                command.Parameters.AddWithValue("@modifTole", GetBoolValueAccess(data, "Tol_Oui"));
                command.Parameters.AddWithValue("@realisateurTole", GetStringValueAccess(data, "Tol_Qui") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateModifTole", GetDateValueAccess(data, "Tol_Dat") ?? (object)DBNull.Value);

                command.Parameters.AddWithValue("@modifCodeBE", GetBoolValueAccess(data, "CodeBE_Oui"));
                command.Parameters.AddWithValue("@realisateurCodeBE", GetStringValueAccess(data, "CodeBE_Qui") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateModifCodeBE", GetDateValueAccess(data, "CodeBE_Dat") ?? (object)DBNull.Value);

                command.Parameters.AddWithValue("@codeBE", codeBE);
                command.Parameters.AddWithValue("@dateSaisie", dateSaisie);

                LogInfo($"UPDATE BloModificationsFM WHERE CodeBE={codeBE}, DateSaisie={dateSaisie:yyyy-MM-dd HH:mm:ss}");
                int rowsAffected = command.ExecuteNonQuery();
                LogInfo($"Lignes affectées: {rowsAffected}");
            }
        }

        private void DeleteFromModifications(OleDbConnection connection, string codeBE, DateTime dateSaisie)
        {
            const string sql = "DELETE FROM Modifications WHERE [CodeBE] = ? AND [Saisie] = ?";

            using (var command = new OleDbCommand(sql, connection))
            {
                AddStringParameter(command, "@codeBE", codeBE, 50);
                command.Parameters.Add("@saisie", OleDbType.Date).Value = dateSaisie;

                LogInfo($"DELETE Modifications WHERE CodeBE={codeBE}, Saisie={dateSaisie:yyyy-MM-dd}");
                command.ExecuteNonQuery();
            }
        }

        private void DeleteFromBloModificationsFM(SqlConnection connection, string codeBE, DateTime dateSaisie)
        {
            const string sql = "DELETE FROM BloModificationsFM WHERE CodeBE = @codeBE AND DateSaisie = @dateSaisie";

            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.AddWithValue("@codeBE", codeBE);
                command.Parameters.AddWithValue("@dateSaisie", dateSaisie);
                command.ExecuteNonQuery();
            }
        }

        // =============================================================================
        // MÉTHODES UTILITAIRES
        // =============================================================================

        private void MarkAsSynchronized(SqlConnection connection, int logID, bool success, string errorMessage = null)
        {
            const string sql = "UPDATE SyncLog SET Synchronized = @success, SyncAttempts = SyncAttempts + 1, LastSyncError = @error WHERE ID = @id";

            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.AddWithValue("@success", success);
                command.Parameters.AddWithValue("@error", errorMessage ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@id", logID);
                command.ExecuteNonQuery();
            }
        }

        // Méthodes utilitaires pour l'extraction de données JSON
        private string GetStringValue(Dictionary<string, object> data, string key)
        {
            return data.ContainsKey(key) && data[key] != null ? data[key].ToString() : null;
        }

        private DateTime? GetDateValue(Dictionary<string, object> data, string key)
        {
            if (data.ContainsKey(key) && data[key] != null)
            {
                var value = data[key].ToString();
                if (!string.IsNullOrEmpty(value) && DateTime.TryParse(value, out DateTime result))
                    return result;
            }
            return null;
        }

        private int GetIntValue(Dictionary<string, object> data, string key)
        {
            if (data.ContainsKey(key) && data[key] != null)
            {
                if (int.TryParse(data[key].ToString(), out int result))
                    return result;
            }
            return 0;
        }

        private bool GetBoolValue(Dictionary<string, object> data, string key)
        {
            if (data.ContainsKey(key) && data[key] != null)
            {
                var value = data[key].ToString().ToLower();
                return value == "true" || value == "1";
            }
            return false;
        }

        private string GetKeyValue(Dictionary<string, object> keyData, string[] possibleKeys)
        {
            foreach (string key in possibleKeys)
            {
                if (keyData.ContainsKey(key) && keyData[key] != null)
                {
                    return keyData[key].ToString();
                }
            }
            throw new InvalidOperationException($"Clé non trouvée parmi: {string.Join(", ", possibleKeys)}");
        }

        private DateTime GetKeyDateValue(Dictionary<string, object> keyData, string[] possibleKeys)
        {
            foreach (string key in possibleKeys)
            {
                if (keyData.ContainsKey(key) && keyData[key] != null)
                {
                    var value = keyData[key].ToString();
                    if (value != "null" && DateTime.TryParse(value, out DateTime result))
                    {
                        return result;
                    }
                }
            }

            // CORRECTION 4: Si pas de date valide, utiliser DateTime.Now avec seconde tronquée
            var now = DateTime.Now;
            return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second);
        }

        private string GetStringValueAccess(Dictionary<string, object> data, string key)
        {
            return data.ContainsKey(key) && data[key] != null ? data[key].ToString() : null;
        }

        private DateTime? GetDateValueAccess(Dictionary<string, object> data, string key)
        {
            if (data.ContainsKey(key) && data[key] != null)
            {
                var value = data[key].ToString();
                if (!string.IsNullOrEmpty(value) && value != "null" && DateTime.TryParse(value, out DateTime result))
                    return result;
            }
            return null;
        }

        private bool GetBoolValueAccess(Dictionary<string, object> data, string key)
        {
            if (data.ContainsKey(key) && data[key] != null)
            {
                var value = data[key].ToString().ToLower();
                return value == "true" || value == "1" || value == "yes";
            }
            return false;
        }

        private void AddStringParameter(OleDbCommand command, string paramName, string value, int maxLength)
        {
            if (string.IsNullOrEmpty(value))
                command.Parameters.Add(paramName, OleDbType.VarChar, maxLength).Value = DBNull.Value;
            else
                command.Parameters.Add(paramName, OleDbType.VarChar, maxLength).Value = value;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed && disposing)
            {
                // Cleanup managed resources
                disposed = true;
            }
        }
    }
}