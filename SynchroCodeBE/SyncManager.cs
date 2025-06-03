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
            using (var sqlConnection = new SqlConnection(sqlServerConnectionString))
            {
                sqlConnection.Open();

                // Synchroniser SQL Server vers Access
                SyncSqlServerToAccess(sqlConnection);

                // Synchroniser Access vers SQL Server
                SyncAccessToSqlServer(sqlConnection);
            }
        }

        private void SyncSqlServerToAccess(SqlConnection sqlConnection)
        {
            const string query = @"
        SELECT ID, TableName, Operation, KeyValues, NewValues, OldValues 
        FROM SyncLog 
        WHERE Synchronized = 0 
        AND CreatedBy NOT LIKE 'ACCESS_%'
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

                // Traiter chaque entrée de log
                foreach (var logEntry in logsToProcess)
                {
                    try
                    {
                        ApplyChangeToAccess(logEntry);
                        MarkAsSynchronized(sqlConnection, logEntry.ID, true);
                    }
                    catch (Exception ex)
                    {
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

                // Traiter chaque entrée de log
                foreach (var logEntry in logsToProcess)
                {
                    try
                    {
                        ApplyChangeToSqlServer(sqlConnection, logEntry);
                        MarkAsSynchronized(sqlConnection, logEntry.ID, true);
                    }
                    catch (Exception ex)
                    {
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

            string codeBE = keyData["CodeBE"].ToString();
            DateTime dateSaisie = DateTime.Parse(keyData["DateSaisie"].ToString());

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
                command.Parameters.AddWithValue("@atelier", atelier);
                command.Parameters.AddWithValue("@dateDemande", dateDemande);
                command.Parameters.AddWithValue("@refBE", refBE);
                command.Parameters.AddWithValue("@origineModif", GetStringValue(data, "OrigineModif"));
                command.Parameters.AddWithValue("@typeErreur", GetStringValue(data, "TypeErreur"));
                command.Parameters.AddWithValue("@commentaire", GetStringValue(data, "Commentaire"));
                command.Parameters.AddWithValue("@dateModif", GetDateValue(data, "DateModif") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@reponse", GetStringValue(data, "Reponse"));
                // Conversion Statut -> Refus : seul le statut 5 (Refusée) = True
                command.Parameters.AddWithValue("@refus", GetIntValue(data, "Statut") == 5);

                command.ExecuteNonQuery();
            }
        }

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
                command.Parameters.AddWithValue("@origineModif", GetStringValue(data, "OrigineModif"));
                command.Parameters.AddWithValue("@typeErreur", GetStringValue(data, "TypeErreur"));
                command.Parameters.AddWithValue("@commentaire", GetStringValue(data, "Commentaire"));
                command.Parameters.AddWithValue("@dateModif", GetDateValue(data, "DateModif") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@reponse", GetStringValue(data, "Reponse"));
                // Conversion Statut -> Refus
                command.Parameters.AddWithValue("@refus", GetIntValue(data, "Statut") == 5);
                command.Parameters.AddWithValue("@atelier", atelier);
                command.Parameters.AddWithValue("@dateDemande", dateDemande);
                command.Parameters.AddWithValue("@refBE", refBE);

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
                command.Parameters.AddWithValue("@atelier", atelier);
                command.Parameters.AddWithValue("@dateDemande", dateDemande);
                command.Parameters.AddWithValue("@refBE", refBE);
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
                INSERT INTO Modifications ([CodeBE], [Saisie], [DesModif], [FaitOui], [FaitDat], 
                [TolOui], [TolDat], [CodeBEOui], [CodeBEDat]) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

            using (var command = new OleDbCommand(sql, connection))
            {
                command.Parameters.AddWithValue("@codeBE", codeBE);
                command.Parameters.AddWithValue("@dateSaisie", dateSaisie);
                command.Parameters.AddWithValue("@desModif", GetStringValue(data, "Description"));
                command.Parameters.AddWithValue("@faitOui", GetStringValue(data, "Realisateur"));
                command.Parameters.AddWithValue("@faitDat", GetDateValue(data, "DateRealisation") ?? (object)DBNull.Value);
                // TolOui prend la valeur du bit ModifTole
                command.Parameters.AddWithValue("@tolOui", GetBoolValue(data, "ModifTole"));
                command.Parameters.AddWithValue("@tolDat", GetDateValue(data, "DateModifTole") ?? (object)DBNull.Value);
                // CodeBEOui prend la valeur du bit ModifCodeBE
                command.Parameters.AddWithValue("@codeBEOui", GetBoolValue(data, "ModifCodeBE"));
                command.Parameters.AddWithValue("@codeBEDat", GetDateValue(data, "DateModifCodeBE") ?? (object)DBNull.Value);

                command.ExecuteNonQuery();
            }
            // Note: CausesBlocage n'existe pas dans Access - ignoré
        }

        private void UpdateModificationsFromSqlServer(OleDbConnection connection, string jsonValues, string codeBE, DateTime dateSaisie)
        {
            var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonValues);

            const string sql = @"
                UPDATE Modifications SET 
                [DesModif] = ?, [FaitOui] = ?, [FaitDat] = ?, [TolOui] = ?, [TolDat] = ?, 
                [CodeBEOui] = ?, [CodeBEDat] = ?
                WHERE [CodeBE] = ? AND [Saisie] = ?";

            using (var command = new OleDbCommand(sql, connection))
            {
                command.Parameters.AddWithValue("@desModif", GetStringValue(data, "Description"));
                command.Parameters.AddWithValue("@faitOui", GetStringValue(data, "Realisateur"));
                command.Parameters.AddWithValue("@faitDat", GetDateValue(data, "DateRealisation") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@tolOui", GetBoolValue(data, "ModifTole"));
                command.Parameters.AddWithValue("@tolDat", GetDateValue(data, "DateModifTole") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@codeBEOui", GetBoolValue(data, "ModifCodeBE"));
                command.Parameters.AddWithValue("@codeBEDat", GetDateValue(data, "DateModifCodeBE") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@codeBE", codeBE);
                command.Parameters.AddWithValue("@dateSaisie", dateSaisie);

                command.ExecuteNonQuery();
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
                command.Parameters.AddWithValue("@description", GetStringValue(data, "Description") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@realisateur", GetStringValue(data, "Realisateur") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateRealisation", GetDateValue(data, "DateRealisation") ?? (object)DBNull.Value);
                // ModifTole prend la valeur du bit TolOui d'Access
                command.Parameters.AddWithValue("@modifTole", GetBoolValue(data, "TolOui"));
                // RealisateurTole prend la même valeur que FaitOui (car même champ dans Access)
                command.Parameters.AddWithValue("@realisateurTole", GetStringValue(data, "FaitOui") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateModifTole", GetDateValue(data, "TolDat") ?? (object)DBNull.Value);
                // ModifCodeBE prend la valeur du bit CodeBEOui d'Access
                command.Parameters.AddWithValue("@modifCodeBE", GetBoolValue(data, "CodeBEOui"));
                // RealisateurCodeBE prend la même valeur que FaitOui (car même champ dans Access)
                command.Parameters.AddWithValue("@realisateurCodeBE", GetStringValue(data, "FaitOui") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateModifCodeBE", GetDateValue(data, "CodeBEDat") ?? (object)DBNull.Value);
                // CausesBlocage n'existe pas dans Access - mettre NULL
                command.Parameters.AddWithValue("@causesBlocage", DBNull.Value);

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
                command.Parameters.AddWithValue("@description", GetStringValue(data, "Description") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@realisateur", GetStringValue(data, "Realisateur") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateRealisation", GetDateValue(data, "DateRealisation") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@modifTole", GetBoolValue(data, "TolOui"));
                command.Parameters.AddWithValue("@realisateurTole", GetStringValue(data, "FaitOui") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateModifTole", GetDateValue(data, "TolDat") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@modifCodeBE", GetBoolValue(data, "CodeBEOui"));
                command.Parameters.AddWithValue("@realisateurCodeBE", GetStringValue(data, "FaitOui") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@dateModifCodeBE", GetDateValue(data, "CodeBEDat") ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@codeBE", codeBE);
                command.Parameters.AddWithValue("@dateSaisie", dateSaisie);

                command.ExecuteNonQuery();
            }
            // Note: CausesBlocage n'est pas mis à jour car il n'existe pas dans Access
        }

        private void DeleteFromModifications(OleDbConnection connection, string codeBE, DateTime dateSaisie)
        {
            const string sql = "DELETE FROM Modifications WHERE [CodeBE] = ? AND [Saisie] = ?";

            using (var command = new OleDbCommand(sql, connection))
            {
                command.Parameters.AddWithValue("@codeBE", codeBE);
                command.Parameters.AddWithValue("@dateSaisie", dateSaisie);
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