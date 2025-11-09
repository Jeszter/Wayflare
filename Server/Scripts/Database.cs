using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.IO;
using System.Threading;
using System.Threading.Tasks;


namespace DevelopersHub.RealtimeNetworking.Server
{
    class Database

    {


        #region Main Data And Methods

        private const string _mysqlServer = "127.0.0.1";
        private const string _mysqlUsername = "root";
        private const string _mysqlPassword = "";
        private const string _mysqlDatabase = "clash";
        public static readonly string dataFolderPath = "C:\\Clash Of Whatever\\";

        public static MySqlConnection GetMysqlConnection()
        {
            MySqlConnection connection = new MySqlConnection("SERVER=" + _mysqlServer + "; DATABASE=" + _mysqlDatabase + "; UID=" + _mysqlUsername + "; PASSWORD=" + _mysqlPassword + "; POOLING=TRUE");
            connection.Open();
            return connection;
        }

        private static DateTime collectTime = DateTime.Now;
        private static bool collecting = false;

        private static DateTime updateTime = DateTime.Now;
        private static bool updating = false;

        private static bool warCheckUpdating = false;
        private static DateTime warCheckUpdateTime = DateTime.Now;

        private static DateTime warUpdateTime = DateTime.Now;

        public static void Initialize()
        {
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = String.Format("UPDATE accounts SET is_online = 0, client_id = 0;");
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
                connection.Close();
            }
        }

        public static void Update()
        {
            if (!collecting)
            {
                double deltaTime = (DateTime.Now - collectTime).TotalSeconds;
                if (deltaTime >= 5)
                {
                    collecting = true;
                    collectTime = DateTime.Now;
                    UpdateCollectabes(deltaTime);
                }
            }
            if (!updating)
            {
                double deltaTime = (DateTime.Now - updateTime).TotalSeconds;
                if (deltaTime >= 0.1d)
                {
                    updating = true;
                    updateTime = DateTime.Now;
                    GeneralUpdate(deltaTime);
                }
            }
            if (warUpdating == false)
            {
                double deltaTime = (DateTime.Now - warUpdateTime).TotalSeconds;
                if (deltaTime >= 0.1d)
                {
                    warUpdating = true;
                    warUpdateTime = DateTime.Now;
                    WarUpdate();
                }
            }
            if (warCheckUpdating == false)
            {
                double deltaTime = (DateTime.Now - warCheckUpdateTime).TotalSeconds;
                if (deltaTime >= 0.1d)
                {
                    warCheckUpdating = true;
                    warCheckUpdateTime = DateTime.Now;
                    GeneralUpdateWar();
                }
            }
        }

        public async static void PlayerDisconnected(int id)
        {
            long account_id = Server.clients[id].account;
            await PlayerDisconnectedAsync(account_id);
            EndBattle(account_id, true, 0);
        }

        private async static Task<bool> PlayerDisconnectedAsync(long account_id)
        {
            Task<bool> task = Task.Run(() =>
            {
                return Retry.Do(() => _PlayerDisconnectedAsync(account_id), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static bool _PlayerDisconnectedAsync(long account_id)
        {
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = String.Format("UPDATE accounts SET is_online = 0, client_id = 0 WHERE id = {0}", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
                connection.Close();
            }
            return true;
        }

        #endregion


        #region Player

        public async static void AuthenticatePlayer(int id, string device, string password, string username)
        {
            Data.InitializationData auth = await AuthenticatePlayerAsync(id, device, password, username);
            if(auth != null)
            {
                Server.clients[id].device = device;
                Server.clients[id].account = auth.accountID;
                string authData = await Data.SerializeAsync<Data.InitializationData>(auth);
                Packet packet = new Packet();
                packet.Write((int)Terminal.RequestsID.AUTH);
                byte[] bytes = await Data.CompressAsync(authData);
                packet.Write(bytes.Length);
                packet.Write(bytes);
                Sender.TCP_Send(id, packet);
            }
        }

        private async static Task<Data.Player> GetPlayerDataAsync(long id)
        {
            Task<Data.Player> task = Task.Run(() =>
            {
                return Retry.Do(() => _GetPlayerDataAsync(id), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static Data.Player _GetPlayerDataAsync(long id)
        {
            Data.Player data = new Data.Player();
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = String.Format("SELECT id, name, gems, trophies, shield, level, xp, clan_join_timer, clan_id, clan_rank, war_id, NOW() AS now_time, email FROM accounts WHERE id = {0};", id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                data.id = id;
                                data.name = reader["name"].ToString();
                                data.email = reader["email"].ToString();
                                int.TryParse(reader["gems"].ToString(), out data.gems);
                                int.TryParse(reader["trophies"].ToString(), out data.trophies);
                                DateTime.TryParse(reader["now_time"].ToString(), out data.nowTime);
                                DateTime.TryParse(reader["shield"].ToString(), out data.shield);
                                DateTime.TryParse(reader["clan_join_timer"].ToString(), out data.clanTimer);
                                int.TryParse(reader["level"].ToString(), out data.level);
                                int.TryParse(reader["xp"].ToString(), out data.xp);
                                long.TryParse(reader["clan_id"].ToString(), out data.clanID);
                                int.TryParse(reader["clan_rank"].ToString(), out data.clanRank);
                                long.TryParse(reader["war_id"].ToString(), out data.warID);
                            }
                        }
                    }
                }
                connection.Close();
            }
            return data;
        }

        public async static void SyncPlayerData(int id, string device)
        {
            long account_id = Server.clients[id].account;
            Data.Player player = await GetPlayerDataAsync(account_id);
            List<Data.Building> buildings = await GetBuildingsAsync(account_id);
            player.units = await GetUnitsAsync(account_id);
            player.spells = await GetSpellsAsync(account_id);
            player.buildings = buildings;
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.SYNC);
            string playerData = await Data.SerializeAsync<Data.Player>(player);
            packet.Write(playerData);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<Data.InitializationData> AuthenticatePlayerAsync(int id, string device, string password, string username)
        {
            Task<Data.InitializationData> task = Task.Run(() =>
            {
                return Retry.Do(() => _AuthenticatePlayerAsync(id, device, password, username), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static Data.InitializationData _AuthenticatePlayerAsync(int id, string device, string password, string username)
        {
            Data.InitializationData initializationData = new Data.InitializationData();
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = String.Format("SELECT id, password FROM accounts WHERE device_id = '{0}' AND password = '{1}';", device, password);
                bool found = false;
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                initializationData.accountID = long.Parse(reader["id"].ToString());
                                initializationData.password = password;
                                found = true;
                            }
                        }
                    }
                }
                if (found == false)
                {
                    // TODO: Log out any account with this device_id
                    query = String.Format("UPDATE accounts SET device_id = '' WHERE device_id = '{0}'", device);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    initializationData.password = Data.EncrypteToMD5(Tools.GenerateToken());
                    query = String.Format("INSERT INTO accounts (device_id, password, name) VALUES('{0}', '{1}', '{2}');", device, initializationData.password, username);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        command.ExecuteNonQuery();
                        initializationData.accountID = command.LastInsertedId;
                    }
                    query = String.Format("INSERT INTO buildings (global_id, account_id, x_position, y_position, level, track_time, x_war, y_war) VALUES('{0}', {1}, {2}, {3}, 1, NOW() - INTERVAL 1 HOUR, {4}, {5});", Data.BuildingID.townhall.ToString(), initializationData.accountID, 25, 25, 25, 25);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    query = String.Format("INSERT INTO buildings (global_id, account_id, x_position, y_position, level, track_time, x_war, y_war) VALUES('{0}', {1}, {2}, {3}, 1, NOW() - INTERVAL 1 HOUR, {4}, {5});", Data.BuildingID.goldmine.ToString(), initializationData.accountID, 27, 21, 27, 21);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    query = String.Format("INSERT INTO buildings (global_id, account_id, x_position, y_position, level, track_time, x_war, y_war) VALUES('{0}', {1}, {2}, {3}, 1, NOW() - INTERVAL 1 HOUR, {4}, {5});", Data.BuildingID.goldstorage.ToString(), initializationData.accountID, 30, 28, 30, 28);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    query = String.Format("INSERT INTO buildings (global_id, account_id, x_position, y_position, level, track_time, x_war, y_war) VALUES('{0}', {1}, {2}, {3}, 1, NOW() - INTERVAL 1 HOUR, {4}, {5});", Data.BuildingID.elixirmine.ToString(), initializationData.accountID, 21, 27, 21, 27);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    query = String.Format("INSERT INTO buildings (global_id, account_id, x_position, y_position, level, track_time, x_war, y_war) VALUES('{0}', {1}, {2}, {3}, 1, NOW() - INTERVAL 1 HOUR, {4}, {5});", Data.BuildingID.elixirstorage.ToString(), initializationData.accountID, 25, 30, 25, 30);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    query = String.Format("INSERT INTO buildings (global_id, account_id, x_position, y_position, level, track_time, x_war, y_war) VALUES('{0}', {1}, {2}, {3}, 1, NOW() - INTERVAL 1 HOUR, {4}, {5});", Data.BuildingID.buildershut.ToString(), initializationData.accountID, 22, 24, 22, 24);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    AddResources(connection, initializationData.accountID, 10000, 10000, 0, 250);
                }
                query = String.Format("UPDATE accounts SET is_online = 1, client_id = {0} WHERE id = {1}", id, initializationData.accountID);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
                initializationData.serverUnits = GetServerUnits(connection);
                initializationData.serverSpells = GetServerSpells(connection);
                connection.Close();
            }
            return initializationData;
        }

        private async static Task<int> LogOutAsync(long account_id, string device)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _LogOutAsync(account_id, device), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }
        public async static void LogOut(int id, string device)
        {
            long account_id = Server.clients[id].account;
            int response = await LogOutAsync(account_id, device);
        }

        private static int _LogOutAsync(long account_id, string device)
        {
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = String.Format("SELECT id FROM accounts WHERE id = {0} AND device_id = '{1}' AND is_online > 0;", account_id, device);
                bool found = false;
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            found = true;
                        }
                    }
                }
                List<int> clients = new List<int>();
                if (found)
                {
                    /*
                    query = String.Format("SELECT client_id FROM accounts WHERE device_id = '{0}' AND is_online > 0 AND id <> = {1};", device, account_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    int id = 0;
                                    int.TryParse(reader["client_id"].ToString(), out id);
                                    clients.Add(id);
                                }
                            }
                        }
                    }
                    */
                    query = String.Format("UPDATE accounts SET device_id = '', is_online = 0 WHERE device_id = '{0}';", device);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    /*
                    for (int i = 0; i < clients.Count; i++)
                    {
                        // TODO -> Disconnect
                    }
                    */
                }
                connection.Close();
            }
            return response;
        }

        #endregion

        #region Helpers

        private static int GetBuildingCount(long accountID, string globalID, MySqlConnection connection)
        {
            int count = 0;
            string query = String.Format("SELECT id FROM buildings WHERE account_id = {0} AND global_id = '{1}';", accountID, globalID);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            count++;
                        }
                    }
                }
            }
            return count;
        }

        private static int GetBuildingConstructionCount(long accountID, MySqlConnection connection)
        {
            int count = 0;
            string query = String.Format("SELECT id FROM buildings WHERE account_id = {0} AND is_constructing > 0;", accountID);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            count++;
                        }
                    }
                }
            }
            return count;
        }

        #endregion

        #region Resource Manager

        private static bool SpendResources(MySqlConnection connection, long account_id, int gold, int elixir, int gems, int darkElixir)
        {
            if (CheckResources(connection, account_id, gold, elixir, gems, darkElixir))
            {
                if (gold > 0 || elixir > 0 || darkElixir > 0)
                {
                    List<Data.Building> buildings = new List<Data.Building>();
                    string query = String.Format("SELECT id, global_id, gold_storage, elixir_storage, dark_elixir_storage FROM buildings WHERE account_id = {0} AND global_id IN('townhall', 'goldstorage', 'elixirstorage', 'darkelixirstorage');", account_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    Data.Building building = new Data.Building();
                                    building.id = (Data.BuildingID)Enum.Parse(typeof(Data.BuildingID), reader["global_id"].ToString());
                                    building.goldStorage = (int)Math.Floor(float.Parse(reader["gold_storage"].ToString()));
                                    building.elixirStorage = (int)Math.Floor(float.Parse(reader["elixir_storage"].ToString()));
                                    building.darkStorage = (int)Math.Floor(float.Parse(reader["dark_elixir_storage"].ToString()));
                                    building.databaseID = long.Parse(reader["id"].ToString());
                                    buildings.Add(building);
                                }
                            }
                        }
                    }
                    if (buildings.Count > 0)
                    {
                        int spendGold = 0;
                        int spendElixir = 0;
                        int spendDarkElixir = 0;
                        for (int i = 0; i < buildings.Count; i++)
                        {
                            if (spendGold >= gold && spendElixir >= elixir && spendDarkElixir >= darkElixir)
                            {
                                break;
                            }
                            int toSpendGold = 0;
                            int toSpendElixir = 0;
                            int toSpendDark = 0;
                            switch (buildings[i].id)
                            {
                                case Data.BuildingID.townhall:
                                    if (spendGold < gold)
                                    {
                                        if (buildings[i].goldStorage >= (gold - spendGold))
                                        {
                                            toSpendGold = gold - spendGold;
                                        }
                                        else
                                        {
                                            toSpendGold = buildings[i].goldStorage;
                                        }
                                        spendGold += toSpendGold;
                                    }
                                    if (spendElixir < elixir)
                                    {
                                        if (buildings[i].elixirStorage >= (elixir - spendElixir))
                                        {
                                            toSpendElixir = elixir - spendElixir;
                                        }
                                        else
                                        {
                                            toSpendElixir = buildings[i].elixirStorage;
                                        }
                                        spendElixir += toSpendElixir;
                                    }
                                    if (spendDarkElixir < darkElixir)
                                    {
                                        if (buildings[i].darkStorage >= (darkElixir - spendDarkElixir))
                                        {
                                            toSpendDark = darkElixir - spendDarkElixir;
                                        }
                                        else
                                        {
                                            toSpendDark = buildings[i].darkStorage;
                                        }
                                        spendDarkElixir += toSpendDark;
                                    }
                                    break;
                                case Data.BuildingID.goldstorage:
                                    if (spendGold < gold)
                                    {
                                        if (buildings[i].goldStorage >= (gold - spendGold))
                                        {
                                            toSpendGold = gold - spendGold;
                                        }
                                        else
                                        {
                                            toSpendGold = buildings[i].goldStorage;
                                        }
                                        spendGold += toSpendGold;
                                    }
                                    break;
                                case Data.BuildingID.elixirstorage:
                                    if (spendElixir < elixir)
                                    {
                                        if (buildings[i].elixirStorage >= (elixir - spendElixir))
                                        {
                                            toSpendElixir = elixir - spendElixir;
                                        }
                                        else
                                        {
                                            toSpendElixir = buildings[i].elixirStorage;
                                        }
                                        spendElixir += toSpendElixir;
                                    }
                                    break;
                                case Data.BuildingID.darkelixirstorage:
                                    if (spendDarkElixir < darkElixir)
                                    {
                                        if (buildings[i].darkStorage >= (darkElixir - spendDarkElixir))
                                        {
                                            toSpendDark = darkElixir - spendDarkElixir;
                                        }
                                        else
                                        {
                                            toSpendDark = buildings[i].darkStorage;
                                        }
                                        spendDarkElixir += toSpendDark;
                                    }
                                    break;
                            }
                            query = String.Format("UPDATE buildings SET gold_storage = gold_storage - {0}, elixir_storage = elixir_storage - {1}, dark_elixir_storage = dark_elixir_storage - {2} WHERE id = {3};", toSpendGold, toSpendElixir, toSpendDark, buildings[i].databaseID);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                        }
                        if (spendGold < gold || spendElixir < elixir || spendDarkElixir < darkElixir)
                        {
                            return false;
                        }
                    }
                    else
                    {
                        return false;
                    }
                }

                if (gems > 0)
                {
                    string query = String.Format("UPDATE accounts SET gems = gems - {0} WHERE id = {1};", gems, account_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }
            else
            {
                return false;
            }
            return true;
        }

        private static bool CheckResources(MySqlConnection connection, long account_id, int gold, int elixir, int gems, int darkElixir)
        {
            int haveGold = 0;
            int haveElixir = 0;
            int haveGems = 0;
            int haveDarkElixir = 0;

            if (gold > 0 || elixir > 0 || darkElixir > 0)
            {
                string query = String.Format("SELECT global_id, gold_storage, elixir_storage, dark_elixir_storage FROM buildings WHERE account_id = {0} AND global_id IN('townhall', 'goldstorage', 'elixirstorage', 'darkelixirstorage');", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                Data.BuildingID id = (Data.BuildingID)Enum.Parse(typeof(Data.BuildingID), reader["global_id"].ToString());
                                int gold_storage = (int)Math.Floor(float.Parse(reader["gold_storage"].ToString()));
                                int elixir_storage = (int)Math.Floor(float.Parse(reader["elixir_storage"].ToString()));
                                int dark_elixir_storage = (int)Math.Floor(float.Parse(reader["dark_elixir_storage"].ToString()));
                                switch (id)
                                {
                                    case Data.BuildingID.townhall:
                                        haveGold += gold_storage;
                                        haveElixir += elixir_storage;
                                        haveDarkElixir += dark_elixir_storage;
                                        break;
                                    case Data.BuildingID.goldstorage:
                                        haveGold += gold_storage;
                                        break;
                                    case Data.BuildingID.elixirstorage:
                                        haveElixir += elixir_storage;
                                        break;
                                    case Data.BuildingID.darkelixirstorage:
                                        haveDarkElixir += dark_elixir_storage;
                                        break;
                                }
                            }
                        }
                    }
                }
                if (haveGold < gold || haveElixir < elixir || haveDarkElixir < darkElixir)
                {
                    return false;
                }
            }

            if (gems > 0)
            {
                string query = String.Format("SELECT gems FROM accounts WHERE id = {0}", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                haveGems = int.Parse(reader["gems"].ToString());
                            }
                        }
                    }
                }
                if (haveGems < gems)
                {
                    return false;
                }
            }

            return true;
        }

        private static (int, int, int, int) AddResources(MySqlConnection connection, long account_id, int gold, int elixir, int darkElixir, int gems)
        {
            int addedGold = 0;
            int addedElixir = 0;
            int addedDark = 0;

            if (gold > 0 || elixir > 0 || darkElixir > 0)
            {
                List<Data.Building> storages = new List<Data.Building>();
                string query = String.Format("SELECT buildings.id, buildings.global_id, buildings.gold_storage, buildings.elixir_storage, buildings.dark_elixir_storage, server_buildings.gold_capacity, server_buildings.elixir_capacity, server_buildings.dark_elixir_capacity FROM buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level WHERE buildings.account_id = {0} AND buildings.global_id IN('{1}', '{2}', '{3}', '{4}') AND buildings.level > 0;", account_id, Data.BuildingID.townhall.ToString(), Data.BuildingID.goldstorage.ToString(), Data.BuildingID.elixirstorage.ToString(), Data.BuildingID.darkelixirstorage.ToString());
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                Data.Building building = new Data.Building();
                                building.databaseID = long.Parse(reader["id"].ToString());
                                building.id = (Data.BuildingID)Enum.Parse(typeof(Data.BuildingID), reader["global_id"].ToString());
                                building.goldStorage = (int)Math.Floor(float.Parse(reader["gold_storage"].ToString()));
                                building.elixirStorage = (int)Math.Floor(float.Parse(reader["elixir_storage"].ToString()));
                                building.darkStorage = (int)Math.Floor(float.Parse(reader["dark_elixir_storage"].ToString()));
                                building.goldCapacity = int.Parse(reader["gold_capacity"].ToString());
                                building.elixirCapacity = int.Parse(reader["elixir_capacity"].ToString());
                                building.darkCapacity = int.Parse(reader["dark_elixir_capacity"].ToString());
                                storages.Add(building);
                            }
                        }
                    }
                }

                if (storages.Count > 0)
                {
                    int remainedGold = gold;
                    int remainedElixir = elixir;
                    int remainedDatk = darkElixir;
                    for (int i = 0; i < storages.Count; i++)
                    {
                        if (remainedGold <= 0 && remainedElixir <= 0 && remainedDatk <= 0)
                        {
                            break;
                        }

                        int goldSpace = storages[i].goldCapacity - storages[i].goldStorage;
                        int elixirSpace = storages[i].elixirCapacity - storages[i].elixirStorage;
                        int darkSpace = storages[i].darkCapacity - storages[i].darkStorage;

                        int addGold = 0;
                        int addElixir = 0;
                        int addDark = 0;

                        switch (storages[i].id)
                        {
                            case Data.BuildingID.townhall:
                                addGold = (goldSpace >= remainedGold) ? remainedGold : goldSpace;
                                addElixir = (elixirSpace >= remainedElixir) ? remainedElixir : elixirSpace;
                                addDark = (darkSpace >= remainedDatk) ? remainedDatk : darkSpace;
                                break;
                            case Data.BuildingID.goldstorage:
                                addGold = (goldSpace >= remainedGold) ? remainedGold : goldSpace;
                                break;
                            case Data.BuildingID.elixirstorage:
                                addElixir = (elixirSpace >= remainedElixir) ? remainedElixir : elixirSpace;
                                break;
                            case Data.BuildingID.darkelixirstorage:
                                addDark = (darkSpace >= remainedDatk) ? remainedDatk : darkSpace;
                                break;
                        }

                        query = String.Format("UPDATE buildings SET gold_storage = gold_storage + {0}, elixir_storage = elixir_storage + {1}, dark_elixir_storage = dark_elixir_storage + {2} WHERE id = {3};", addGold, addElixir, addDark, storages[i].databaseID);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }

                        remainedGold -= addGold;
                        remainedElixir -= addElixir;
                        remainedDatk -= addDark;

                        addedGold += addGold;
                        addedElixir += addElixir;
                        addedDark += addDark;
                    }
                }
            }

            if (gems > 0)
            {
                string query = String.Format("UPDATE accounts SET gems = gems + {0} WHERE id = {1};", gems, account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
            }

            return (addedGold, addedElixir, addedDark, gems);
        }

        private static void AddXP(MySqlConnection connection, long account_id, int xp)
        {
            int haveXp = 0;
            int level = 0;
            string query = String.Format("SELECT xp, level FROM accounts WHERE id = {0}", account_id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            int.TryParse(reader["xp"].ToString(), out haveXp);
                            int.TryParse(reader["level"].ToString(), out level);
                        }
                    }
                    else
                    {
                        return;
                    }
                }
            }
            int reachedLevel = level;
            int reqXp = Data.GetNexLevelRequiredXp(reachedLevel);
            int remainedXp = haveXp + xp;
            while (remainedXp >= reqXp)
            {
                remainedXp -= reqXp;
                reachedLevel++;
                reqXp = Data.GetNexLevelRequiredXp(reachedLevel);
            }
            query = String.Format("UPDATE accounts SET level = {0}, xp = {1} WHERE id = {2} AND level = {3} AND xp = {4}", reachedLevel, remainedXp, account_id, level, haveXp);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }
        }

        private static void AddClanXP(MySqlConnection connection, long clan_id, int xp)
        {
            int haveXp = 0;
            int level = 0;
            string query = String.Format("SELECT xp, level FROM clans WHERE id = {0}", clan_id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            int.TryParse(reader["xp"].ToString(), out haveXp);
                            int.TryParse(reader["level"].ToString(), out level);
                        }
                    }
                    else
                    {
                        return;
                    }
                }
            }
            int reachedLevel = level;
            int reqXp = Data.GetClanNexLevelRequiredXp(reachedLevel);
            int remainedXp = haveXp + xp;
            while (remainedXp >= reqXp)
            {
                remainedXp -= reqXp;
                reachedLevel++;
                reqXp = Data.GetClanNexLevelRequiredXp(reachedLevel);
            }
            query = String.Format("UPDATE clans SET level = {0}, xp = {1} WHERE id = {2} AND level = {3} AND xp = {4}", reachedLevel, remainedXp, clan_id, level, haveXp);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }
        }

        private static void ChangeClanTrophies(MySqlConnection connection, long clan_id, int amount)
        {
            if (amount == 0) { return; }
            if (amount > 0)
            {
                string query = String.Format("UPDATE clans SET trophies = trophies + {0} WHERE id = {1}", amount, clan_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
            else
            {
                string query = String.Format("UPDATE clans SET trophies = trophies - {0} WHERE id = {1}", -amount, clan_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
                query = String.Format("UPDATE clans SET trophies = 0 WHERE id = {0} AND trophies < 0", clan_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        private static void ChangeTrophies(MySqlConnection connection, long account_id, int amount)
        {
            if (amount == 0) { return; }
            if (amount > 0)
            {
                string query = String.Format("UPDATE accounts SET trophies = trophies + {0} WHERE id = {1}", amount, account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
            else
            {
                string query = String.Format("UPDATE accounts SET trophies = trophies - {0} WHERE id = {1}", -amount, account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
                query = String.Format("UPDATE accounts SET trophies = 0 WHERE id = {0} AND trophies < 0", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        #endregion

        #region Server Buildings

        private async static Task<Data.ServerBuilding> GetServerBuildingAsync(string id, int level)
        {
            Task<Data.ServerBuilding> task = Task.Run(() =>
            {
                return Retry.Do(() => _GetServerBuildingAsync(id, level), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static Data.ServerBuilding _GetServerBuildingAsync(string id, int level)
        {
            Data.ServerBuilding data = null;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = String.Format("SELECT id, req_gold, req_elixir, req_gems, req_dark_elixir, columns_count, rows_count, build_time, gained_xp FROM server_buildings WHERE global_id = '{0}' AND level = {1};", id, level);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                data = new Data.ServerBuilding();
                                data.id = id;
                                data.databaseID = long.Parse(reader["id"].ToString());
                                data.level = level;
                                data.requiredGold = int.Parse(reader["req_gold"].ToString());
                                data.requiredElixir = int.Parse(reader["req_elixir"].ToString());
                                data.requiredGems = int.Parse(reader["req_gems"].ToString());
                                data.requiredDarkElixir = int.Parse(reader["req_dark_elixir"].ToString());
                                data.columns = int.Parse(reader["columns_count"].ToString());
                                data.rows = int.Parse(reader["rows_count"].ToString());
                                data.buildTime = int.Parse(reader["build_time"].ToString());
                                data.gainedXp = int.Parse(reader["gained_xp"].ToString());
                            }
                        }
                    }
                }
                connection.Close();
            }
            return data;
        }

        #endregion

        #region Collect Resources

        public async static void UpdateCollectabes(double deltaTime)
        {
            await UpdateCollectabesAsync(deltaTime);
            collecting = false;
        }

        public async static void CollectMapResources(int id, int gold, int elixir, int gems)
        {
            long account_id = Server.clients[id].account;
            await CollectMapResourcesAsync(account_id, id, gold, elixir, gems);
        }

        private async static Task<bool> CollectMapResourcesAsync(long account_id, int clientID, int gold, int elixir, int gems)
        {
            Task<bool> task = Task.Run(() =>
            {
                return Retry.Do(() => _CollectMapResources(account_id, clientID, gold, elixir, gems), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static bool _CollectMapResources(long account_id, int clientID, int gold, int elixir, int gems)
        {
            using (MySqlConnection connection = GetMysqlConnection())
            {
                // кладём в хранилища: gold, elixir, НО darkElixir = 0 (ты просил без тёмного)
                // метод вернёт, сколько реально влезло
                var added = AddResources(connection, account_id, gold, elixir, 0, gems);
                connection.Close();

                // отправим клиенту, что именно зачислилось
                // у нас уже есть в Terminal.RequestsID.MAPCOLLECT = 40
                Packet packet = new Packet();
                packet.Write((int)Terminal.RequestsID.MAPCOLLECT);
                packet.Write(added.Item1); // реально добавленное золото
                packet.Write(added.Item2); // реально добавленный эликсир
                packet.Write(gems);        // гемы у тебя в AddResources всегда зачисляются полностью
                Sender.TCP_Send(clientID, packet);
            }
            return true;
        }


        private async static Task<bool> UpdateCollectabesAsync(double deltaTime)
        {
            Task<bool> task = Task.Run(() =>
            {
                return Retry.Do(() => _UpdateCollectabesAsync(deltaTime), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static bool _UpdateCollectabesAsync(double deltaTime)
        {
            using (MySqlConnection connection = GetMysqlConnection())
            {

                // Add Gold
                string query = String.Format("UPDATE buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level SET buildings.gold_storage = buildings.gold_storage + (server_buildings.speed * {0} * IF(buildings.boost >= NOW(), 2, 1)) WHERE buildings.global_id = '{1}' AND buildings.level > 0", deltaTime / 3600d, Data.BuildingID.goldmine.ToString());
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }

                // Add Elixir
                query = String.Format("UPDATE buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level SET buildings.elixir_storage = buildings.elixir_storage + (server_buildings.speed * {0} * IF(buildings.boost >= NOW(), 2, 1)) WHERE buildings.global_id = '{1}' AND buildings.level > 0", deltaTime / 3600d, Data.BuildingID.elixirmine.ToString());
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }

                // Add Dark Elixir
                query = String.Format("UPDATE buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level SET buildings.dark_elixir_storage = buildings.dark_elixir_storage + (server_buildings.speed * {0} * IF(buildings.boost >= NOW(), 2, 1)) WHERE buildings.global_id = '{1}' AND buildings.level > 0", deltaTime / 3600d, Data.BuildingID.darkelixirmine.ToString());
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }

                // Limit Gold
                query = String.Format("UPDATE buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level SET buildings.gold_storage = server_buildings.gold_capacity WHERE buildings.gold_storage > server_buildings.gold_capacity AND buildings.global_id = '{0}' And buildings.level > 0", Data.BuildingID.goldmine.ToString());
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }

                // Limit Elixir
                query = String.Format("UPDATE buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level SET buildings.elixir_storage = server_buildings.elixir_capacity WHERE buildings.elixir_storage > server_buildings.elixir_capacity AND buildings.global_id = '{0}' And buildings.level > 0", Data.BuildingID.elixirmine.ToString());
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }

                // Limit Dark Elixir
                query = String.Format("UPDATE buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level SET buildings.dark_elixir_storage = server_buildings.dark_elixir_capacity WHERE buildings.dark_elixir_storage > server_buildings.dark_elixir_capacity AND buildings.global_id = '{0}' And buildings.level > 0", Data.BuildingID.darkelixirmine.ToString());
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
                connection.Close();
            }
            return true;
        }

        public async static void Collect(int id, long database_id)
        {
            long account_id = Server.clients[id].account;
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.COLLECT);
            int amount = await CollectAsync(account_id, database_id);
            packet.Write(database_id);
            packet.Write(amount);
            Sender.TCP_Send(id, packet);
        }
        
        private async static Task<int> CollectAsync(long account_id, long database_id)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _CollectAsync(account_id, database_id), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _CollectAsync(long account_id, long database_id)
        {
            int amount = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                int amountGold = 0;
                int amountElixir = 0;
                int amountDark = 0;
                string query = String.Format("SELECT global_id, gold_storage, elixir_storage, dark_elixir_storage FROM buildings WHERE id = {0} AND account_id = {1};", database_id, account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                Data.BuildingID global_id = (Data.BuildingID)Enum.Parse(typeof(Data.BuildingID), reader["global_id"].ToString());
                                switch (global_id)
                                {
                                    case Data.BuildingID.goldmine:
                                        amountGold = (int)Math.Floor(float.Parse(reader["gold_storage"].ToString()));
                                        break;
                                    case Data.BuildingID.elixirmine:
                                        amountElixir = (int)Math.Floor(float.Parse(reader["elixir_storage"].ToString()));
                                        break;
                                    case Data.BuildingID.darkelixirmine:
                                        amountDark = (int)Math.Floor(float.Parse(reader["dark_elixir_storage"].ToString()));
                                        break;
                                }
                            }
                        }
                    }
                }
                if (amountGold > 0)
                {
                    amount = AddResources(connection, account_id, amountGold, 0, 0, 0).Item1;
                    query = String.Format("UPDATE buildings SET gold_storage = gold_storage - {0} WHERE id = {1};", amount, database_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection)) { command.ExecuteNonQuery(); }
                }
                else if (amountElixir > 0)
                {
                    amount = AddResources(connection, account_id, 0, amountElixir, 0, 0).Item2;
                    query = String.Format("UPDATE buildings SET elixir_storage = elixir_storage - {0} WHERE id = {1};", amount, database_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection)) { command.ExecuteNonQuery(); }
                }
                else if (amountDark > 0)
                {
                    amount = AddResources(connection, account_id, 0, 0, amountDark, 0).Item3;
                    query = String.Format("UPDATE buildings SET dark_elixir_storage = dark_elixir_storage - {0} WHERE id = {1};", amount, database_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection)) { command.ExecuteNonQuery(); }
                }
                connection.Close();
            }
            return amount;
        }

        #endregion

        #region General Update

        public async static void GeneralUpdate(double deltaTime)
        {
            await GeneralUpdateAsync(deltaTime);
            updating = false;
        }
        
        private async static Task<bool> GeneralUpdateAsync(double deltaTime)
        {
            Task<bool> task = Task.Run(() =>
            {
                return Retry.Do(() => _GeneralUpdateAsync(deltaTime), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static bool _GeneralUpdateAsync(double deltaTime)
        {
            using (MySqlConnection connection = GetMysqlConnection())
            {
                GeneralUpdateBuildings(connection);
                GeneralUpdateUnitTraining(connection, (float)deltaTime);
                GeneralUpdateSpellBrewing(connection, (float)deltaTime);
                GeneralUpdateBattle(connection, deltaTime);
                connection.Close();
            }
            return true;
        }

        private static void GeneralUpdateBuildings(MySqlConnection connection)
        {
            string time = "";
            string query = String.Format("SELECT NOW() AS time");
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            time = DateTime.Parse(reader["time"].ToString()).ToString("yyyy-MM-dd H:mm:ss");
                        }
                    }
                }
            }

            query = String.Format("UPDATE buildings SET level = level + 1, is_constructing = 0, track_time = '{0}' WHERE is_constructing > 0 AND construction_time <= NOW()", time);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }

            Dictionary<long, int> accounts = new Dictionary<long, int>();

            query = String.Format("SELECT buildings.account_id, server_buildings.gained_xp FROM buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level WHERE buildings.track_time = '{0}'", time);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            long id = 0;
                            int xp = 0;
                            if (long.TryParse(reader["account_id"].ToString(), out id) && int.TryParse(reader["gained_xp"].ToString(), out xp) && xp > 0)
                            {
                                accounts.Add(id, xp);
                            }
                        }
                    }
                }
            }

            query = String.Format("UPDATE buildings SET track_time = track_time - INTERVAL 1 HOUR WHERE track_time = '{0}'", time);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }

            if (accounts.Count > 0)
            {
                foreach (var account in accounts)
                {
                    AddXP(connection, account.Key, account.Value);
                }
            }
        }

        private static void GeneralUpdateUnitTraining(MySqlConnection connection, float deltaTime)
        {
            string query = String.Format("UPDATE units LEFT JOIN server_units ON units.global_id = server_units.global_id AND units.level = server_units.level SET trained = 1 WHERE units.trained_time >= server_units.train_time");
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }

            query = String.Format("UPDATE units AS t1 INNER JOIN (SELECT units.id FROM units LEFT JOIN server_units ON units.global_id = server_units.global_id AND units.level = server_units.level WHERE units.trained <= 0 AND units.trained_time < server_units.train_time GROUP BY units.account_id) t2 ON t1.id = t2.id SET trained_time = trained_time + {0}", deltaTime);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }

            query = String.Format("UPDATE units AS t1 INNER JOIN (SELECT units.id, (IFNULL(buildings.capacity, 0) - IFNULL(t.occupied, 0)) AS capacity, server_units.housing FROM units LEFT JOIN server_units ON units.global_id = server_units.global_id AND units.level = server_units.level LEFT JOIN (SELECT buildings.account_id, SUM(server_buildings.capacity) AS capacity FROM buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level WHERE buildings.global_id = 'armycamp' AND buildings.level > 0 GROUP BY buildings.account_id) AS buildings ON units.account_id = buildings.account_id LEFT JOIN (SELECT units.account_id, SUM(server_units.housing) AS occupied FROM units LEFT JOIN server_units ON units.global_id = server_units.global_id AND units.level = server_units.level WHERE units.ready > 0 GROUP BY units.account_id) AS t ON units.account_id = t.account_id WHERE units.trained > 0 AND units.ready <= 0 GROUP BY units.account_id) t2 ON t1.id = t2.id SET ready = 1 WHERE housing <= capacity");
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }
        }

        private static void GeneralUpdateSpellBrewing(MySqlConnection connection, float deltaTime)
        {
            string query = String.Format("UPDATE spells LEFT JOIN server_spells ON spells.global_id = server_spells.global_id AND spells.level = server_spells.level SET brewed = 1 WHERE spells.brewed_time >= server_spells.brew_time");
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }

            query = String.Format("UPDATE spells AS t1 INNER JOIN (SELECT spells.id FROM spells LEFT JOIN server_spells ON spells.global_id = server_spells.global_id AND spells.level = server_spells.level WHERE spells.brewed <= 0 AND spells.brewed_time < server_spells.brew_time GROUP BY spells.account_id) t2 ON t1.id = t2.id SET brewed_time = brewed_time + {0}", deltaTime);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }

            query = String.Format("UPDATE spells AS t1 INNER JOIN (SELECT spells.id, (IFNULL(buildings.capacity, 0) - IFNULL(t.occupied, 0)) AS capacity, server_spells.housing, server_spells.building_code FROM spells LEFT JOIN server_spells ON spells.global_id = server_spells.global_id AND spells.level = server_spells.level LEFT JOIN (SELECT buildings.account_id, SUM(server_buildings.capacity) AS capacity FROM buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level WHERE buildings.global_id = '{0}' AND buildings.level > 0 GROUP BY buildings.account_id) AS buildings ON spells.account_id = buildings.account_id LEFT JOIN (SELECT spells.account_id, SUM(server_spells.housing) AS occupied FROM spells LEFT JOIN server_spells ON spells.global_id = server_spells.global_id AND spells.level = server_spells.level WHERE spells.ready > 0 AND server_spells.building_code = {1} GROUP BY spells.account_id) AS t ON spells.account_id = t.account_id WHERE spells.brewed > 0 AND spells.ready <= 0 GROUP BY spells.account_id) t2 ON t1.id = t2.id SET ready = 1 WHERE housing <= capacity AND building_code = {1}", Data.BuildingID.spellfactory.ToString(), 0);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }

            query = String.Format("UPDATE spells AS t1 INNER JOIN (SELECT spells.id, (IFNULL(buildings.capacity, 0) - IFNULL(t.occupied, 0)) AS capacity, server_spells.housing, server_spells.building_code FROM spells LEFT JOIN server_spells ON spells.global_id = server_spells.global_id AND spells.level = server_spells.level LEFT JOIN (SELECT buildings.account_id, SUM(server_buildings.capacity) AS capacity FROM buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level WHERE buildings.global_id = '{0}' AND buildings.level > 0 GROUP BY buildings.account_id) AS buildings ON spells.account_id = buildings.account_id LEFT JOIN (SELECT spells.account_id, SUM(server_spells.housing) AS occupied FROM spells LEFT JOIN server_spells ON spells.global_id = server_spells.global_id AND spells.level = server_spells.level WHERE spells.ready > 0 AND server_spells.building_code = {1} GROUP BY spells.account_id) AS t ON spells.account_id = t.account_id WHERE spells.brewed > 0 AND spells.ready <= 0 GROUP BY spells.account_id) t2 ON t1.id = t2.id SET ready = 1 WHERE housing <= capacity AND building_code = {1}", Data.BuildingID.darkspellfactory.ToString(), 1);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }
        }

        private static void GeneralUpdateBattle(MySqlConnection connection, double deltaTime)
        {
            for (int i = battles.Count - 1; i >= 0; i--)
            {
                // Failsafe, Just in case
                double time = (DateTime.Now - battles[i].battle.baseTime).TotalSeconds;
                if (time >= battles[i].battle.duration * 1.3f)
                {
                    battles[i].battle.end = true;
                }

                while (battles[i].frames.Count > 0)
                {
                    int from = battles[i].battle.frameCount + 1;
                    for (int f = from; f <= battles[i].frames[0].frame; f++)
                    {
                        if (f == battles[i].frames[0].frame)
                        {
                            for (int u = 0; u < battles[i].frames[0].units.Count; u++)
                            {
                                battles[i].frames[0].units[u].unit = GetUnit(connection, battles[i].frames[0].units[u].id, battles[i].battle.attacker);
                                if (battles[i].frames[0].units[u].unit != null)
                                {
                                    if (battles[i].battle.CanAddUnit(battles[i].frames[0].units[u].x, battles[i].frames[0].units[u].y))
                                    {
                                        DeleteUnit(battles[i].frames[0].units[u].id, connection);
                                        battles[i].battle.AddUnit(battles[i].frames[0].units[u].unit, battles[i].frames[0].units[u].x, battles[i].frames[0].units[u].y);
                                    }
                                }
                            }
                            for (int u = 0; u < battles[i].frames[0].spells.Count; u++)
                            {
                                battles[i].frames[0].spells[u].spell = GetSpell(connection, battles[i].frames[0].spells[u].id, battles[i].battle.attacker, true);
                                if (battles[i].frames[0].spells[u].spell != null)
                                {
                                    if (battles[i].battle.CanAddSpell(battles[i].frames[0].spells[u].x, battles[i].frames[0].spells[u].y))
                                    {
                                        DeleteSpell(battles[i].frames[0].spells[u].id, connection);
                                        battles[i].battle.AddSpell(battles[i].frames[0].spells[u].spell, battles[i].frames[0].spells[u].x, battles[i].frames[0].spells[u].y);
                                    }
                                }
                            }
                            battles[i].savedFrames.Add(battles[i].frames[0]);
                        }
                        battles[i].battle.ExecuteFrame();
                    }
                    battles[i].frames.RemoveAt(0);
                }

                if (battles[i].battle.end)
                {
                    while (battles[i].battle.CanBattleGoOn())
                    {
                        if (battles[i].battle.surrender)
                        {
                            if (battles[i].battle.surrenderFrame <= 0)
                            {
                                battles[i].battle.surrenderFrame = battles[i].battle.frameCount;
                            }
                            if (battles[i].battle.frameCount >= battles[i].battle.surrenderFrame)
                            {
                                break;
                            }
                        }
                        battles[i].battle.ExecuteFrame();
                    }

                    int client_id = 0;
                    string query = String.Format("SELECT client_id FROM accounts WHERE id = {0} AND is_online > 0;", battles[i].battle.attacker);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    int.TryParse(reader["client_id"].ToString(), out client_id);
                                }
                            }
                        }
                    }

                    string reply = Data.Serialize<List<Data.BattleFrame>>(battles[i].savedFrames);

                    var looted = battles[i].battle.GetlootedResources();
                    int stars = battles[i].battle.stars;
                    int unitsDeployed = battles[i].battle.unitsDeployed;
                    int lootedGold = looted.Item1;
                    int lootedElixir = looted.Item2;
                    int lootedDark = looted.Item3;
                    int trophies = battles[i].battle.GetTrophies();
                    int frame = battles[i].battle.frameCount;

                    long attacker_id = battles[i].battle.attacker;
                    long defender_id = battles[i].battle.defender;

                    Data.BattleType battleType = battles[i].type;

                    battles.RemoveAt(i);

                    if (trophies > 0)
                    {
                        SpendResources(connection, defender_id, lootedGold, lootedElixir, 0, lootedDark);
                        AddResources(connection, attacker_id, lootedGold, lootedElixir, lootedDark, 0);
                    }

                    if (battleType == Data.BattleType.normal)
                    {
                        ChangeTrophies(connection, attacker_id, trophies);
                        ChangeTrophies(connection, defender_id, -trophies);
                    }

                    if (client_id > 0 && Server.clients[client_id].account == attacker_id)
                    {
                        Packet packet = new Packet();
                        packet.Write((int)Terminal.RequestsID.BATTLEEND);
                        packet.Write(stars);
                        packet.Write(unitsDeployed);
                        packet.Write(lootedGold);
                        packet.Write(lootedElixir);
                        packet.Write(lootedDark);
                        packet.Write(trophies);
                        packet.Write(frame);
                        Sender.TCP_Send(client_id, packet);
                    }

                    string key = Guid.NewGuid().ToString();
                    string path = dataFolderPath + key + ".txt";
                    if (!Directory.Exists(dataFolderPath))
                    {
                        Directory.CreateDirectory(dataFolderPath);
                    }
                    File.WriteAllText(path, reply);

                    long reply_id = 0;
                    if (battleType == Data.BattleType.war)
                    {
                        long warId = 0;
                        Data.Clan clan = GetClanByAccountId(connection, attacker_id);
                        if (clan.war != null)
                        {
                            warId = clan.war.id;
                        }
                        if (warId > 0)
                        {
                            query = String.Format("INSERT INTO clan_war_attacks (war_id, attacker_id, defender_id, stars, looted_gold, looted_elixir, looted_dark_elixir, replay_path) VALUES({0}, {1}, {2}, {3}, {4}, {5}, {6}, '{7}')", warId, attacker_id, defender_id, stars, lootedGold, lootedElixir, lootedDark, path.Replace("\\", "\\\\"));
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                                reply_id = command.LastInsertedId;
                            }
                        }
                    }
                    else
                    {
                        query = String.Format("INSERT INTO battles (attacker_id, defender_id, replay_path) VALUES({0}, {1}, '{2}')", attacker_id, defender_id, path.Replace("\\", "\\\\"));
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                            reply_id = command.LastInsertedId;
                        }
                    }
                }
            }
        }

        #endregion

        #region War Update

        private static bool warUpdating = false;

        public async static void WarUpdate()
        {
            await WarUpdateAsync();
            warUpdating = false;
        }

        private async static Task<int> WarUpdateAsync()
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _WarUpdateAsync(), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _WarUpdateAsync()
        {
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                List<Data.ClanWarSearch> clans = new List<Data.ClanWarSearch>();
                string query = String.Format("SELECT id, clan_id, account_id FROM clan_wars_search");
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {

                                Data.ClanWarSearch clan = new Data.ClanWarSearch();
                                long.TryParse(reader["clan_id"].ToString(), out clan.clan);
                                long.TryParse(reader["id"].ToString(), out clan.id);
                                long.TryParse(reader["account_id"].ToString(), out clan.player);
                                clan.members = null;
                                clans.Add(clan);
                                int c = -1;
                                for (int i = 0; i < clansSearch.Count; i++)
                                {
                                    if (clansSearch[i].clan == clan.clan)
                                    {
                                        if (clansSearch[i].player != clan.player)
                                        {
                                            clansSearch[i].time = DateTime.Now;
                                            clansSearch[i].members = null;
                                        }
                                        c = i;
                                        break;
                                    }
                                }
                                if (c < 0)
                                {
                                    clansSearch.Add(clan);
                                }
                            }
                        }
                    }
                }
                if (clansSearch.Count > 0)
                {
                    for (int i = clansSearch.Count - 1; i >= 0; i--)
                    {
                        bool found = false;
                        for (int j = 0; j < clans.Count; j++)
                        {
                            if (clansSearch[i].clan == clans[j].clan)
                            {
                                found = true;
                                break;
                            }
                        }
                        if (found)
                        {
                            if (clansSearch[i].members == null)
                            {
                                clansSearch[i].members = new List<Data.ClanWarSearchMember>();
                                List<Data.ClanMember> members = GetClanMembers(connection, clansSearch[i].clan);
                                for (int k = 0; k < members.Count; k++)
                                {
                                    if (members[k].warID == 0)
                                    {
                                        Data.ClanWarSearchMember member = new Data.ClanWarSearchMember();
                                        member.data = members[k];
                                        member.tempPosition = -1;
                                        member.warPosition = -1;
                                        member.Buildings = GetBuildings(connection, members[k].id);
                                        for (int b = 0; b < member.Buildings.Count; b++)
                                        {
                                            switch (member.Buildings[b].id)
                                            {
                                                case Data.BuildingID.townhall:
                                                    member.townHall = member.Buildings[b].level;
                                                    break;
                                                case Data.BuildingID.armycamp:
                                                    member.campsCapacity += member.Buildings[b].capacity;
                                                    break;
                                                case Data.BuildingID.barracks:
                                                    member.barracks = member.Buildings[b].level;
                                                    break;
                                                case Data.BuildingID.darkbarracks:
                                                    member.darkBarracks = member.Buildings[b].level;
                                                    break;
                                                case Data.BuildingID.spellfactory:
                                                    member.spellFactory = member.Buildings[b].level;
                                                    break;
                                                case Data.BuildingID.darkspellfactory:
                                                    member.darkSpellFactory = member.Buildings[b].level;
                                                    break;
                                                case Data.BuildingID.wall:
                                                    member.wallsPower += member.Buildings[b].level;
                                                    break;
                                            }
                                        }
                                        clansSearch[i].members.Add(member);
                                    }
                                }
                            }
                        }
                        else
                        {
                            clansSearch.RemoveAt(i);
                        }
                    }
                }

                if (clansSearch.Count > 1)
                {
                    int maxTownHallLevel = 20;
                    int maxSpellFactoryLevel = 20;
                    int maxDarkSpellFactoryLevel = 20;
                    int maxBarracksLevel = 20;
                    int maxDarkBarracksLevel = 20;
                    int maxCampsCapacity = 1000;
                    query = String.Format("SELECT MAX(level) AS level FROM server_buildings WHERE global_id = '{0}'", Data.BuildingID.townhall.ToString());
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    int.TryParse(reader["level"].ToString(), out maxTownHallLevel);
                                }
                            }
                        }
                    }
                    query = String.Format("SELECT MAX(level) AS level FROM server_buildings WHERE global_id = '{0}'", Data.BuildingID.barracks.ToString());
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    int.TryParse(reader["level"].ToString(), out maxBarracksLevel);
                                }
                            }
                        }
                    }
                    query = String.Format("SELECT MAX(level) AS level FROM server_buildings WHERE global_id = '{0}'", Data.BuildingID.darkbarracks.ToString());
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    int.TryParse(reader["level"].ToString(), out maxDarkBarracksLevel);
                                }
                            }
                        }
                    }
                    query = String.Format("SELECT MAX(level) AS level FROM server_buildings WHERE global_id = '{0}'", Data.BuildingID.spellfactory.ToString());
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    int.TryParse(reader["level"].ToString(), out maxSpellFactoryLevel);
                                }
                            }
                        }
                    }
                    query = String.Format("SELECT MAX(level) AS level FROM server_buildings WHERE global_id = '{0}'", Data.BuildingID.darkspellfactory.ToString());
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    int.TryParse(reader["level"].ToString(), out maxDarkSpellFactoryLevel);
                                }
                            }
                        }
                    }
                    Data.BuildingCount building = Data.GetBuildingLimits(maxTownHallLevel, Data.BuildingID.armycamp.ToString());
                    if (building != null)
                    {
                        query = String.Format("SELECT MAX(capacity) AS capacity FROM server_buildings WHERE global_id = '{0}'", Data.BuildingID.armycamp.ToString());
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        int capacity = 20;
                                        int.TryParse(reader["capacity"].ToString(), out capacity);
                                        if (building != null)
                                        {
                                            maxCampsCapacity = capacity * building.count;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    for (int i = clansSearch.Count - 1; i >= 0; i--)
                    {
                        if (clansSearch[i].match >= 0) { continue; }
                        /*
                        if((DateTime.Now - clansSearch[i].time).TotalHours >= 72)
                        {
                            // Maybe Cancel Search
                        }
                        */
                        int bestMatch = -1;
                        double bestMatchPercentage = -1;
                        for (int j = 0; j < clansSearch.Count; j++)
                        {
                            if (i == j || clansSearch[j].match >= 0 || clansSearch[i].members.Count != clansSearch[j].members.Count || clansSearch[i].notMatch.Contains(clansSearch[j].clan) || clansSearch[j].notMatch.Contains(clansSearch[i].clan)) { continue; }

                            double percentage = 0;

                            for (int k = 0; k < clansSearch[i].members.Count; k++)
                            {
                                clansSearch[i].members[k].tempPosition = -1;
                                clansSearch[j].members[k].tempPosition = -1;
                            }

                            int position = -1;

                            for (int k = 0; k < clansSearch[i].members.Count; k++)
                            {
                                if (clansSearch[i].members[k].tempPosition >= 0) { continue; }
                                int bestMember = -1;
                                double bestMemberPercentage = -1;
                                for (int m = 0; m < clansSearch[j].members.Count; m++)
                                {
                                    if (clansSearch[j].members[m].tempPosition >= 0) { continue; }
                                    double memberPercentage = 0;

                                    if (clansSearch[i].members[k].townHall != clansSearch[j].members[m].townHall)
                                    {
                                        memberPercentage += (1d - (Math.Abs(clansSearch[i].members[k].townHall - clansSearch[j].members[m].townHall) / (double)maxTownHallLevel)) * Data.clanWarMatchTownHallEffectPercentage;
                                    }
                                    else
                                    {
                                        memberPercentage += Data.clanWarMatchTownHallEffectPercentage;
                                    }

                                    if (clansSearch[i].members[k].campsCapacity != clansSearch[j].members[m].campsCapacity)
                                    {
                                        memberPercentage += (1d - (Math.Abs(clansSearch[i].members[k].campsCapacity - clansSearch[j].members[m].campsCapacity) / (double)maxCampsCapacity)) * Data.clanWarMatchCampsEffectPercentage;
                                    }
                                    else
                                    {
                                        memberPercentage += Data.clanWarMatchCampsEffectPercentage;
                                    }

                                    if (clansSearch[i].members[k].barracks != clansSearch[j].members[m].barracks)
                                    {
                                        memberPercentage += (1d - (Math.Abs(clansSearch[i].members[k].barracks - clansSearch[j].members[m].barracks) / (double)maxBarracksLevel)) * Data.clanWarMatchBarracksEffectPercentage;
                                    }
                                    else
                                    {
                                        memberPercentage += Data.clanWarMatchBarracksEffectPercentage;
                                    }

                                    if (clansSearch[i].members[k].darkBarracks != clansSearch[j].members[m].darkBarracks)
                                    {
                                        memberPercentage += (1d - (Math.Abs(clansSearch[i].members[k].darkBarracks - clansSearch[j].members[m].darkBarracks) / (double)maxDarkBarracksLevel)) * Data.clanWarMatchDarkBarracksEffectPercentage;
                                    }
                                    else
                                    {
                                        memberPercentage += Data.clanWarMatchDarkBarracksEffectPercentage;
                                    }

                                    if (clansSearch[i].members[k].spellFactory != clansSearch[j].members[m].spellFactory)
                                    {
                                        memberPercentage += (1d - (Math.Abs(clansSearch[i].members[k].spellFactory - clansSearch[j].members[m].spellFactory) / (double)maxSpellFactoryLevel)) * Data.clanWarMatchSpellFactoryEffectPercentage;
                                    }
                                    else
                                    {
                                        memberPercentage += Data.clanWarMatchSpellFactoryEffectPercentage;
                                    }

                                    if (clansSearch[i].members[k].darkSpellFactory != clansSearch[j].members[m].darkSpellFactory)
                                    {
                                        memberPercentage += (1d - (Math.Abs(clansSearch[i].members[k].darkSpellFactory - clansSearch[j].members[m].darkSpellFactory) / (double)maxDarkSpellFactoryLevel)) * Data.clanWarMatchDarkSpellFactoryEffectPercentage;
                                    }
                                    else
                                    {
                                        memberPercentage += Data.clanWarMatchDarkSpellFactoryEffectPercentage;
                                    }

                                    if (memberPercentage > bestMemberPercentage)
                                    {
                                        bestMember = m;
                                        bestMemberPercentage = memberPercentage;
                                    }
                                }
                                position++;
                                clansSearch[i].members[k].tempPosition = position;
                                clansSearch[j].members[bestMember].tempPosition = position;

                                percentage += bestMemberPercentage;
                            }

                            if (percentage / clansSearch[i].members.Count >= Data.clanWarMatchMinPercentage)
                            {
                                if (percentage > bestMatchPercentage)
                                {
                                    bestMatch = j;
                                    bestMatchPercentage = percentage;
                                    for (int k = 0; k < clansSearch[i].members.Count; k++)
                                    {
                                        clansSearch[i].members[k].warPosition = clansSearch[i].members[k].tempPosition;
                                        clansSearch[j].members[k].warPosition = clansSearch[j].members[k].tempPosition;
                                    }
                                }
                            }
                            else
                            {
                                clansSearch[i].notMatch.Add(clansSearch[j].clan);
                                clansSearch[j].notMatch.Add(clansSearch[i].clan);
                                if (clansSearch[i].notMatch.Count >= 100)
                                {
                                    clansSearch[i].notMatch.RemoveAt(0);
                                }
                                if (clansSearch[j].notMatch.Count >= 100)
                                {
                                    clansSearch[j].notMatch.RemoveAt(0);
                                }
                            }
                        }
                        if (bestMatch >= 0)
                        {
                            StartClanWar(connection, clansSearch[i], clansSearch[bestMatch]);
                            clansSearch[i].match = bestMatch;
                            clansSearch[bestMatch].match = i;
                        }
                    }
                    for (int i = clansSearch.Count - 1; i >= 0; i--)
                    {
                        if (clansSearch[i].match >= 0)
                        {
                            clansSearch.RemoveAt(i);
                        }
                    }
                }
                connection.Close();
            }
            return response;
        }

        public async static void GeneralUpdateWar()
        {
            await GeneralUpdateWarAsync();
            warCheckUpdating = false;
        }

        private async static Task<bool> GeneralUpdateWarAsync()
        {
            Task<bool> task = Task.Run(() =>
            {
                return Retry.Do(() => _GeneralUpdateWarAsync(), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static bool _GeneralUpdateWarAsync()
        {
            using (MySqlConnection connection = GetMysqlConnection())
            {
                List<Data.ClanWar> warsToStart = new List<Data.ClanWar>();
                List<Data.ClanWar> warsToEnd = new List<Data.ClanWar>();
                string query = String.Format("SELECT id, clan_1_id, clan_2_id, TIMESTAMPDIFF(HOUR, start_time, NOW()) AS passed_hours, stage FROM clan_wars WHERE stage > 0;");
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                int passed_hours = 0;
                                int stage = 0;
                                int.TryParse(reader["passed_hours"].ToString(), out passed_hours);
                                int.TryParse(reader["stage"].ToString(), out stage);
                                if (stage == 1 && passed_hours >= Data.clanWarPrepHours)
                                {
                                    Data.ClanWar war = new Data.ClanWar();
                                    war.stage = stage;
                                    long.TryParse(reader["id"].ToString(), out war.id);
                                    long.TryParse(reader["clan_1_id"].ToString(), out war.clan1);
                                    long.TryParse(reader["clan_2_id"].ToString(), out war.clan2);
                                    warsToStart.Add(war);
                                }
                                else if (stage == 2 && passed_hours >= Data.clanWarPrepHours + Data.clanWarBattleHours)
                                {
                                    Data.ClanWar war = new Data.ClanWar();
                                    war.stage = stage;
                                    long.TryParse(reader["id"].ToString(), out war.id);
                                    long.TryParse(reader["clan_1_id"].ToString(), out war.clan1);
                                    long.TryParse(reader["clan_2_id"].ToString(), out war.clan2);
                                    warsToEnd.Add(war);
                                }
                            }
                        }
                    }
                }

                if (warsToStart.Count > 0)
                {
                    for (int i = 0; i < warsToStart.Count; i++)
                    {
                        query = String.Format("UPDATE clan_wars SET stage = 2 WHERE id = {0};", warsToStart[i].id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }
                }

                if (warsToEnd.Count > 0)
                {
                    for (int i = 0; i < warsToEnd.Count; i++)
                    {
                        Data.ClanWarData report = new Data.ClanWarData();
                        report.clan1 = GetClan(connection, warsToEnd[i].clan1);
                        report.clan2 = GetClan(connection, warsToEnd[i].clan2);
                        report.startTime = report.clan1.war.start;

                        query = String.Format("UPDATE clan_wars SET stage = 0 WHERE id = {0};", warsToEnd[i].id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                        query = String.Format("UPDATE accounts SET war_id = -1, war_pos = 0 WHERE war_id = {0};", warsToEnd[i].id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                        query = String.Format("UPDATE clans SET war_id = 0 WHERE war_id = {0};", warsToEnd[i].id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }

                        long winnerId = 0;
                        long clanWithFirstWin = 0;
                        DateTime firstWinTime = DateTime.Now;

                        int clan1Stars = 0;
                        int clan2Stars = 0;
                        int maxStars = report.clan1.members.Count * 3;

                        if (report.clan1.war.attacks != null)
                        {
                            for (int j = 0; j < report.clan1.war.attacks.Count; j++)
                            {
                                if (report.clan1.war.attacks[i].stars > 0)
                                {
                                    if (clanWithFirstWin <= 0 || (clanWithFirstWin > 0 && report.clan1.war.attacks[i].start < firstWinTime))
                                    {
                                        clanWithFirstWin = report.clan1.id;
                                        firstWinTime = report.clan1.war.attacks[i].start;
                                    }
                                }

                                if (report.clan1.war.attacks[j].starsCounted) { continue; }

                                int bestStars = report.clan1.war.attacks[j].stars;

                                for (int k = j + 1; k < report.clan1.war.attacks.Count; k++)
                                {
                                    if (report.clan1.war.attacks[k].starsCounted) { continue; }
                                    if (report.clan1.war.attacks[k].defender == report.clan1.war.attacks[j].defender)
                                    {
                                        if (report.clan1.war.attacks[k].stars > bestStars)
                                        {
                                            bestStars = report.clan1.war.attacks[k].stars;
                                        }
                                        report.clan1.war.attacks[k].starsCounted = true;
                                    }
                                }

                                report.clan1.war.attacks[j].starsCounted = true;
                                clan1Stars += bestStars;
                            }
                        }

                        if (report.clan2.war.attacks != null)
                        {
                            for (int j = 0; j < report.clan2.war.attacks.Count; j++)
                            {
                                if (report.clan2.war.attacks[i].stars > 0)
                                {
                                    if (clanWithFirstWin <= 0 || (clanWithFirstWin > 0 && report.clan2.war.attacks[i].start < firstWinTime))
                                    {
                                        clanWithFirstWin = report.clan2.id;
                                        firstWinTime = report.clan2.war.attacks[i].start;
                                    }
                                }

                                if (report.clan2.war.attacks[j].starsCounted) { continue; }

                                int bestStars = report.clan2.war.attacks[j].stars;

                                for (int k = j + 1; k < report.clan2.war.attacks.Count; k++)
                                {
                                    if (report.clan2.war.attacks[k].starsCounted) { continue; }
                                    if (report.clan2.war.attacks[k].defender == report.clan2.war.attacks[j].defender)
                                    {
                                        if (report.clan2.war.attacks[k].stars > bestStars)
                                        {
                                            bestStars = report.clan2.war.attacks[k].stars;
                                        }
                                        report.clan2.war.attacks[k].starsCounted = true;
                                    }
                                }

                                report.clan2.war.attacks[j].starsCounted = true;
                                clan2Stars += bestStars;
                            }
                        }

                        if (clan1Stars > clan2Stars)
                        {
                            winnerId = report.clan1.id;
                        }
                        else if (clan2Stars > clan1Stars)
                        {
                            winnerId = report.clan2.id;
                        }

                        int clan1Xp = Data.GetClanWarGainedXP(clan1Stars, clan2Stars, maxStars, clanWithFirstWin == report.clan1.id);
                        int clan2Xp = Data.GetClanWarGainedXP(clan2Stars, clan1Stars, maxStars, clanWithFirstWin == report.clan2.id);

                        if (clan1Xp > 0)
                        {
                            AddClanXP(connection, report.clan1.id, clan1Xp);
                        }

                        if (clan2Xp > 0)
                        {
                            AddClanXP(connection, report.clan2.id, clan2Xp);
                        }

                        var trophies = Data.GetWarTrophies(report.clan1.trophies, report.clan2.trophies, clan1Stars, clan2Stars, maxStars);

                        ChangeClanTrophies(connection, report.clan1.id, trophies.Item1);
                        ChangeClanTrophies(connection, report.clan2.id, trophies.Item2);

                        report.clan1ID = report.clan1.id;
                        report.clan2ID = report.clan2.id;
                        report.maxStars = maxStars;
                        report.clan1Stars = clan1Stars;
                        report.clan2Stars = clan2Stars;

                        string reportData = Data.Serialize<Data.ClanWarData>(report);
                        string key = Guid.NewGuid().ToString();
                        string path = dataFolderPath + key + ".txt";
                        if (!Directory.Exists(dataFolderPath))
                        {
                            Directory.CreateDirectory(dataFolderPath);
                        }
                        File.WriteAllText(path, reportData);

                        query = String.Format("UPDATE clan_wars SET report_path = '{0}', winner_id = {1}, clan_1_stars = {2}, clan_2_stars = {3}, war_size = {4} WHERE id = {5};", path.Replace("\\", "\\\\"), winnerId, clan1Stars, clan2Stars, report.clan1.members.Count, warsToEnd[i].id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                        query = String.Format("DELETE FROM clan_war_attacks WHERE war_id = {0};", warsToEnd[i].id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }
                }

                connection.Close();
            }
            return true;
        }

        public async static void GetWarReportsList(int id)
        {
            long account_id = Server.clients[id].account;
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.WARREPORTLIST);
            List<Data.ClanWarData> response = await GetWarReportsListAsync(account_id);
            string data = await Data.SerializeAsync<List<Data.ClanWarData>>(response);
            packet.Write(data);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<List<Data.ClanWarData>> GetWarReportsListAsync(long account_id)
        {
            Task<List<Data.ClanWarData>> task = Task.Run(() =>
            {
                List<Data.ClanWarData> response = null;
                response = Retry.Do(() => _GetWarReportsListAsync(account_id), TimeSpan.FromSeconds(0.1), 1, false);
                if (response == null)
                {
                    response = new List<Data.ClanWarData>();
                }
                return response;
            });
            return await task;
        }

        private static List<Data.ClanWarData> _GetWarReportsListAsync(long account_id)
        {
            List<Data.ClanWarData> response = new List<Data.ClanWarData>();
            using (MySqlConnection connection = GetMysqlConnection())
            {
                long clan_id = 0;
                string query = String.Format("SELECT clan_id FROM accounts WHERE id = {0};", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                long.TryParse(reader["clan_id"].ToString(), out clan_id);
                            }
                        }
                    }
                }
                if (clan_id > 0)
                {
                    query = String.Format("SELECT id, clan_1_id, clan_2_id, start_time, report_path, winner_id, clan_1_stars, clan_2_stars, war_size FROM clan_wars WHERE (clan_1_id = {0} OR clan_2_id = {1}) AND stage = 0;", clan_id, clan_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    Data.ClanWarData report = new Data.ClanWarData();
                                    long.TryParse(reader["id"].ToString(), out report.id);
                                    long.TryParse(reader["winner_id"].ToString(), out report.winnerID);
                                    long.TryParse(reader["clan_1_id"].ToString(), out report.clan1ID);
                                    long.TryParse(reader["clan_2_id"].ToString(), out report.clan2ID);
                                    DateTime.TryParse(reader["start_time"].ToString(), out report.startTime);
                                    int.TryParse(reader["clan_1_stars"].ToString(), out report.clan1Stars);
                                    int.TryParse(reader["clan_2_stars"].ToString(), out report.clan2Stars);
                                    int.TryParse(reader["war_size"].ToString(), out report.size);
                                    report.maxStars = report.size * 3;
                                    report.hasReport = !string.IsNullOrEmpty(reader["report_path"].ToString());
                                    response.Add(report);
                                }
                            }
                        }
                    }
                }
                connection.Close();
            }
            return response;
        }

        public async static void GetWarReport(int id, long report_id)
        {
            long account_id = Server.clients[id].account;
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.WARREPORT);
            Data.ClanWarData response = await GetWarReportAsync(report_id);
            if (response != null)
            {
                packet.Write(true);
                string data = await Data.SerializeAsync<Data.ClanWarData>(response);
                packet.Write(data);
            }
            else
            {
                packet.Write(false);
            }
            Sender.TCP_Send(id, packet);
        }

        private async static Task<Data.ClanWarData> GetWarReportAsync(long report_id)
        {
            Task<Data.ClanWarData> task = Task.Run(() =>
            {
                return Retry.Do(() => _GetWarReportAsync(report_id), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static Data.ClanWarData _GetWarReportAsync(long report_id)
        {
            Data.ClanWarData report = null;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string path = "";
                string query = String.Format("SELECT id, clan_1_id, clan_2_id, start_time, report_path, winner_id, clan_1_stars, clan_2_stars, war_size FROM clan_wars WHERE id = {0};", report_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                report = new Data.ClanWarData();
                                long.TryParse(reader["id"].ToString(), out report.id);
                                long.TryParse(reader["winner_id"].ToString(), out report.winnerID);
                                long.TryParse(reader["clan_1_id"].ToString(), out report.clan1ID);
                                long.TryParse(reader["clan_2_id"].ToString(), out report.clan2ID);
                                DateTime.TryParse(reader["start_time"].ToString(), out report.startTime);
                                int.TryParse(reader["clan_1_stars"].ToString(), out report.clan1Stars);
                                int.TryParse(reader["clan_2_stars"].ToString(), out report.clan2Stars);
                                int.TryParse(reader["war_size"].ToString(), out report.size);
                                report.maxStars = report.size * 3;
                                path = reader["report_path"].ToString();
                                report.hasReport = !string.IsNullOrEmpty(path);
                            }
                        }
                    }
                }
                if (report != null)
                {
                    if (report.hasReport && File.Exists(path))
                    {
                        try
                        {
                            string data = File.ReadAllText(path);
                            Data.ClanWarData savedReport = Data.Desrialize<Data.ClanWarData>(data);
                            /*
                            report.clan1 = GetClan(connection, report.clan1ID);
                            report.clan2 = GetClan(connection, report.clan2ID);

                            if(report.clan1 != null)
                            {
                                savedReport.clan1.name = report.clan1.name;
                                savedReport.clan1.backgroundColor = report.clan1.backgroundColor;
                                savedReport.clan1.patternColor = report.clan1.patternColor;
                                savedReport.clan1.background = report.clan1.background;
                                savedReport.clan1.pattern = report.clan1.pattern;
                            }
                            if(report.clan2 != null)
                            {
                                savedReport.clan2.name = report.clan2.name;
                                savedReport.clan2.backgroundColor = report.clan2.backgroundColor;
                                savedReport.clan2.patternColor = report.clan2.patternColor;
                                savedReport.clan2.background = report.clan2.background;
                                savedReport.clan2.pattern = report.clan2.pattern;
                            }
                            */
                            report.clan1 = savedReport.clan1;
                            report.clan2 = savedReport.clan2;
                        }
                        catch (Exception)
                        {
                            report = null;
                        }
                    }
                    else
                    {
                        report = null;
                    }
                }
                connection.Close();
            }
            return report;
        }

        #endregion

        #region Buildings

        private async static Task<Data.Building> GetBuildingAsync(long id, long account)
        {
            Task<Data.Building> task = Task.Run(() =>
            {
                return Retry.Do(() => _GetBuildingAsync(id, account), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }
        
        private static Data.Building _GetBuildingAsync(long id, long account)
        {
            Data.Building building = null;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = String.Format("SELECT level, global_id FROM buildings WHERE id = {0} AND account_id = {1};", id, account);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            building = new Data.Building();
                            while (reader.Read())
                            {
                                building.id = (Data.BuildingID)Enum.Parse(typeof(Data.BuildingID), reader["global_id"].ToString());
                                int.TryParse(reader["level"].ToString(), out building.level);
                            }
                        }
                    }
                }
                connection.Close();
            }
            return building;
        }

        private static List<Data.Building> GetBuildingsByGlobalID(string globalID, long account, MySqlConnection connection)
        {
            List<Data.Building> buildings = new List<Data.Building>();
            string query = String.Format("SELECT buildings.level, buildings.global_id, server_buildings.capacity FROM buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level WHERE buildings.global_id = '{0}' AND buildings.account_id = {1};", globalID, account);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            Data.Building building = new Data.Building();
                            building.id = (Data.BuildingID)Enum.Parse(typeof(Data.BuildingID), reader["global_id"].ToString());
                            int.TryParse(reader["level"].ToString(), out building.level);
                            int.TryParse(reader["capacity"].ToString(), out building.capacity);
                            buildings.Add(building);
                        }
                    }
                }
            }
            return buildings;
        }

        private async static Task<List<Data.Building>> GetBuildingsAsync(long account)
        {
            Task<List<Data.Building>> task = Task.Run(() =>
            {
                List<Data.Building> buildings = null;
                buildings = Retry.Do(() => _GetBuildingsAsync(account), TimeSpan.FromSeconds(0.1), 1, false);
                if(buildings == null)
                {
                    buildings = new List<Data.Building>();
                }
                return buildings;

            });
            return await task;
        }

        private static List<Data.Building> _GetBuildingsAsync(long account)
        {
            List<Data.Building> data = new List<Data.Building>();
            using (MySqlConnection connection = GetMysqlConnection())
            {
                data = GetBuildings(connection, account);
                connection.Close();
            }
            return data;
        }

        private static List<Data.Building> GetBuildings(MySqlConnection connection, long account)
        {
            List<Data.Building> data = new List<Data.Building>();
            string query = String.Format("SELECT buildings.id, buildings.global_id, buildings.level, buildings.x_position, buildings.x_war, buildings.y_war, buildings.boost, buildings.gold_storage, buildings.elixir_storage, buildings.dark_elixir_storage, buildings.y_position, buildings.construction_time, buildings.is_constructing, buildings.construction_build_time, server_buildings.columns_count, server_buildings.rows_count, server_buildings.health, server_buildings.speed, server_buildings.radius, server_buildings.capacity, server_buildings.gold_capacity, server_buildings.elixir_capacity, server_buildings.dark_elixir_capacity, server_buildings.damage, server_buildings.target_type, server_buildings.blind_radius, server_buildings.splash_radius, server_buildings.projectile_speed FROM buildings LEFT JOIN server_buildings ON buildings.global_id = server_buildings.global_id AND buildings.level = server_buildings.level WHERE buildings.account_id = {0};", account);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            Data.Building building = new Data.Building();
                            building.id = (Data.BuildingID)Enum.Parse(typeof(Data.BuildingID), reader["global_id"].ToString());
                            long.TryParse(reader["id"].ToString(), out building.databaseID);
                            int.TryParse(reader["level"].ToString(), out building.level);
                            int.TryParse(reader["x_position"].ToString(), out building.x);
                            int.TryParse(reader["y_position"].ToString(), out building.y);
                            int.TryParse(reader["x_war"].ToString(), out building.warX);
                            int.TryParse(reader["y_war"].ToString(), out building.warY);
                            int.TryParse(reader["columns_count"].ToString(), out building.columns);
                            int.TryParse(reader["rows_count"].ToString(), out building.rows);

                            float storage = 0;
                            float.TryParse(reader["gold_storage"].ToString(), out storage);
                            building.goldStorage = (int)Math.Floor(storage);

                            storage = 0;
                            float.TryParse(reader["elixir_storage"].ToString(), out storage);
                            building.elixirStorage = (int)Math.Floor(storage);

                            storage = 0;
                            float.TryParse(reader["dark_elixir_storage"].ToString(), out storage);
                            building.darkStorage = (int)Math.Floor(storage);

                            DateTime.TryParse(reader["boost"].ToString(), out building.boost);
                            float.TryParse(reader["damage"].ToString(), out building.damage);
                            int.TryParse(reader["capacity"].ToString(), out building.capacity);
                            int.TryParse(reader["gold_capacity"].ToString(), out building.goldCapacity);
                            int.TryParse(reader["elixir_capacity"].ToString(), out building.elixirCapacity);
                            int.TryParse(reader["dark_elixir_capacity"].ToString(), out building.darkCapacity);
                            float.TryParse(reader["speed"].ToString(), out building.speed);
                            float.TryParse(reader["radius"].ToString(), out building.radius);
                            int.TryParse(reader["health"].ToString(), out building.health);
                            DateTime.TryParse(reader["construction_time"].ToString(), out building.constructionTime);
                            float.TryParse(reader["blind_radius"].ToString(), out building.blindRange);
                            float.TryParse(reader["splash_radius"].ToString(), out building.splashRange);
                            float.TryParse(reader["projectile_speed"].ToString(), out building.rangedSpeed);
                            string tt = reader["target_type"].ToString();
                            if (!string.IsNullOrEmpty(tt))
                            {
                                building.targetType = (Data.BuildingTargetType)Enum.Parse(typeof(Data.BuildingTargetType), tt);
                            }
                            int isConstructing = 0;
                            int.TryParse(reader["is_constructing"].ToString(), out isConstructing);
                            building.isConstructing = isConstructing > 0;
                            int.TryParse(reader["construction_build_time"].ToString(), out building.buildTime);
                            data.Add(building);
                        }
                    }
                }
            }
            return data;
        }

        #endregion

        #region Build And Replace

        public async static void PlaceBuilding(int id, string device, string buildingID, int x, int y, int layout, long layoutID)
        {
            long account_id = Server.clients[id].account;
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.BUILD);
            Data.ServerBuilding building = await GetServerBuildingAsync(buildingID, 1);
            List<Data.Building> buildings = await GetBuildingsAsync(account_id);
            bool canPlaceBuilding = true;
            if (x < 0 || y < 0 || x + building.columns > Data.gridSize || y + building.rows > Data.gridSize)
            {
                canPlaceBuilding = false;
            }
            else
            {
                for (int i = 0; i < buildings.Count; i++)
                {
                    int bX = (layout == 2) ? buildings[i].warX : buildings[i].x;
                    int bY = (layout == 2) ? buildings[i].warY : buildings[i].y;
                    Rectangle rect1 = new Rectangle(bX, bY, buildings[i].columns, buildings[i].rows);
                    Rectangle rect2 = new Rectangle(x, y, building.columns, building.rows);
                    if (rect2.IntersectsWith(rect1))
                    {
                        canPlaceBuilding = false;
                        break;
                    }
                }
            }
            if (canPlaceBuilding)
            {
                int response = await PlaceBuildingAsync(account_id, building, x, y, layout, layoutID);
                packet.Write(response);
            }
            else
            {
                packet.Write(4);
            }
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> PlaceBuildingAsync(long account_id, Data.ServerBuilding building, int x, int y, int layout, long layoutID)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _PlaceBuildingAsync(account_id, building, x, y, layout, layoutID), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _PlaceBuildingAsync(long account_id, Data.ServerBuilding building, int x, int y, int layout, long layoutID)
        {
                int response = 0;
                using (MySqlConnection connection = GetMysqlConnection())
                {
                    if (layout == 2)
                    {
                        long war_id = 0;
                        string query = String.Format("SELECT war_id FROM accounts WHERE id = {0};", account_id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        long.TryParse(reader["war_id"].ToString(), out war_id);
                                    }
                                }
                            }
                        }
                        if (war_id > 0)
                        {
                            int war_stage = 0;
                            query = String.Format("SELECT stage FROM clan_wars WHERE id = {0};", war_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                using (MySqlDataReader reader = command.ExecuteReader())
                                {
                                    if (reader.HasRows)
                                    {
                                        while (reader.Read())
                                        {
                                            int.TryParse(reader["stage"].ToString(), out war_stage);
                                        }
                                    }
                                }
                            }
                            if (war_stage == 1)
                            {
                                query = String.Format("UPDATE buildings SET x_war = {0}, y_war = {1} WHERE id = {2}", x, y, layoutID);
                                using (MySqlCommand command = new MySqlCommand(query, connection))
                                {
                                    command.ExecuteNonQuery();
                                    response = 1;
                                }
                            }
                        }
                    }
                    else
                    {
                        if (building.id == "buildershut")
                        {
                            int c = GetBuildingCount(account_id, "buildershut", connection);
                            switch (c)
                            {
                                case 0: building.requiredGems = 0; break;
                                case 1: building.requiredGems = 250; break;
                                case 2: building.requiredGems = 500; break;
                                case 3: building.requiredGems = 1000; break;
                                case 4: building.requiredGems = 2000; break;
                                default: building.requiredGems = 999999; break;
                            }
                        }
                        int time = 0;
                        bool haveBuilding = false;
                        string query = String.Format("SELECT build_time FROM server_buildings WHERE global_id = '{0}' AND level = 1;", building.id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    haveBuilding = true;
                                    while (reader.Read())
                                    {
                                        time = int.Parse(reader["build_time"].ToString());
                                    }
                                }
                            }
                        }
                        if (haveBuilding)
                        {
                            int buildersCount = GetBuildingCount(account_id, "buildershut", connection);
                            int constructingCount = GetBuildingConstructionCount(account_id, connection);
                            if (time > 0 && buildersCount <= constructingCount)
                            {
                                response = 5;
                            }
                            else
                            {
                                bool limited = false;
                                Data.Building townHall = GetBuildingsByGlobalID("townhall", account_id, connection)[0];
                                if (building.id == "townhall")
                                {

                                }
                                else
                                {
                                    Data.BuildingCount limits = Data.GetBuildingLimits(townHall.level, building.id);
                                    int haveCount = GetBuildingCount(account_id, building.id, connection);
                                    if (limits == null || haveCount >= limits.count)
                                    {
                                        limited = true;
                                    }
                                }
                                if (limited)
                                {
                                    response = 6;
                                }
                                else
                                {
                                    if (SpendResources(connection, account_id, building.requiredGold, building.requiredElixir, building.requiredGems, building.requiredDarkElixir))
                                    {
                                        if (time > 0)
                                        {
                                            query = String.Format("INSERT INTO buildings (global_id, account_id, x_position, y_position, level, is_constructing, construction_time, construction_build_time, track_time) VALUES('{0}', {1}, {2}, {3}, 0, 1, NOW() + INTERVAL {4} SECOND, {5}, NOW() - INTERVAL 1 HOUR);", building.id, account_id, x, y, time, time);
                                        }
                                        else
                                        {
                                            query = String.Format("INSERT INTO buildings (global_id, account_id, x_position, y_position, level, is_constructing, track_time) VALUES('{0}', {1}, {2}, {3}, 1, 0, NOW() - INTERVAL 1 HOUR);", building.id, account_id, x, y);
                                            AddXP(connection, account_id, building.gainedXp);
                                        }
                                        using (MySqlCommand command = new MySqlCommand(query, connection))
                                        {
                                            command.ExecuteNonQuery();
                                            response = 1;
                                        }
                                    }
                                    else
                                    {
                                        response = 2;
                                    }
                                }
                            }
                        }
                        else
                        {
                            response = 3;
                        }
                    }
                    connection.Close();
                }
                return response;
        }

        public async static void ReplaceBuilding(int id, long databaseID, int x, int y, int layout)
        {
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.REPLACE);
            long account_id = Server.clients[id].account;
            List<Data.Building> buildings = await GetBuildingsAsync(account_id);
            Data.Building building = null;

            if (buildings != null && buildings.Count > 0)
            {
                for (int i = 0; i < buildings.Count; i++)
                {
                    if (buildings[i].databaseID == databaseID)
                    {
                        building = buildings[i];
                        break;
                    }
                }
            }

            if (building != null)
            {
                bool canPlaceBuilding = true;
                if (x < 0 || y < 0 || x + building.columns > Data.gridSize || y + building.rows > Data.gridSize)
                {
                    canPlaceBuilding = false;
                }
                else
                {
                    for (int i = 0; i < buildings.Count; i++)
                    {
                        if (buildings[i].databaseID != building.databaseID)
                        {
                            int bX = (layout == 2) ? buildings[i].warX : buildings[i].x;
                            int bY = (layout == 2) ? buildings[i].warY : buildings[i].y;
                            Rectangle rect1 = new Rectangle(bX, bY, buildings[i].columns, buildings[i].rows);
                            Rectangle rect2 = new Rectangle(x, y, building.columns, building.rows);
                            if (rect2.IntersectsWith(rect1))
                            {
                                canPlaceBuilding = false;
                                break;
                            }
                        }
                    }
                }
                if (canPlaceBuilding)
                {
                    bool done = await ReplaceBuildingAsync(account_id, databaseID, x, y, layout);
                    if (done)
                    {
                        packet.Write(1);
                    }
                    else
                    {
                        packet.Write(3);
                    }
                }
                else
                {
                    packet.Write(2);
                }
            }
            else
            {
                packet.Write(0);
            }
            packet.Write(x);
            packet.Write(y);
            packet.Write(databaseID);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<bool> ReplaceBuildingAsync(long account_id, long building_id, int x, int y, int layout)
        {
            Task<bool> task = Task.Run(() =>
            {
                return Retry.Do(() => _ReplaceBuildingAsync(account_id, building_id, x, y, layout), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static bool _ReplaceBuildingAsync(long account_id, long building_id, int x, int y, int layout)
        {
            bool response = false;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = "";
                if (layout == 2)
                {
                    long war_id = 0;
                    query = String.Format("SELECT war_id FROM accounts WHERE id = {0};", account_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    long.TryParse(reader["war_id"].ToString(), out war_id);
                                }
                            }
                        }
                    }
                    if (war_id > 0)
                    {
                        int war_stage = 0;
                        query = String.Format("SELECT stage FROM clan_wars WHERE id = {0};", war_id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        int.TryParse(reader["stage"].ToString(), out war_stage);
                                    }
                                }
                            }
                        }
                        if (war_stage == 1)
                        {
                            query = String.Format("UPDATE buildings SET x_war = {0}, y_war = {1} WHERE id = {2};", x, y, building_id);
                        }
                        else
                        {
                            query = "";
                        }
                    }
                    else
                    {
                        query = "";
                    }
                }
                else
                {
                    query = String.Format("UPDATE buildings SET x_position = {0}, y_position = {1} WHERE id = {2};", x, y, building_id);
                }
                if (!string.IsNullOrEmpty(query))
                {
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    response = true;
                }
                connection.Close();
            }
            return response;
        }

        public async static void GetNextLevelRequirements(int id, long databaseID)
        {
            long account_id = Server.clients[id].account;
            Data.Building building = await GetBuildingAsync(databaseID, account_id);
            if (building != null)
            {
                Data.ServerBuilding b = await GetServerBuildingAsync(building.id.ToString(), building.level + 1);
                string data = await Data.SerializeAsync<Data.ServerBuilding>(b);
                Packet packet = new Packet();
                packet.Write((int)Terminal.RequestsID.PREUPGRADE);
                packet.Write(databaseID);
                packet.Write(data);
                Sender.TCP_Send(id, packet);

            }
        }

        public async static void UpgradeBuilding(int id, long buildingID)
        {
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.UPGRADE);
            long account_id = Server.clients[id].account;
            Data.Building building = await GetBuildingAsync(buildingID, account_id);
            if (building == null)
            {
                packet.Write(0);
            }
            else
            {
                int response = await UpgradeBuildingAsync(account_id, buildingID, building.level, building.id.ToString());
                packet.Write(response);
            }
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> UpgradeBuildingAsync(long account_id, long buildingID, int level, string globalID)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _UpgradeBuildingAsync(account_id, buildingID, level, globalID), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _UpgradeBuildingAsync(long account_id, long buildingID, int level, string globalID)
        {
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                int time = 0;
                bool haveLevel = false;
                int reqGold = 0;
                int reqElixir = 0;
                int reqDarkElixir = 0;
                int reqGems = 0;
                string query = String.Format("SELECT req_gold, req_elixir, req_dark_elixir, req_gems, build_time FROM server_buildings WHERE global_id = '{0}' AND level = {1};", globalID, level + 1);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                haveLevel = true;
                                time = int.Parse(reader["build_time"].ToString());
                                reqGold = int.Parse(reader["req_gold"].ToString());
                                reqElixir = int.Parse(reader["req_elixir"].ToString());
                                reqDarkElixir = int.Parse(reader["req_dark_elixir"].ToString());
                                reqGems = int.Parse(reader["req_gems"].ToString());
                            }
                        }
                    }
                }

                if (haveLevel)
                {
                    int buildersCount = GetBuildingCount(account_id, "buildershut", connection);
                    int constructingCount = GetBuildingConstructionCount(account_id, connection);
                    if (time > 0 && buildersCount <= constructingCount)
                    {
                        response = 5;
                    }
                    else
                    {
                        bool limited = false;
                        Data.Building townHall = GetBuildingsByGlobalID("townhall", account_id, connection)[0];
                        if (globalID == "townhall")
                        {

                        }
                        else
                        {
                            Data.BuildingCount limits = Data.GetBuildingLimits(townHall.level, globalID);
                            int haveCount = GetBuildingCount(account_id, globalID, connection);
                            if (haveCount >= limits.count && level >= limits.maxLevel)
                            {
                                limited = true;
                            }
                        }
                        if (limited)
                        {
                            response = 6;
                        }
                        else
                        {
                            if (SpendResources(connection, account_id, reqGold, reqElixir, reqGems, reqDarkElixir))
                            {
                                query = String.Format("UPDATE buildings SET is_constructing = 1, construction_time =  NOW() + INTERVAL {0} SECOND, construction_build_time = {1} WHERE id = {2};", time, time, buildingID);
                                using (MySqlCommand command = new MySqlCommand(query, connection))
                                {
                                    command.ExecuteNonQuery();
                                    response = 1;
                                }
                            }
                            else
                            {
                                response = 2;
                            }
                        }
                    }
                }
                else
                {
                    response = 3;
                }
                connection.Close();
            }
            return response;
        }

        public async static void InstantBuild(int id, long buildingID)
        {
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.INSTANTBUILD);
            long account_id = Server.clients[id].account;
            Data.Building building = await GetBuildingAsync(buildingID, account_id);
            if (building == null)
            {
                packet.Write(0);
            }
            else
            {
                int res = await InstantBuildAsync(account_id, buildingID, building.level, building.id.ToString());
                packet.Write(res);
            }
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> InstantBuildAsync(long account_id, long buildingID, int level, string globalID)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _InstantBuildAsync(account_id, buildingID, level, globalID), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _InstantBuildAsync(long account_id, long buildingID, int level, string globalID)
        {
            int id = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                int time = 0;
                string query = String.Format("SELECT construction_time, NOW() AS now_time FROM buildings WHERE id = {0} AND account_id = {1} AND is_constructing > 0;", buildingID, account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                DateTime target = DateTime.Parse(reader["construction_time"].ToString());
                                DateTime now = DateTime.Parse(reader["now_time"].ToString());
                                if (target > now)
                                {
                                    time = (int)(target - now).TotalSeconds;
                                }
                            }
                        }
                    }
                }
                if (time > 0)
                {
                    int requiredGems = Data.GetInstantBuildRequiredGems(time);
                    if (SpendResources(connection, account_id, 0, 0, requiredGems, 0))
                    {
                        query = String.Format("UPDATE buildings SET construction_time = NOW() WHERE id = {0}", buildingID);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                            id = 1;
                        }
                    }
                    else
                    {
                        id = 2;
                    }
                }
                connection.Close();
            }
            return id;
        }

        #endregion

        #region Units

        private async static Task<List<Data.Unit>> GetUnitsAsync(long account_id)
        {
            Task<List<Data.Unit>> task = Task.Run(() =>
            {
                List<Data.Unit> units = null;
                units = Retry.Do(() => _GetUnitsAsync(account_id), TimeSpan.FromSeconds(0.1), 10, false);
                if (units == null)
                {
                    units = new List<Data.Unit>();
                }
                return units;
            });
            return await task;
        }

        private static List<Data.Unit> _GetUnitsAsync(long account_id)
        {
            List<Data.Unit> units = new List<Data.Unit>();
            using (MySqlConnection connection = GetMysqlConnection())
            {
                units = GetUnits(account_id, connection);
                connection.Close();
            }
            return units;
        }

        private static List<Data.Unit> GetUnits(long account, MySqlConnection connection)
        {
            List<Data.Unit> data = new List<Data.Unit>();
            string query = String.Format("SELECT units.id, units.global_id, units.level, units.trained, units.ready, units.trained_time, server_units.health, server_units.train_time, server_units.housing, server_units.attack_range, server_units.attack_speed, server_units.move_speed, server_units.damage, server_units.move_type, server_units.target_priority, server_units.priority_multiplier FROM units LEFT JOIN server_units ON units.global_id = server_units.global_id AND units.level = server_units.level WHERE units.account_id = {0};", account);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            Data.Unit unit = new Data.Unit();
                            unit.id = (Data.UnitID)Enum.Parse(typeof(Data.UnitID), reader["global_id"].ToString());
                            long.TryParse(reader["id"].ToString(), out unit.databaseID);
                            int.TryParse(reader["level"].ToString(), out unit.level);
                            int.TryParse(reader["health"].ToString(), out unit.health);
                            int.TryParse(reader["housing"].ToString(), out unit.hosing);
                            int.TryParse(reader["train_time"].ToString(), out unit.trainTime);
                            float.TryParse(reader["trained_time"].ToString(), out unit.trainedTime);

                            float.TryParse(reader["damage"].ToString(), out unit.damage);
                            float.TryParse(reader["attack_speed"].ToString(), out unit.attackSpeed);
                            float.TryParse(reader["move_speed"].ToString(), out unit.moveSpeed);
                            float.TryParse(reader["attack_range"].ToString(), out unit.attackRange);

                            unit.movement = (Data.UnitMoveType)Enum.Parse(typeof(Data.UnitMoveType), reader["move_type"].ToString());
                            unit.priority = (Data.TargetPriority)Enum.Parse(typeof(Data.TargetPriority), reader["target_priority"].ToString());
                            float.TryParse(reader["priority_multiplier"].ToString(), out unit.priorityMultiplier);

                            int isTrue = 0;
                            int.TryParse(reader["trained"].ToString(), out isTrue);
                            unit.trained = isTrue > 0;

                            isTrue = 0;
                            int.TryParse(reader["ready"].ToString(), out isTrue);
                            unit.ready = isTrue > 0;
                            data.Add(unit);
                        }
                    }
                }
            }
            return data;
        }

        public async static void TrainUnit(int id, string globalID)
        {
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.TRAIN);
            long account_id = Server.clients[id].account;
            int res = await TrainUnitAsync(account_id, 1, globalID);
            packet.Write(res);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> TrainUnitAsync(long account_id, int level, string globalID)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _TrainUnitAsync(account_id, level, globalID), TimeSpan.FromSeconds(0.1), 10, false);
            });
            return await task;
        }

        private static int _TrainUnitAsync(long account_id, int level, string globalID)
        {
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                Data.ServerUnit unit = GetServerUnit(connection, globalID, level);
                if (unit != null)
                {
                    int capacity = 0;
                    List<Data.Building> barracks = GetBuildingsByGlobalID(Data.BuildingID.barracks.ToString(), account_id, connection);
                    for (int i = 0; i < barracks.Count; i++)
                    {
                        capacity += barracks[i].capacity;
                    }

                    int occupied = 999;
                    string query = String.Format("SELECT SUM(server_units.housing) AS occupied FROM units LEFT JOIN server_units ON units.global_id = server_units.global_id AND units.level = server_units.level WHERE units.account_id = {0} AND ready <= 0;", account_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    int.TryParse(reader["occupied"].ToString(), out occupied);
                                }
                            }
                        }
                    }

                    if (capacity - occupied >= unit.housing)
                    {
                        if (SpendResources(connection, account_id, unit.requiredGold, unit.requiredElixir, unit.requiredGems, unit.requiredDarkElixir))
                        {
                            query = String.Format("INSERT INTO units (global_id, level, account_id) VALUES('{0}', {1}, {2})", globalID, level, account_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                                response = 1;
                            }
                        }
                        else
                        {
                            response = 2;
                        }
                    }
                    else
                    {
                        response = 4;
                    }
                }
                else
                {
                    response = 3;
                }
                connection.Close();
            }
            return response;
        }

        private static List<Data.ServerUnit> GetServerUnits(MySqlConnection connection)
        {
            List<Data.ServerUnit> units = new List<Data.ServerUnit>();
            string query = String.Format("SELECT global_id, level, req_gold, req_elixir, req_gem, req_dark_elixir, train_time, health, housing FROM server_units;");
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            Data.ServerUnit unit = new Data.ServerUnit();
                            unit.id = (Data.UnitID)Enum.Parse(typeof(Data.UnitID), reader["global_id"].ToString());
                            int.TryParse(reader["level"].ToString(), out unit.level);
                            int.TryParse(reader["req_gold"].ToString(), out unit.requiredGold);
                            int.TryParse(reader["req_elixir"].ToString(), out unit.requiredElixir);
                            int.TryParse(reader["req_gem"].ToString(), out unit.requiredGems);
                            int.TryParse(reader["req_dark_elixir"].ToString(), out unit.requiredDarkElixir);
                            int.TryParse(reader["train_time"].ToString(), out unit.trainTime);
                            int.TryParse(reader["health"].ToString(), out unit.health);
                            int.TryParse(reader["housing"].ToString(), out unit.housing);
                            units.Add(unit);
                        }
                    }
                }
            }
            return units;
        }

        private static Data.ServerUnit GetServerUnit(MySqlConnection connection, string id, int level)
        {
            Data.ServerUnit unit = null;
            string query = String.Format("SELECT global_id, level, req_gold, req_elixir, req_gem, req_dark_elixir, train_time, health, housing FROM server_units WHERE global_id = '{0}' AND level = {1};", id, level);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            unit = new Data.ServerUnit();
                            unit.id = (Data.UnitID)Enum.Parse(typeof(Data.UnitID), reader["global_id"].ToString());
                            int.TryParse(reader["level"].ToString(), out unit.level);
                            int.TryParse(reader["req_gold"].ToString(), out unit.requiredGold);
                            int.TryParse(reader["req_elixir"].ToString(), out unit.requiredElixir);
                            int.TryParse(reader["req_gem"].ToString(), out unit.requiredGems);
                            int.TryParse(reader["req_dark_elixir"].ToString(), out unit.requiredDarkElixir);
                            int.TryParse(reader["train_time"].ToString(), out unit.trainTime);
                            int.TryParse(reader["health"].ToString(), out unit.health);
                            int.TryParse(reader["housing"].ToString(), out unit.housing);
                        }
                    }
                }
            }
            return unit;
        }

        public async static void CancelTrainUnit(int id, long databaseID)
        {
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.CANCELTRAIN);
            long account_id = Server.clients[id].account;
            int res = await CancelTrainUnitAsync(account_id, databaseID);
            packet.Write(res);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> CancelTrainUnitAsync(long account_id, long databaseID)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _CancelTrainUnitAsync(account_id, databaseID), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _CancelTrainUnitAsync(long account_id, long databaseID)
        {
            int id = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = String.Format("DELETE FROM units WHERE id = {0} AND account_id = {1} AND ready <= 0", databaseID, account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                    id = 1;
                }
                connection.Close();
            }
            return id;
        }

        public static void DeleteUnit(long id, MySqlConnection connection)
        {
            string query = String.Format("DELETE FROM units WHERE id = {0};", id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }
        }

        private static Data.Unit GetUnit(MySqlConnection connection, long database_id, long account_id)
        {
            Data.Unit unit = null;
            string query = String.Format("SELECT units.id, units.global_id, units.level, units.trained, units.ready, units.trained_time, server_units.health, server_units.train_time, server_units.housing, server_units.attack_range, server_units.attack_speed, server_units.move_speed, server_units.damage, server_units.move_type, server_units.target_priority, server_units.priority_multiplier FROM units LEFT JOIN server_units ON units.global_id = server_units.global_id AND units.level = server_units.level WHERE units.id = {0} AND units.account_id = {1};", database_id, account_id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            unit = new Data.Unit();
                            unit.id = (Data.UnitID)Enum.Parse(typeof(Data.UnitID), reader["global_id"].ToString());
                            long.TryParse(reader["id"].ToString(), out unit.databaseID);
                            int.TryParse(reader["level"].ToString(), out unit.level);
                            int.TryParse(reader["health"].ToString(), out unit.health);
                            int.TryParse(reader["housing"].ToString(), out unit.hosing);
                            int.TryParse(reader["train_time"].ToString(), out unit.trainTime);
                            float.TryParse(reader["trained_time"].ToString(), out unit.trainedTime);

                            float.TryParse(reader["damage"].ToString(), out unit.damage);
                            float.TryParse(reader["attack_speed"].ToString(), out unit.attackSpeed);
                            float.TryParse(reader["move_speed"].ToString(), out unit.moveSpeed);
                            float.TryParse(reader["attack_range"].ToString(), out unit.attackRange);

                            unit.movement = (Data.UnitMoveType)Enum.Parse(typeof(Data.UnitMoveType), reader["move_type"].ToString());
                            unit.priority = (Data.TargetPriority)Enum.Parse(typeof(Data.TargetPriority), reader["target_priority"].ToString());
                            float.TryParse(reader["priority_multiplier"].ToString(), out unit.priorityMultiplier);

                            int isTrue = 0;
                            int.TryParse(reader["trained"].ToString(), out isTrue);
                            unit.trained = isTrue > 0;

                            isTrue = 0;
                            int.TryParse(reader["ready"].ToString(), out isTrue);
                            unit.ready = isTrue > 0;
                        }
                    }
                }
            }
            return unit;
        }

        #endregion

        #region Battle

        private static List<Data.BattleData> battles = new List<Data.BattleData>();

        public async static void FindBattleTarget(int id)
        {
            long account_id = Server.clients[id].account;
            long target = await FindBattleTargetAsync(account_id);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.BATTLEFIND);
            packet.Write(target);
            if (target > 0)
            {
                Data.OpponentData opponent = new Data.OpponentData();
                opponent.id = target;
                opponent.buildings = await GetBuildingsAsync(target);
                Console.Clear();
                Console.WriteLine("Here: " + target + " -> " + opponent.buildings.Count);
                opponent.buildings = await SetBuildingsPercentAsync(opponent.buildings, Data.BattleType.normal);
                string data = await Data.SerializeAsync<Data.OpponentData>(opponent);
                packet.Write(data);
            }
            Sender.TCP_Send(id, packet);
        }

        private async static Task<long> FindBattleTargetAsync(long account_id)
        {
            Task<long> task = Task.Run(() =>
            {
                return Retry.Do(() => _FindBattleTargetAsync(account_id), TimeSpan.FromSeconds(0.1), 10, false);
            });
            return await task;
        }

        private static long _FindBattleTargetAsync(long account_id)
        {
            long id = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = String.Format("SELECT id FROM accounts WHERE id <> {0} AND shield < NOW() AND is_online <= 0 ORDER BY RAND() LIMIT 1;", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                long.TryParse(reader["id"].ToString(), out id);
                            }
                        }
                    }
                }
                connection.Close();
            }
            return id;
        }

        public async static void AddBattleFrame(int id, string data)
        {
            long account_id = Server.clients[id].account;
            Data.BattleFrame frame = await Data.DesrializeAsync<Data.BattleFrame>(data);
            for (int i = 0; i < battles.Count; i++)
            {
                if (battles[i].battle.attacker == account_id)
                {
                    double time = frame.frame * Data.battleFrameRate;
                    if (battles[i].battle.percentage < 1f && battles[i].battle.end == false && time <= battles[i].battle.duration)
                    {
                        battles[i].frames.Add(frame);
                    }
                    break;
                }
            }
        }

        public static void EndBattle(long account_id, bool surrender, int surrenderFrame)
        {
            for (int i = battles.Count - 1; i >= 0; i--)
            {
                if (battles[i].battle.attacker == account_id)
                {
                    battles[i].battle.surrender = surrender;
                    battles[i].battle.surrenderFrame = surrenderFrame;
                    battles[i].battle.end = true;
                    break;
                }
            }
        }

        private async static Task<List<Data.Building>> SetBuildingsPercentAsync(List<Data.Building> buildings, Data.BattleType battleType)
        {
            Task<List<Data.Building>> task = Task.Run(() =>
            {
                return Retry.Do(() => _SetBuildingsPercentAsync(buildings, battleType), TimeSpan.FromSeconds(0.1), 10, false);
            });
            return await task;
        }

        private static List<Data.Building> _SetBuildingsPercentAsync(List<Data.Building> buildings, Data.BattleType battleType)
        {
            double count = 0;
            for (int i = 0; i < buildings.Count; i++)
            {
                if (buildings[i].id != Data.BuildingID.wall && (battleType == Data.BattleType.war && buildings[i].warX < 0 && buildings[i].warY < 0) == false && Battle.IsBuildingCanBeAttacked(buildings[i].id))
                {
                    count += (buildings[i].rows * buildings[i].columns);
                }
            }
            for (int i = 0; i < buildings.Count; i++)
            {
                if (buildings[i].id != Data.BuildingID.wall && (battleType == Data.BattleType.war && buildings[i].warX < 0 && buildings[i].warY < 0) == false && Battle.IsBuildingCanBeAttacked(buildings[i].id))
                {
                    buildings[i].percentage = (double)(buildings[i].rows * buildings[i].columns) / count;
                }
                else
                {
                    buildings[i].percentage = 0;
                }
            }
            return buildings;
        }

        public async static void StartBattle(int id, string data, Data.BattleType type)
        {
            long account_id = Server.clients[id].account;
            bool match = true;
            bool canAttack = true;
            bool canWarAttack = true;

            if (type == Data.BattleType.war)
            {
                Data.Clan clan = await GetClanByAccountIdAsync(account_id);
                if (clan == null || clan.war == null)
                {
                    canWarAttack = false;
                }
                else
                {
                    int count = 0;
                    if (clan.war.attacks != null)
                    {
                        for (int i = 0; i < clan.war.attacks.Count; i++)
                        {
                            if (clan.war.attacks[i].attacker == account_id)
                            {
                                count++;
                            }
                        }
                    }
                    if (count >= Data.clanWarAttacksPerPlayer)
                    {
                        canWarAttack = false;
                    }
                }
            }

            List<Data.BattleStartBuildingData> startData = new List<Data.BattleStartBuildingData>();
            List<Battle.Building> buildings = new List<Battle.Building>();
            Data.OpponentData opponentClent = await Data.DesrializeAsync<Data.OpponentData>(data);
            long defender = opponentClent.id;

            for (int i = 0; i < battles.Count; i++)
            {
                if ((type == Data.BattleType.normal && (battles[i].battle.attacker == account_id || battles[i].battle.defender == defender)) || (type == Data.BattleType.war && battles[i].battle.attacker == account_id))
                {
                    canAttack = false;
                    break;
                }
            }

            if (type == Data.BattleType.war && !canWarAttack)
            {
                canAttack = false;
            }

            if (canAttack)
            {
                Data.OpponentData opponentServer = new Data.OpponentData();
                opponentServer.buildings = await GetBuildingsAsync(opponentClent.id);
                opponentServer.buildings = await SetBuildingsPercentAsync(opponentServer.buildings, type);

                if (opponentServer.buildings.Count == opponentClent.buildings.Count)
                {
                    int townhallLevel = 1;
                    for (int i = 0; i < opponentServer.buildings.Count; i++)
                    {
                        if (opponentServer.buildings[i].id == Data.BuildingID.townhall)
                        {
                            townhallLevel = opponentServer.buildings[i].level;
                            break;
                        }
                    }

                    for (int i = 0; i < opponentServer.buildings.Count; i++)
                    {
                        if (opponentServer.buildings[i].databaseID != opponentClent.buildings[i].databaseID || opponentServer.buildings[i].id != opponentClent.buildings[i].id || opponentServer.buildings[i].health != opponentClent.buildings[i].health || opponentServer.buildings[i].damage != opponentClent.buildings[i].damage || opponentServer.buildings[i].percentage != opponentClent.buildings[i].percentage)
                        {
                            match = false;
                            break;
                        }

                        Battle.Building building = new Battle.Building();
                        building.building = opponentServer.buildings[i];
                        if (type == Data.BattleType.war)
                        {
                            building.building.x = building.building.warX;
                            building.building.y = building.building.warY;
                        }

                        if (building.building.x < 0 || building.building.y < 0)
                        {
                            continue;
                        }

                        building.building.x += Data.battleGridOffset;
                        building.building.y += Data.battleGridOffset;

                        bool storage = false;
                        switch (building.building.id)
                        {
                            case Data.BuildingID.townhall:
                                building.lootGoldStorage = Data.GetStorageGoldAndElixirLoot(townhallLevel, building.building.goldStorage);
                                building.lootElixirStorage = Data.GetStorageGoldAndElixirLoot(townhallLevel, building.building.elixirStorage);
                                building.lootDarkStorage = Data.GetStorageDarkElixirLoot(townhallLevel, building.building.darkStorage);
                                storage = true;
                                break;
                            case Data.BuildingID.goldmine:
                                building.lootGoldStorage = Data.GetMinesGoldAndElixirLoot(townhallLevel, building.building.goldStorage);
                                storage = true;
                                break;
                            case Data.BuildingID.goldstorage:
                                building.lootGoldStorage = Data.GetStorageGoldAndElixirLoot(townhallLevel, building.building.goldStorage);
                                storage = true;
                                break;
                            case Data.BuildingID.elixirmine:
                                building.lootElixirStorage = Data.GetMinesGoldAndElixirLoot(townhallLevel, building.building.elixirStorage);
                                storage = true;
                                break;
                            case Data.BuildingID.elixirstorage:
                                building.lootElixirStorage = Data.GetStorageGoldAndElixirLoot(townhallLevel, building.building.elixirStorage);
                                storage = true;
                                break;
                            case Data.BuildingID.darkelixirmine:
                                building.lootDarkStorage = Data.GetMinesDarkElixirLoot(townhallLevel, building.building.darkStorage);
                                storage = true;
                                break;
                            case Data.BuildingID.darkelixirstorage:
                                building.lootDarkStorage = Data.GetStorageDarkElixirLoot(townhallLevel, building.building.darkStorage);
                                storage = true;
                                break;
                        }
                        /*
                        if (storage)
                        {
                            Data.BattleStartBuildingData st = new Data.BattleStartBuildingData();
                            st.id = building.building.id;
                            st.databaseID = building.building.databaseID;
                            st.lootGoldStorage = building.building.goldStorage;
                            st.lootElixirStorage = building.building.elixirStorage;
                            st.lootDarkStorage = building.building.darkStorage;
                            startData.Add(st);
                        }
                        */
                        buildings.Add(building);
                    }
                }
                else
                {
                    match = false;
                }
            }
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.BATTLESTART);
            packet.Write(match);
            packet.Write(canAttack);
            if (match && canAttack)
            {
                Data.Player attackerData = await GetPlayerDataAsync(account_id);
                Data.Player defenderData = await GetPlayerDataAsync(defender);
                var trophies = Data.GetBattleTrophies(attackerData.trophies, defenderData.trophies);
                Data.BattleData battle = new Data.BattleData();
                battle.type = type;
                battle.battle = new Battle();
                battle.battle.Initialize(buildings, DateTime.Now);
                battle.battle.attacker = account_id;
                battle.battle.defender = defender;
                battle.battle.winTrophies = trophies.Item1;
                battle.battle.loseTrophies = trophies.Item2;
                battles.Add(battle);
                packet.Write(battle.battle.winTrophies);
                packet.Write(battle.battle.loseTrophies);
                string bd = await Data.SerializeAsync<List<Data.BattleStartBuildingData>>(startData);
                packet.Write(bd);
            }
            Sender.TCP_Send(id, packet);
        }

        #endregion

        #region Clan

        public async static void CreateClan(int id, string name, int minTrophies, int minTownhallLevel, int pattern, int background, string patternColor, string backgroundColor, int joinType)
        {
            long account_id = Server.clients[id].account;
            int res = await CreateClanAsync(account_id, name, minTrophies, minTownhallLevel, pattern, background, patternColor, backgroundColor, joinType);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.CREATECLAN);
            packet.Write(res);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> CreateClanAsync(long account_id, string name, int minTrophies, int minTownhallLevel, int pattern, int background, string patternColor, string backgroundColor, int joinType)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _CreateClanAsync(account_id, name, minTrophies, minTownhallLevel, pattern, background, patternColor, backgroundColor, joinType), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _CreateClanAsync(long account_id, string name, int minTrophies, int minTownhallLevel, int pattern, int background, string patternColor, string backgroundColor, int joinType)
        {
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = String.Format("SELECT clan_id FROM accounts WHERE id = {0} AND clan_join_timer <= NOW();", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                int id = 0;
                                int.TryParse(reader["clan_id"].ToString(), out id);
                                if (id > 0)
                                {
                                    // Already in a clan
                                    response = 2;
                                }
                            }
                        }
                        else
                        {
                            // Time limit
                            response = 3;
                        }
                    }
                }
                if (response == 0)
                {
                    if (SpendResources(connection, account_id, Data.clanCreatePrice, 0, 0, 0))
                    {
                        long clan_id = 0;
                        query = String.Format("INSERT INTO clans (name, min_trophies, min_townhall_level, pattern, background, pattern_color, background_color, join_type) VALUES('{0}', {1}, {2}, {3}, {4}, '{5}', '{6}', {7})", name, minTrophies, minTownhallLevel, pattern, background, patternColor, backgroundColor, joinType);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                            clan_id = command.LastInsertedId;
                        }
                        if (clan_id > 0)
                        {
                            query = String.Format("UPDATE accounts SET clan_join_timer = NOW() + INTERVAL {0} HOUR, clan_id = {1}, clan_rank = 1, clan_chat_blocked = 0 WHERE id = {2}", Data.clanJoinTimeGapHours, clan_id, account_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                            response = 1;
                        }
                    }
                    else
                    {
                        // No resources
                        response = 4;
                    }
                }
                connection.Close();
            }
            return response;
        }

        private async static Task<Data.Clan> GetClanAsync(long clan_id)
        {
            Task<Data.Clan> task = Task.Run(() =>
            {
                return Retry.Do(() => _GetClanAsync(clan_id), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static Data.Clan _GetClanAsync(long clan_id)
        {
            Data.Clan response = null;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                response = GetClan(connection, clan_id);
                connection.Close();
            }
            return response;
        }

        private async static Task<List<Data.ClanMember>> GetClanMembersAsync(long clan_id)
        {
            Task<List<Data.ClanMember>> task = Task.Run(() =>
            {
                List<Data.ClanMember> response = null;
                response = Retry.Do(() => _GetClanMembersAsync(clan_id), TimeSpan.FromSeconds(0.1), 10, false);
                if (response == null)
                {
                    response = new List<Data.ClanMember>();
                }
                return response;
            });
            return await task;
        }

        private static List<Data.ClanMember> _GetClanMembersAsync(long clan_id)
        {
            List<Data.ClanMember> response = new List<Data.ClanMember>();
            using (MySqlConnection connection = GetMysqlConnection())
            {
                response = GetClanMembers(connection, clan_id);
                connection.Close();
            }
            return response;
        }

        public async static void OpenClan(int id, long account_id = 0, long clan_id = 0)
        {
            if (clan_id <= 0)
            {
                if (account_id <= 0)
                {
                    account_id = Server.clients[id].account;
                }
                Data.Player player = await GetPlayerDataAsync(account_id);
                clan_id = player.clanID;
            }
            Data.Clan clan = await GetClanAsync(clan_id);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.OPENCLAN);
            if (clan != null)
            {
                packet.Write(true);
                string clanData = await Data.SerializeAsync<Data.Clan>(clan);
                packet.Write(clanData);
                if (clan.war.id > 0)
                {
                    List<Data.ClanMember> enemies = await GetClanMembersAsync(clan.war.clan1 == clan.id ? clan.war.clan2 : clan.war.clan1);
                    string membersData = await Data.SerializeAsync<List<Data.ClanMember>>(enemies);
                    packet.Write(membersData);
                }
            }
            else
            {
                packet.Write(false);
            }
            Sender.TCP_Send(id, packet);
        }

        public async static void JoinClan(int id, long clanID)
        {
            long account_id = Server.clients[id].account;
            int response = await JoinClanAsync(account_id, clanID);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.JOINCLAN);
            packet.Write(response);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> JoinClanAsync(long account_id, long id)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _JoinClanAsync(account_id, id), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _JoinClanAsync(long account_id, long id)
        {
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                int townHallLevel = 1;
                int trophies = 1;
                string query = String.Format("SELECT accounts.clan_id, accounts.trophies, buildings.level FROM accounts LEFT JOIN buildings ON buildings.account_id = accounts.id AND buildings.global_id = '{0}' WHERE accounts.id = {1} AND accounts.clan_join_timer <= NOW();", Data.BuildingID.townhall.ToString(), account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                int clan_id = 0;
                                int.TryParse(reader["clan_id"].ToString(), out clan_id);
                                int.TryParse(reader["trophies"].ToString(), out trophies);
                                int.TryParse(reader["level"].ToString(), out townHallLevel);
                                if (clan_id > 0)
                                {
                                    // Already in a clan
                                    response = 2;
                                }
                            }
                        }
                        else
                        {
                            // Time limit
                            response = 3;
                        }
                    }
                }
                if (response == 0)
                {
                    int join_type = 0;
                    query = String.Format("SELECT join_type, min_trophies, min_townhall_level FROM clans WHERE id = {0};", id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    int min_townhall_level = 0;
                                    int min_trophies = 0;
                                    int.TryParse(reader["join_type"].ToString(), out join_type);
                                    int.TryParse(reader["min_trophies"].ToString(), out min_trophies);
                                    int.TryParse(reader["min_townhall_level"].ToString(), out min_townhall_level);
                                    if (townHallLevel < min_townhall_level || trophies < min_trophies)
                                    {
                                        // Not meeting min
                                        response = 4;
                                    }
                                }
                            }
                            else
                            {
                                // No clan
                                response = 5;
                            }
                        }
                    }
                    if (response == 0)
                    {
                        switch ((Data.ClanJoinType)join_type)
                        {
                            case Data.ClanJoinType.AnyoneCanJoin:
                                query = String.Format("SELECT COUNT(id) AS members FROM accounts WHERE clan_id = {0};", id);
                                using (MySqlCommand command = new MySqlCommand(query, connection))
                                {
                                    using (MySqlDataReader reader = command.ExecuteReader())
                                    {
                                        if (reader.HasRows)
                                        {
                                            while (reader.Read())
                                            {
                                                int count = Data.clanMaxMembers;
                                                int.TryParse(reader["members"].ToString(), out count);
                                                if (count >= Data.clanMaxMembers)
                                                {
                                                    // Max members
                                                    response = 6;
                                                }
                                            }
                                        }
                                    }
                                }
                                if (response == 0)
                                {
                                    JoinClanFinal(connection, id, account_id);
                                    response = 1;
                                }
                                break;
                            case Data.ClanJoinType.NotAcceptingNewMembers:
                                // Not accepting
                                response = 7;
                                break;
                            case Data.ClanJoinType.TakingJoinRequests:
                                bool haveRequest = false;
                                query = String.Format("SELECT id FROM clan_join_requests WHERE clan_id = {0} AND account_id = {1};", id, account_id);
                                using (MySqlCommand command = new MySqlCommand(query, connection))
                                {
                                    using (MySqlDataReader reader = command.ExecuteReader())
                                    {
                                        if (reader.HasRows)
                                        {
                                            haveRequest = true;
                                        }
                                    }
                                }
                                if (haveRequest)
                                {
                                    // Request sent already
                                    response = 9;
                                }
                                else
                                {
                                    query = String.Format("INSERT INTO clan_join_requests (clan_id, account_id) VALUES({0}, {1})", id, account_id);
                                    using (MySqlCommand command = new MySqlCommand(query, connection))
                                    {
                                        command.ExecuteNonQuery();
                                    }
                                    // Request sent
                                    response = 8;
                                }
                                break;
                        }
                    }
                }
                connection.Close();
            }
            return response;
        }

        private static void JoinClanFinal(MySqlConnection connection, long clan_id, long account_id)
        {
            string query = String.Format("UPDATE accounts SET clan_id = {0}, clan_join_timer = NOW() + INTERVAL {1} HOUR, clan_chat_blocked = 0 WHERE id = {2}", clan_id, Data.clanJoinTimeGapHours, account_id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }
            query = String.Format("DELETE FROM clan_join_requests WHERE account_id = {0}", account_id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }
        }

        public async static void LeaveClan(int id)
        {
            long account_id = Server.clients[id].account;
            int response = await LeaveClanAsync(account_id);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.LEAVECLAN);
            packet.Write(response);
            Sender.TCP_Send(id, packet);
        }

        public async static void GetClans(int id, int page)
        {
            long account_id = Server.clients[id].account;
            Data.ClansList response = await GetClansAsync(page, account_id);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.GETCLANS);
            string clanData = await Data.SerializeAsync<Data.ClansList>(response);
            packet.Write(clanData);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<Data.ClansList> GetClansAsync(int page, long account_id = 0, long clan_id = 0)
        {
            Task<Data.ClansList> task = Task.Run(() =>
            {
                Data.ClansList response = new Data.ClansList();
                response = Retry.Do(() => _GetClansAsync(page, account_id, clan_id), TimeSpan.FromSeconds(0.1), 1, false);
                if (response == null)
                {
                    response = new Data.ClansList();
                }
                return response;
            });
            return await task;
        }

        private static Data.ClansList _GetClansAsync(int page, long account_id = 0, long clan_id = 0)
        {
            Data.ClansList response = new Data.ClansList();
            List<Data.Clan> clans = new List<Data.Clan>();
            using (MySqlConnection connection = GetMysqlConnection())
            {
                if (page <= 0)
                {
                    if (account_id > 0 && clan_id <= 0)
                    {
                        using (MySqlCommand command = new MySqlCommand(String.Format("SELECT clan_id FROM accounts WHERE id = {0};", account_id), connection))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        long.TryParse(reader["clan_id"].ToString(), out clan_id);
                                    }
                                }
                            }
                        }
                    }
                    if (clan_id <= 0)
                    {
                        page = 1;
                    }
                }

                int clansCount = 0;
                string query = "SELECT COUNT(*) AS count FROM clans";
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                int.TryParse(reader["count"].ToString(), out clansCount);
                            }
                        }
                    }
                }

                response.pagesCount = Convert.ToInt32(Math.Ceiling((double)clansCount / (double)Data.clansPerPage));

                if (response.pagesCount > 0)
                {
                    if (clan_id > 0)
                    {
                        page = 1;
                        int clanRank = GetClanRank(connection, clan_id);
                        if (clanRank > 0)
                        {
                            page = Convert.ToInt32(Math.Ceiling((double)clanRank / (double)Data.clansPerPage));
                        }
                    }
                    int start = ((page - 1) * Data.clansPerPage) + 1;
                    int end = start + Data.clansPerPage;
                    query = String.Format("SELECT id, name, join_type, xp, level, trophies, min_trophies, min_townhall_level, pattern, background, pattern_color, background_color, war_id, rank FROM (SELECT id, name, join_type, xp, level, trophies, min_trophies, min_townhall_level, pattern, background, pattern_color, background_color, war_id, ROW_NUMBER() OVER(ORDER BY trophies DESC) AS 'rank' FROM clans) AS ranks WHERE ranks.rank >= {0} AND ranks.rank < {1}", start, end);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    Data.Clan clan = new Data.Clan();
                                    clan.name = reader["name"].ToString();
                                    long.TryParse(reader["id"].ToString(), out clan.id);
                                    int.TryParse(reader["rank"].ToString(), out clan.rank);
                                    int joinType = 0;
                                    int.TryParse(reader["join_type"].ToString(), out joinType);
                                    clan.joinType = (Data.ClanJoinType)joinType;
                                    int.TryParse(reader["xp"].ToString(), out clan.xp);
                                    int.TryParse(reader["level"].ToString(), out clan.level);
                                    int.TryParse(reader["trophies"].ToString(), out clan.trophies);
                                    int.TryParse(reader["min_trophies"].ToString(), out clan.minTrophies);
                                    int.TryParse(reader["min_townhall_level"].ToString(), out clan.minTownhallLevel);
                                    int.TryParse(reader["pattern"].ToString(), out clan.pattern);
                                    int.TryParse(reader["background"].ToString(), out clan.background);
                                    clan.patternColor = reader["pattern_color"].ToString();
                                    clan.backgroundColor = reader["background_color"].ToString();
                                    clan.war = new Data.ClanWar();
                                    long.TryParse(reader["war_id"].ToString(), out clan.war.id);
                                    response.clans.Add(clan);
                                }
                            }
                        }
                    }
                }
                connection.Close();
            }
            return response;
        }

        private static int GetClanRank(MySqlConnection connection, long clan_id)
        {
            int rank = 0;
            string query = query = String.Format("SELECT id, rank FROM (SELECT id, ROW_NUMBER() OVER(ORDER BY trophies DESC) AS 'rank' FROM clans) AS ranks WHERE id = {0}", clan_id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            int.TryParse(reader["rank"].ToString(), out rank);
                        }
                    }
                }
            }
            return rank;
        }

        public async static void EditClan(int id, string name, int minTrophies, int minTownhallLevel, int pattern, int background, string patternColor, string backgroundColor, int joinType)
        {
            long account_id = Server.clients[id].account;
            int res = await EditClanAsync(account_id, name, minTrophies, minTownhallLevel, pattern, background, patternColor, backgroundColor, joinType);
            if (res == 1)
            {
                OpenClan(id);
            }
            else
            {
                Packet packet = new Packet();
                packet.Write((int)Terminal.RequestsID.EDITCLAN);
                packet.Write(res);
                Sender.TCP_Send(id, packet);
            }
        }

        private async static Task<int> EditClanAsync(long account_id, string name, int minTrophies, int minTownhallLevel, int pattern, int background, string patternColor, string backgroundColor, int joinType)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _EditClanAsync(account_id, name, minTrophies, minTownhallLevel, pattern, background, patternColor, backgroundColor, joinType), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _EditClanAsync(long account_id, string name, int minTrophies, int minTownhallLevel, int pattern, int background, string patternColor, string backgroundColor, int joinType)
        {
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                int clan_rank = 0;
                long clan_id = 0;
                string query = String.Format("SELECT clan_id, clan_rank FROM accounts WHERE id = {0};", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                long.TryParse(reader["clan_id"].ToString(), out clan_id);
                                int.TryParse(reader["clan_rank"].ToString(), out clan_rank);
                            }
                        }
                    }
                }

                if (clan_id > 0)
                {
                    bool havePermission = false;
                    for (int i = 0; i < Data.clanRanksWithEditPermission.Length; i++)
                    {
                        if (Data.clanRanksWithEditPermission[i] == clan_rank)
                        {
                            havePermission = true;
                            break;
                        }
                    }
                    if (havePermission)
                    {
                        Data.Clan clan = GetClan(connection, clan_id);
                        if (clan != null)
                        {
                            query = String.Format("UPDATE clans SET name = '{0}', min_trophies = {1}, min_townhall_level = {2}, pattern = {3}, background = {4}, pattern_color = '{5}', background_color = '{6}', join_type = {7} WHERE id = {8}", name, minTrophies, minTownhallLevel, pattern, background, patternColor, backgroundColor, joinType, clan_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                            response = 1;
                        }
                    }
                    else
                    {
                        // Do not have permission
                        response = 2;
                    }
                }
                connection.Close();
            }
            return response;
        }

        private static Data.Clan GetClan(MySqlConnection connection, long clan_id)
        {
            Data.Clan clan = null;
            string query = String.Format("SELECT name, join_type, xp, level, trophies, min_trophies, min_townhall_level, pattern, background, pattern_color, background_color, war_id FROM clans WHERE id = {0};", clan_id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            clan = new Data.Clan();
                            clan.id = clan_id;
                            clan.war = new Data.ClanWar();
                            clan.name = reader["name"].ToString();
                            int.TryParse(reader["xp"].ToString(), out clan.xp);
                            int.TryParse(reader["level"].ToString(), out clan.level);
                            int.TryParse(reader["trophies"].ToString(), out clan.trophies);
                            int.TryParse(reader["min_trophies"].ToString(), out clan.minTrophies);
                            int.TryParse(reader["min_townhall_level"].ToString(), out clan.minTownhallLevel);
                            int.TryParse(reader["pattern"].ToString(), out clan.pattern);
                            int.TryParse(reader["background"].ToString(), out clan.background);
                            clan.patternColor = reader["pattern_color"].ToString();
                            clan.backgroundColor = reader["background_color"].ToString();
                            long.TryParse(reader["war_id"].ToString(), out clan.war.id);
                            int joinType = 0;
                            int.TryParse(reader["join_type"].ToString(), out joinType);
                            clan.joinType = (Data.ClanJoinType)joinType;
                        }
                    }
                }
            }
            if (clan != null)
            {
                clan.members = GetClanMembers(connection, clan.id);
                if (clan.war.id > 0)
                {
                    query = String.Format("SELECT clan_1_id, clan_2_id, start_time, stage FROM clan_wars WHERE id = {0} AND stage > 0;", clan.war.id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    long.TryParse(reader["clan_1_id"].ToString(), out clan.war.clan1);
                                    long.TryParse(reader["clan_2_id"].ToString(), out clan.war.clan2);
                                    DateTime.TryParse(reader["start_time"].ToString(), out clan.war.start);
                                    int.TryParse(reader["stage"].ToString(), out clan.war.stage);
                                }
                            }
                            else
                            {
                                clan.war.id = 0;
                            }
                        }
                    }
                }
                if (clan.war.id > 0)
                {
                    clan.war.attacks = new List<Data.ClanWarAttack>();
                    query = String.Format("SELECT id, attacker_id, defender_id, start_time, stars, looted_gold, looted_elixir, looted_dark_elixir FROM clan_war_attacks WHERE war_id = {0};", clan.war.id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    Data.ClanWarAttack attack = new Data.ClanWarAttack();
                                    long.TryParse(reader["id"].ToString(), out attack.id);
                                    long.TryParse(reader["attacker_id"].ToString(), out attack.attacker);
                                    long.TryParse(reader["defender_id"].ToString(), out attack.defender);
                                    DateTime.TryParse(reader["start_time"].ToString(), out attack.start);
                                    int.TryParse(reader["stars"].ToString(), out attack.stars);
                                    int.TryParse(reader["looted_gold"].ToString(), out attack.gold);
                                    int.TryParse(reader["looted_elixir"].ToString(), out attack.elixir);
                                    int.TryParse(reader["looted_dark_elixir"].ToString(), out attack.dark);
                                    clan.war.attacks.Add(attack);
                                }
                            }
                        }
                    }
                }
            }
            return clan;
        }

        private static List<Data.ClanMember> GetClanMembers(MySqlConnection connection, long clan_id)
        {
            List<Data.ClanMember> members = new List<Data.ClanMember>();
            string query = String.Format("SELECT accounts.id, accounts.name, accounts.is_online, accounts.shield, accounts.level, accounts.xp, accounts.trophies, accounts.clan_rank, accounts.war_id, accounts.war_pos, buildings.level AS town_hall_level FROM accounts LEFT JOIN buildings ON buildings.account_id = accounts.id AND buildings.global_id = '{0}' WHERE accounts.clan_id = {1};", Data.BuildingID.townhall.ToString(), clan_id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            Data.ClanMember member = new Data.ClanMember();
                            member.name = reader["name"].ToString();
                            int.TryParse(reader["xp"].ToString(), out member.xp);
                            int.TryParse(reader["level"].ToString(), out member.level);
                            int.TryParse(reader["trophies"].ToString(), out member.trophies);
                            long.TryParse(reader["id"].ToString(), out member.id);
                            long.TryParse(reader["war_id"].ToString(), out member.warID);
                            int.TryParse(reader["war_pos"].ToString(), out member.warPos);
                            int online = 0;
                            int.TryParse(reader["is_online"].ToString(), out online);
                            member.online = (online > 0);
                            int.TryParse(reader["town_hall_level"].ToString(), out member.townHallLevel);
                            int.TryParse(reader["clan_rank"].ToString(), out member.rank);
                            member.clanID = clan_id;
                            members.Add(member);
                        }
                    }
                }
            }
            return members;
        }

        public async static void GetClanJoinRequests(int id)
        {
            long account_id = Server.clients[id].account;
            string response = await GetClanJoinRequestsAsync(account_id);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.JOINREQUESTS);
            packet.Write(response);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<string> GetClanJoinRequestsAsync(long account_id)
        {
            Task<string> task = Task.Run(() =>
            {
                string response = "";
                response = Retry.Do(() => _GetClanJoinRequestsAsync(account_id), TimeSpan.FromSeconds(0.1), 1, false);
                if (string.IsNullOrEmpty(response))
                {
                    return Data.Serialize<List<Data.JoinRequest>>(new List<Data.JoinRequest>());
                }
                else
                {
                    return response;
                }

            });
            return await task;
        }

        private static string _GetClanJoinRequestsAsync(long account_id)
        {
            List<Data.JoinRequest> requests = new List<Data.JoinRequest>();
            using (MySqlConnection connection = GetMysqlConnection())
            {
                long clan_id = 0;
                string query = String.Format("SELECT clan_id FROM accounts WHERE id = {0};", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                long.TryParse(reader["clan_id"].ToString(), out clan_id);
                            }
                        }
                    }
                }
                if (clan_id > 0)
                {
                    query = String.Format("SELECT clan_join_requests.id, clan_join_requests.account_id, clan_join_requests.request_time, accounts.name, accounts.level, accounts.trophies FROM clan_join_requests LEFT JOIN accounts ON clan_join_requests.account_id = accounts.id WHERE clan_join_requests.clan_id = {0};", clan_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    Data.JoinRequest request = new Data.JoinRequest();
                                    long.TryParse(reader["id"].ToString(), out request.id);
                                    long.TryParse(reader["account_id"].ToString(), out request.accountID);
                                    int.TryParse(reader["level"].ToString(), out request.level);
                                    int.TryParse(reader["trophies"].ToString(), out request.trophies);
                                    request.name = reader["name"].ToString();
                                    DateTime.TryParse(reader["request_time"].ToString(), out request.time);
                                    requests.Add(request);
                                }
                            }
                        }
                    }
                }
                connection.Close();
            }
            return Data.Serialize<List<Data.JoinRequest>>(requests);
        }

        public async static void ClanJoinRequestResponse(int id, long request_id, bool accepted)
        {
            long account_id = Server.clients[id].account;
            int response = await ClanJoinRequestResponseAsync(account_id, request_id, accepted);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.JOINRESPONSE);
            packet.Write(response);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> ClanJoinRequestResponseAsync(long account_id, long request_id, bool accepted)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _ClanJoinRequestResponseAsync(account_id, request_id, accepted), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _ClanJoinRequestResponseAsync(long account_id, long request_id, bool accepted)
        {
            int rresponse = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                long clan_id = 0;
                int clan_rank = 0;
                string query = String.Format("SELECT clan_id, clan_rank FROM accounts WHERE id = {0};", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                long.TryParse(reader["clan_id"].ToString(), out clan_id);
                                int.TryParse(reader["clan_rank"].ToString(), out clan_rank);
                            }
                        }
                    }
                }
                if (clan_id > 0)
                {
                    bool havePermission = false;
                    for (int i = 0; i < Data.clanRanksWithAcceptJoinRequstsPermission.Length; i++)
                    {
                        if (Data.clanRanksWithAcceptJoinRequstsPermission[i] == clan_rank)
                        {
                            havePermission = true;
                            break;
                        }
                    }
                    if (havePermission)
                    {
                        long id = 0;
                        query = String.Format("SELECT account_id FROM clan_join_requests WHERE id = {0} AND clan_id = {1};", request_id, clan_id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        long.TryParse(reader["account_id"].ToString(), out id);
                                    }
                                }
                            }
                        }
                        if (id > 0)
                        {
                            int members = 0;
                            query = String.Format("SELECT COUNT(id) AS count FROM accounts WHERE clan_id = {0};", clan_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                using (MySqlDataReader reader = command.ExecuteReader())
                                {
                                    if (reader.HasRows)
                                    {
                                        while (reader.Read())
                                        {
                                            int.TryParse(reader["count"].ToString(), out members);
                                        }
                                    }
                                }
                            }
                            if (members > 0 && members < Data.clanMaxMembers)
                            {
                                long haveClan = 0;
                                query = String.Format("SELECT clan_id FROM accounts WHERE id = {0};", id);
                                using (MySqlCommand command = new MySqlCommand(query, connection))
                                {
                                    using (MySqlDataReader reader = command.ExecuteReader())
                                    {
                                        if (reader.HasRows)
                                        {
                                            while (reader.Read())
                                            {
                                                long.TryParse(reader["clan_id"].ToString(), out haveClan);
                                            }
                                        }
                                    }
                                }
                                if (haveClan <= 0)
                                {
                                    query = String.Format("UPDATE accounts SET clan_id = {0}, clan_rank = 0, clan_chat_blocked = 0, clan_join_timer = NOW() + INTERVAL {1} HOUR WHERE id = {2} AND clan_id <= 0;", clan_id, Data.clanJoinTimeGapHours, id);
                                    using (MySqlCommand command = new MySqlCommand(query, connection))
                                    {
                                        command.ExecuteNonQuery();
                                    }
                                    query = String.Format("DELETE FROM clan_join_requests WHERE id = {0};", request_id);
                                    using (MySqlCommand command = new MySqlCommand(query, connection))
                                    {
                                        command.ExecuteNonQuery();
                                    }
                                    rresponse = 1;
                                }
                            }
                            else
                            {
                                rresponse = 3;
                            }
                        }
                    }
                    else
                    {
                        rresponse = 2;
                    }
                }
                connection.Close();
            }
            return rresponse;
        }

        private async static Task<int> LeaveClanAsync(long account_id)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _LeaveClanAsync(account_id), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _LeaveClanAsync(long account_id)
        {
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                long clan_id = 0;
                int clan_rank = 0;
                long war_id = 0;
                string query = String.Format("SELECT clan_id, clan_rank, war_id FROM accounts WHERE id = {0};", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                long.TryParse(reader["clan_id"].ToString(), out clan_id);
                                int.TryParse(reader["clan_rank"].ToString(), out clan_rank);
                                long.TryParse(reader["war_id"].ToString(), out war_id);
                            }
                        }
                    }
                }
                if (clan_id > 0)
                {
                    /*
                    long clan_war_id = 0;
                    query = String.Format("SELECT war_id FROM clans WHERE id = {0} AND stage > 0;", clan_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    long.TryParse(reader["war_id"].ToString(), out clan_war_id);
                                }
                            }
                        }
                    }
                    */
                    if (war_id >= 0)
                    {
                        // Can quit during war
                        response = 2;
                    }
                    else
                    {
                        int membersCount = 0;
                        long newLeader = 0;
                        query = String.Format("SELECT id, clan_rank FROM accounts WHERE clan_id = {0};", clan_id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                int rankCheck = 0;
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        membersCount++;
                                        if (clan_rank == 1)
                                        {
                                            long memberID = 0;
                                            long.TryParse(reader["id"].ToString(), out memberID);
                                            if (memberID != account_id)
                                            {
                                                int memberRank = 0;
                                                int.TryParse(reader["clan_rank"].ToString(), out memberRank);
                                                if (memberRank > 0)
                                                {
                                                    if (newLeader == 0)
                                                    {
                                                        rankCheck = memberRank;
                                                        newLeader = memberID;
                                                    }
                                                    else
                                                    {
                                                        if (memberRank < rankCheck)
                                                        {
                                                            rankCheck = memberRank;
                                                            newLeader = memberID;
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    if (newLeader == 0)
                                                    {
                                                        newLeader = memberID;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (clan_rank == 1)
                        {
                            query = String.Format("UPDATE accounts SET clan_rank = 1 WHERE id = {0}", newLeader);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                        }
                        query = String.Format("UPDATE accounts SET clan_id = 0, clan_rank = 0, war_id = -1, clan_join_timer = NOW() + INTERVAL {0} HOUR WHERE id = {1}", Data.clanJoinTimeGapHours, account_id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                        if (membersCount <= 1)
                        {
                            query = String.Format("DELETE FROM clans WHERE id = {0}", clan_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                            query = String.Format("DELETE FROM chat_messages WHERE clan_id = {0} AND type = {1}", clan_id, (int)Data.ChatType.clan);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                            query = String.Format("DELETE FROM clan_join_requests WHERE clan_id = {0}", clan_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                            List<long> wars = new List<long>();
                            query = String.Format("SELECT id FROM clan_wars WHERE clan_1_id = {0} OR clan_2_id = {1};", clan_id, clan_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                using (MySqlDataReader reader = command.ExecuteReader())
                                {
                                    if (reader.HasRows)
                                    {
                                        while (reader.Read())
                                        {
                                            long id = 0;
                                            long.TryParse(reader["id"].ToString(), out id);
                                            wars.Add(id);
                                        }
                                    }
                                }
                            }
                            query = String.Format("DELETE FROM clan_wars WHERE clan_1_id = {0} OR clan_2_id = {1};", clan_id, clan_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                            for (int i = 0; i < wars.Count; i++)
                            {
                                query = String.Format("DELETE FROM clan_war_attacks WHERE war_id = {0}", wars[i]);
                                using (MySqlCommand command = new MySqlCommand(query, connection))
                                {
                                    command.ExecuteNonQuery();
                                }
                            }
                        }
                        response = 1;
                    }
                }
                connection.Close();
            }
            return response;
        }

        private static Data.Clan GetClanByAccountId(MySqlConnection connection, long account_id)
        {
            Data.Clan clan = null;
            long clan_id = 0;
            string query = String.Format("SELECT clan_id FROM accounts WHERE id = {0};", account_id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            long.TryParse(reader["clan_id"].ToString(), out clan_id);
                        }
                    }
                }
            }
            if (clan_id > 0)
            {
                clan = GetClan(connection, clan_id);
            }
            return clan;
        }

        private async static Task<Data.Clan> GetClanByAccountIdAsync(long account_id)
        {
            Task<Data.Clan> task = Task.Run(() =>
            {
                return Retry.Do(() => _GetClanByAccountIdAsync(account_id), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static Data.Clan _GetClanByAccountIdAsync(long account_id)
        {
            Data.Clan clan = null;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                clan = GetClanByAccountId(connection, account_id);
                connection.Close();
            }
            return clan;
        }

        public async static void KickOutClanMember(int id, long member_id)
        {
            long account_id = Server.clients[id].account;
            var response = await KickOutClanMemberAsync(account_id, member_id);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.KICKMEMBER);
            packet.Write(member_id);
            packet.Write(response.Item1);
            Sender.TCP_Send(id, packet);
            if (response.Item3 != -1)
            {
                Packet packet_member = new Packet();
                packet.Write((int)Terminal.RequestsID.KICKMEMBER);
                packet.Write(member_id);
                packet.Write(-1);
                packet.Write(response.Item2);
                Sender.TCP_Send(response.Item3, packet);
            }
        }

        private async static Task<(int, string, int)> KickOutClanMemberAsync(long account_id, long member_id)
        {
            Task<(int, string, int)> task = Task.Run(() =>
            {
                return Retry.Do(() => _KickOutClanMemberAsync(account_id, member_id), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static (int, string, int) _KickOutClanMemberAsync(long account_id, long member_id)
        {
            int response = 0;
            int member_client_id = -1;
            string name = "";
            using (MySqlConnection connection = GetMysqlConnection())
            {
                long clan_id = 0;
                int clan_rank = 0;
                long member_clan_id = 0;
                int member_clan_rank = 0;
                long member_war_id = 0;
                int member_is_online = 0;
                string query = String.Format("SELECT id, clan_id, clan_rank, war_id, name, is_online, client_id FROM accounts WHERE id = {0} OR id = {1};", account_id, member_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                long id = 0;
                                long.TryParse(reader["id"].ToString(), out id);
                                if (id == member_id)
                                {
                                    long.TryParse(reader["clan_id"].ToString(), out member_clan_id);
                                    int.TryParse(reader["clan_rank"].ToString(), out member_clan_rank);
                                    long.TryParse(reader["war_id"].ToString(), out member_war_id);
                                    int.TryParse(reader["is_online"].ToString(), out member_is_online);
                                    int.TryParse(reader["client_id"].ToString(), out member_client_id);
                                }
                                else
                                {
                                    long.TryParse(reader["clan_id"].ToString(), out clan_id);
                                    int.TryParse(reader["clan_rank"].ToString(), out clan_rank);
                                    name = reader["name"].ToString();
                                }
                            }
                        }
                    }
                }
                if (clan_id > 0 && clan_id == member_clan_id)
                {
                    bool haveKickPermission = false;
                    if (clan_rank > 0 && clan_rank < member_clan_rank)
                    {
                        for (int i = 0; i < Data.clanRanksWithKickMembersPermission.Length; i++)
                        {
                            if (Data.clanRanksWithKickMembersPermission[i] == clan_rank)
                            {
                                haveKickPermission = true;
                                break;
                            }
                        }
                    }
                    if (haveKickPermission)
                    {
                        if (member_war_id >= 0)
                        {
                            response = 3;
                        }
                        else
                        {
                            query = String.Format("UPDATE accounts SET clan_id = 0 WHERE id = {0};", member_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                            response = 1;
                            if (member_is_online <= 0)
                            {
                                member_client_id = -1;
                            }
                        }
                    }
                    else
                    {
                        response = 2;
                    }
                }
                connection.Close();
            }
            return (response, name, member_client_id);
        }

        #endregion

        #region Clan War

        private static List<Data.ClanWarSearch> clansSearch = new List<Data.ClanWarSearch>();

        private static void StartClanWar(MySqlConnection connection, Data.ClanWarSearch search1_id, Data.ClanWarSearch search2_id)
        {
            string query = String.Format("DELETE FROM clan_wars_search WHERE id = {0} OR id = {1}", search1_id.id, search2_id.id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }

            bool found = false;
            query = String.Format("SELECT id FROM clan_wars WHERE (clan_1_id = {0} OR clan_1_id = {1} OR clan_2_id = {2} OR clan_2_id = {3}) AND stage > 0", search1_id.clan, search2_id.clan, search1_id.clan, search2_id.clan);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        found = true;
                    }
                }
            }

            if (!found)
            {
                long id = 0;
                query = String.Format("INSERT INTO clan_wars (clan_1_id, clan_2_id) VALUES({0}, {1})", search1_id.clan, search2_id.clan);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                    id = command.LastInsertedId;
                }

                if (id > 0)
                {
                    query = String.Format("UPDATE clans SET war_id = {0} WHERE id = {1} OR id = {2}", id, search1_id.clan, search2_id.clan);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    for (int i = 0; i < search1_id.members.Count; i++)
                    {
                        query = String.Format("UPDATE accounts SET war_id = {0}, war_pos = {1} WHERE id = {2}", id, search1_id.members[i].warPosition, search1_id.members[i].data.id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }

                        query = String.Format("UPDATE accounts SET war_id = {0}, war_pos = {1} WHERE id = {2}", id, search2_id.members[i].warPosition, search2_id.members[i].data.id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }

                    query = String.Format("SELECT client_id FROM accounts WHERE war_id = {0} AND is_online > 0;", id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    int client_id = 0;
                                    int.TryParse(reader["client_id"].ToString(), out client_id);
                                    Packet packet = new Packet();
                                    packet.Write((int)Terminal.RequestsID.WARSTARTED);
                                    packet.Write(id);
                                    Sender.TCP_Send(client_id, packet);
                                }
                            }
                        }
                    }
                }
            }
        }

        public async static void StartClanWar(int id, string members)
        {
            long account_id = Server.clients[id].account;
            List<long> members_id = await Data.DesrializeAsync<List<long>>(members);
            int res = await StartClanWarAsync(account_id, members_id);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.STARTWAR);
            packet.Write(res);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> StartClanWarAsync(long account_id, List<long> members)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _StartClanWarAsync(account_id, members), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _StartClanWarAsync(long account_id, List<long> members)
        {
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                int clan_rank = 0;
                long clan_id = 0;
                string query = String.Format("SELECT clan_id, clan_rank FROM accounts WHERE id = {0};", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                long.TryParse(reader["clan_id"].ToString(), out clan_id);
                                int.TryParse(reader["clan_rank"].ToString(), out clan_rank);
                            }
                        }
                    }
                }

                if (clan_id > 0)
                {
                    bool havePermission = false;
                    for (int i = 0; i < Data.clanRanksWithWarPermission.Length; i++)
                    {
                        if (Data.clanRanksWithWarPermission[i] == clan_rank)
                        {
                            havePermission = true;
                            break;
                        }
                    }
                    if (havePermission)
                    {
                        Data.Clan clan = GetClan(connection, clan_id);
                        if (clan != null)
                        {
                            if (clan.war.id > 0)
                            {
                                // Clan is already in war
                                response = 3;
                            }
                            else
                            {
                                bool found = false;
                                query = String.Format("SELECT id FROM clan_wars_search WHERE clan_id = {0};", clan_id);
                                using (MySqlCommand command = new MySqlCommand(query, connection))
                                {
                                    using (MySqlDataReader reader = command.ExecuteReader())
                                    {
                                        if (reader.HasRows)
                                        {
                                            found = true;
                                        }
                                    }
                                }
                                if (found)
                                {
                                    // Clan is already is searching
                                    response = 4;
                                }
                                else
                                {
                                    bool valid = true;
                                    for (int i = 0; i < members.Count; i++)
                                    {
                                        query = String.Format("SELECT id FROM accounts WHERE id = {0} AND war_id < 0 AND clan_id = {1};", members[i], clan_id);
                                        using (MySqlCommand command = new MySqlCommand(query, connection))
                                        {
                                            using (MySqlDataReader reader = command.ExecuteReader())
                                            {
                                                if (!reader.HasRows)
                                                {
                                                    valid = false;
                                                }
                                            }
                                        }
                                        if (!valid) { break; }
                                    }
                                    if (valid)
                                    {
                                        long id = 0;
                                        for (int i = 0; i < members.Count; i++)
                                        {
                                            query = String.Format("UPDATE accounts SET war_id = 0 WHERE id = {0}", members[i]);
                                            using (MySqlCommand command = new MySqlCommand(query, connection))
                                            {
                                                command.ExecuteNonQuery();
                                            }
                                        }
                                        query = String.Format("INSERT INTO clan_wars_search (clan_id, account_id) VALUES({0}, {1});", clan_id, account_id);
                                        using (MySqlCommand command = new MySqlCommand(query, connection))
                                        {
                                            command.ExecuteNonQuery();
                                            id = command.LastInsertedId;
                                        }
                                        response = 1;
                                    }
                                    else
                                    {
                                        // Members is not valid
                                        response = 5;
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        // Do not have permission
                        response = 2;
                    }
                }
                connection.Close();
            }
            return response;
        }

        public async static void CancelClanWar(int id)
        {
            long account_id = Server.clients[id].account;
            int res = await CancelClanWarAsync(account_id);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.CANCELWAR);
            packet.Write(res);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> CancelClanWarAsync(long account_id)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _CancelClanWarAsync(account_id), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _CancelClanWarAsync(long account_id)
        {
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                int clan_rank = 0;
                long clan_id = 0;
                string query = String.Format("SELECT clan_id, clan_rank FROM accounts WHERE id = {0};", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                long.TryParse(reader["clan_id"].ToString(), out clan_id);
                                int.TryParse(reader["clan_rank"].ToString(), out clan_rank);
                            }
                        }
                    }
                }

                if (clan_id > 0)
                {
                    bool havePermission = false;
                    for (int i = 0; i < Data.clanRanksWithWarPermission.Length; i++)
                    {
                        if (Data.clanRanksWithWarPermission[i] == clan_rank)
                        {
                            havePermission = true;
                            break;
                        }
                    }
                    if (havePermission)
                    {
                        query = String.Format("DELETE FROM clan_wars_search WHERE clan_id = {0}", clan_id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                        query = String.Format("UPDATE accounts SET war_id = -1 WHERE clan_id = {0}", clan_id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                        response = 1;
                    }
                    else
                    {
                        // Do not have permission
                        response = 2;
                    }
                }
                connection.Close();
            }
            return response;
        }

        public async static void OpenClanWar(int id)
        {
            long account_id = Server.clients[id].account;
            Data.ClanWarData war = await OpenClanWarAsync(account_id);
            string warData = await Data.SerializeAsync<Data.ClanWarData>(war);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.OPENWAR);
            packet.Write(warData);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<Data.ClanWarData> OpenClanWarAsync(long account_id)
        {
            Task<Data.ClanWarData> task = Task.Run(() =>
            {
                Data.ClanWarData war = null;
                war = Retry.Do(() => _OpenClanWarAsync(account_id), TimeSpan.FromSeconds(0.1), 1, false);
                if (war == null)
                {
                    war = new Data.ClanWarData();
                }
                return war;
            });
            return await task;
        }

        private static Data.ClanWarData _OpenClanWarAsync(long account_id)
        {
            Data.ClanWarData war = new Data.ClanWarData();
            war.searching = false;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                long clan_id = 0;
                string query = String.Format("SELECT clan_id FROM accounts WHERE id = {0};", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                long.TryParse(reader["clan_id"].ToString(), out clan_id);
                            }
                        }
                    }
                }
                Data.Clan clan1 = GetClan(connection, clan_id);
                if (clan1 != null)
                {
                    if (clan1.war.id > 0)
                    {
                        Data.Clan clan2 = GetClan(connection, clan_id == clan1.war.clan1 ? clan1.war.clan2 : clan1.war.clan1);
                        if (clan2 != null && clan2.war.id == clan1.war.id)
                        {
                            war.id = clan1.war.id;
                            war.clan1 = clan1;
                            war.clan2 = clan2;
                        }
                    }
                    for (int i = 0; i < clan1.members.Count; i++)
                    {
                        if (clan1.members[i].warID >= 0)
                        {
                            war.count += 1;
                        }
                    }
                }
                if (war.id <= 0)
                {
                    long id = 0;
                    query = String.Format("SELECT account_id FROM clan_wars_search WHERE clan_id = {0};", clan_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    long.TryParse(reader["account_id"].ToString(), out id);
                                }
                            }
                        }
                    }
                    if (id > 0)
                    {
                        war.searching = true;
                        query = String.Format("SELECT name FROM accounts WHERE id = {0};", id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        war.starter = reader["name"].ToString();
                                    }
                                }
                            }
                        }
                    }
                }
                connection.Close();
            }
            return war;
        }

        public async static void StartWarAttack(int id, long defender)
        {
            long account_id = Server.clients[id].account;
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.WARATTACK);
            defender = await StartWarAttackAsync(account_id, defender);
            packet.Write(defender);
            if (defender > 0)
            {
                Data.OpponentData opponent = new Data.OpponentData();
                opponent.id = defender;
                opponent.buildings = await GetBuildingsAsync(defender);
                opponent.buildings = await SetBuildingsPercentAsync(opponent.buildings, Data.BattleType.war);
                string data = await Data.SerializeAsync<Data.OpponentData>(opponent);
                packet.Write(data);
            }
            Sender.TCP_Send(id, packet);
        }

        private async static Task<long> StartWarAttackAsync(long account_id, long defender_id)
        {
            Task<long> task = Task.Run(() =>
            {
                return Retry.Do(() => _StartWarAttackAsync(account_id, defender_id), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static long _StartWarAttackAsync(long account_id, long defender_id)
        {
            long response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                Data.Clan clan1 = GetClanByAccountId(connection, account_id);
                Data.Clan clan2 = GetClanByAccountId(connection, defender_id);
                if (clan1 != null && clan2 != null && clan1.war != null && clan2.war != null && clan1.war.id == clan2.war.id)
                {
                    if (clan1.war.stage == 2)
                    {
                        int attacks = 0;
                        for (int i = 0; i < clan1.war.attacks.Count; i++)
                        {
                            if (clan1.war.attacks[i].attacker == account_id)
                            {
                                attacks++;
                            }
                        }
                        if (attacks < Data.clanWarAttacksPerPlayer)
                        {
                            response = defender_id;
                        }
                    }
                    else
                    {
                        response = -1;
                    }
                }
                connection.Close();
            }
            return response;
        }

        #endregion

        #region Email

        public async static void SendRecoveryCode(int id, string device, string email)
        {
            var code = await SendRecoveryCodeAsync(id, device, email);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.SENDCODE);
            packet.Write(code.Item1);
            packet.Write(code.Item2);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<(int, int)> SendRecoveryCodeAsync(int id, string device, string email)
        {
            Task<(int, int)> task = Task.Run(() =>
            {
                return Retry.Do(() => _SendRecoveryCodeAsync(id, device, email), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static (int, int) _SendRecoveryCodeAsync(int id, string device, string email)
        {
            int expiration = 0;
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                long account_id = 0;
                if (!string.IsNullOrEmpty(email))
                {
                    string query = String.Format("SELECT id FROM accounts WHERE email = '{0}' AND is_online <= 0;", email);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    long.TryParse(reader["id"].ToString(), out account_id);
                                }
                            }
                        }
                    }
                }
                if (account_id > 0)
                {
                    long code_id = 0;
                    DateTime nowTime = DateTime.Now;
                    DateTime expireTime = nowTime;
                    string query = String.Format("SELECT id, NOW() AS now_time, expire_time FROM verification_codes WHERE device_id = '{0}' AND target = '{1}' AND NOW() < expire_time;", device, email);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    long.TryParse(reader["id"].ToString(), out code_id);
                                    DateTime.TryParse(reader["now_time"].ToString(), out nowTime);
                                    DateTime.TryParse(reader["expire_time"].ToString(), out expireTime);
                                }
                            }
                        }
                    }
                    if (code_id > 0)
                    {
                        response = 1;
                        expiration = (int)Math.Floor((expireTime - nowTime).TotalSeconds);
                    }
                    else
                    {
                        string code = Data.RandomCode(Data.recoveryCodeLength);
                        if (Email.SendEmailVerificationCode(code, email))
                        {
                            query = String.Format("INSERT INTO verification_codes (target, device_id, code, expire_time) VALUES('{0}', '{1}', '{2}', NOW() + INTERVAL {3} SECOND)", email, device, code, Data.recoveryCodeExpiration);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                            response = 1;
                            expiration = Data.recoveryCodeExpiration;
                        }
                        else
                        {
                            response = 2;
                        }
                    }
                }
                connection.Close();
            }
            return (response, expiration);
        }

        public async static void ConfirmRecoveryCode(int id, string device, string email, string code)
        {
            var response = await ConfirmRecoveryCodeAsync(id, device, email, code);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.CONFIRMCODE);
            packet.Write(response.Item1);
            packet.Write(response.Item2);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<(int, string)> ConfirmRecoveryCodeAsync(int id, string device, string email, string code)
        {
            Task<(int, string)> task = Task.Run(() =>
            {
                return Retry.Do(() => _ConfirmRecoveryCodeAsync(id, device, email, code), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static (int, string) _ConfirmRecoveryCodeAsync(int id, string device, string email, string code)
        {
            string password = "";
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                long account_id = 0;
                if (!string.IsNullOrEmpty(email))
                {
                    string query = String.Format("SELECT id FROM accounts WHERE email = '{0}' AND is_online <= 0;", email);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    long.TryParse(reader["id"].ToString(), out account_id);
                                }
                            }
                        }
                    }
                }
                if (account_id > 0)
                {
                    long code_id = 0;
                    string query = String.Format("SELECT id FROM verification_codes WHERE device_id = '{0}' AND target = '{1}' AND code = '{2}' AND NOW() < expire_time;", device, email, code);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    long.TryParse(reader["id"].ToString(), out code_id);
                                }
                            }
                        }
                    }
                    if (code_id > 0)
                    {
                        password = Data.EncrypteToMD5(Tools.GenerateToken());
                        query = String.Format("UPDATE accounts SET device_id = '{0}', password = '{1}' WHERE id = {2};", device, password, account_id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                        query = String.Format("DELETE FROM verification_codes WHERE id = {0};", code_id);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                        response = 1;
                    }
                    else
                    {
                        response = 2;
                    }
                }
                connection.Close();
            }
            return (response, password);
        }

        public async static void SendEmailCode(int id, string device, string email)
        {
            long account_id = Server.clients[id].account;
            var code = await SendEmailCodeAsync(id, account_id, device, email);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.EMAILCODE);
            packet.Write(code.Item1);
            packet.Write(code.Item2);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<(int, int)> SendEmailCodeAsync(int id, long account_id, string device, string email)
        {
            Task<(int, int)> task = Task.Run(() =>
            {
                return Retry.Do(() => _SendEmailCodeAsync(id, account_id, device, email), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static (int, int) _SendEmailCodeAsync(int id, long account_id, string device, string email)
        {
            int expiration = 0;
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                bool found = false;
                if (!string.IsNullOrEmpty(email))
                {
                    string query = String.Format("SELECT id FROM accounts WHERE id = {0} AND device_id = '{1}' AND is_online > 0;", account_id, device);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                found = true;
                            }
                        }
                    }
                }
                if (found)
                {
                    found = false;
                    string query = String.Format("SELECT id FROM accounts WHERE email = '{0}';", email);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                found = true;
                            }
                        }
                    }
                    if (!found)
                    {
                        long code_id = 0;
                        DateTime nowTime = DateTime.Now;
                        DateTime expireTime = nowTime;
                        query = String.Format("SELECT id, NOW() AS now_time, expire_time FROM verification_codes WHERE device_id = '{0}' AND target = '{1}' AND NOW() < expire_time;", device, email);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        long.TryParse(reader["id"].ToString(), out code_id);
                                        DateTime.TryParse(reader["now_time"].ToString(), out nowTime);
                                        DateTime.TryParse(reader["expire_time"].ToString(), out expireTime);
                                    }
                                }
                            }
                        }
                        if (code_id > 0)
                        {
                            response = 1;
                            expiration = (int)Math.Floor((expireTime - nowTime).TotalSeconds);
                        }
                        else
                        {
                            string code = Data.RandomCode(Data.recoveryCodeLength);
                            if (Email.SendEmailConfirmationCode(code, email))
                            {
                                query = String.Format("INSERT INTO verification_codes (target, device_id, code, expire_time) VALUES('{0}', '{1}', '{2}', NOW() + INTERVAL {3} SECOND)", email, device, code, Data.confirmationCodeExpiration);
                                using (MySqlCommand command = new MySqlCommand(query, connection))
                                {
                                    command.ExecuteNonQuery();
                                }
                                response = 1;
                                expiration = Data.confirmationCodeExpiration;
                            }
                            else
                            {
                                response = 2;
                            }
                        }
                    }
                    else
                    {
                        response = 3;
                    }
                }
                connection.Close();
            }
            return (response, expiration);
        }

        public async static void ConfirmEmailCode(int id, string device, string email, string code)
        {
            long account_id = Server.clients[id].account;
            int response = await ConfirmEmailCodeAsync(account_id, device, email, code);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.EMAILCONFIRM);
            packet.Write(response);
            packet.Write(email);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> ConfirmEmailCodeAsync(long account_id, string device, string email, string code)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _ConfirmEmailCodeAsync(account_id, device, email, code), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _ConfirmEmailCodeAsync(long account_id, string device, string email, string code)
        {
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                bool found = false;
                if (!string.IsNullOrEmpty(email))
                {
                    string query = String.Format("SELECT id FROM accounts WHERE id = {0} AND device_id = '{1}' AND is_online > 0;", account_id, device);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                found = true;
                            }
                        }
                    }
                }
                if (found)
                {
                    found = false;
                    string query = String.Format("SELECT id FROM accounts WHERE email = '{0}';", email);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                found = false;
                            }
                        }
                    }
                    if (!found)
                    {
                        long code_id = 0;
                        query = String.Format("SELECT id FROM verification_codes WHERE device_id = '{0}' AND target = '{1}' AND code = '{2}' AND NOW() < expire_time;", device, email, code);
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        long.TryParse(reader["id"].ToString(), out code_id);
                                    }
                                }
                            }
                        }
                        if (code_id > 0)
                        {
                            query = String.Format("UPDATE accounts SET email = '{0}' WHERE id = {1};", email, account_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                            query = String.Format("DELETE FROM verification_codes WHERE id = {0};", code_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                            response = 1;
                        }
                        else
                        {
                            response = 2;
                        }
                    }
                    else
                    {
                        response = 3;
                    }
                }
                connection.Close();
            }
            return response;
        }

        #endregion

        #region Messages

        public async static void SyncMessages(int id, Data.ChatType type, long lastMessage)
        {
            long account_id = Server.clients[id].account;
            List<Data.CharMessage> response = await GetChatMessagesAsync(account_id, type, lastMessage);
            string data = await Data.SerializeAsync<List<Data.CharMessage>>(response);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.GETCHATS);
            packet.Write(data);
            packet.Write((int)type);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<List<Data.CharMessage>> GetChatMessagesAsync(long account_id, Data.ChatType type, long lastMessage)
        {
            Task<List<Data.CharMessage>> task = Task.Run(() =>
            {
                List<Data.CharMessage> response = null;
                response = Retry.Do(() => _GetChatMessagesAsync(account_id, type, lastMessage), TimeSpan.FromSeconds(0.1), 1, false);
                if (response == null)
                {
                    response = new List<Data.CharMessage>();
                }
                return response;
            });
            return await task;
        }

        private static List<Data.CharMessage> _GetChatMessagesAsync(long account_id, Data.ChatType type, long lastMessage)
        {
            List<Data.CharMessage> response = new List<Data.CharMessage>();
            using (MySqlConnection connection = GetMysqlConnection())
            {
                response = GetChatMessages(connection, account_id, type, lastMessage);
                connection.Close();
            }
            return response;
        }

        private static List<Data.CharMessage> GetChatMessages(MySqlConnection connection, long account_id, Data.ChatType type, long lastMessage)
        {
            List<Data.CharMessage> messages = new List<Data.CharMessage>();

            long clan_id = 0;
            long global_id = 0;

            string query = "";

            string filterTime = "";
            if (lastMessage > 0)
            {
                query = String.Format("SELECT DATE_FORMAT(send_time, '{0}') AS send_time FROM chat_messages WHERE id = {1}", Data.mysqlDateTimeFormat, lastMessage);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                filterTime = reader["send_time"].ToString();
                            }
                        }
                    }
                }
            }

            if (type == Data.ChatType.clan)
            {
                query = String.Format("SELECT clan_id FROM accounts WHERE id = {0} AND clan_id > 0;", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                long.TryParse(reader["clan_id"].ToString(), out clan_id);
                            }
                        }
                    }
                }
                if (clan_id > 0)
                {
                    if (!string.IsNullOrEmpty(filterTime))
                    {
                        query = String.Format("SELECT chat_messages.id, chat_messages.account_id, chat_messages.type, chat_messages.global_id, chat_messages.clan_id, chat_messages.message, DATE_FORMAT(chat_messages.send_time, '{0}') AS send_time, accounts.name, accounts.chat_color FROM chat_messages LEFT JOIN accounts ON chat_messages.account_id = accounts.id WHERE chat_messages.clan_id = {1} AND chat_messages.type = {2} AND chat_messages.send_time > '{3}'", Data.mysqlDateTimeFormat, clan_id, (int)type, filterTime);
                    }
                    else
                    {
                        query = String.Format("SELECT chat_messages.id, chat_messages.account_id, chat_messages.type, chat_messages.global_id, chat_messages.clan_id, chat_messages.message, DATE_FORMAT(chat_messages.send_time, '{0}') AS send_time, accounts.name, accounts.chat_color FROM chat_messages LEFT JOIN accounts ON chat_messages.account_id = accounts.id WHERE chat_messages.clan_id = {1} AND chat_messages.type = {2}", Data.mysqlDateTimeFormat, clan_id, (int)type);
                    }
                }
                else
                {
                    query = "";
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(filterTime))
                {
                    query = String.Format("SELECT chat_messages.id, chat_messages.account_id, chat_messages.type, chat_messages.global_id, chat_messages.clan_id, chat_messages.message, DATE_FORMAT(chat_messages.send_time, '{0}') AS send_time, accounts.name, accounts.chat_color FROM chat_messages LEFT JOIN accounts ON chat_messages.account_id = accounts.id WHERE chat_messages.global_id = {1} AND chat_messages.type = {2} AND chat_messages.send_time > '{3}'", Data.mysqlDateTimeFormat, global_id, (int)type, filterTime);
                }
                else
                {
                    query = String.Format("SELECT chat_messages.id, chat_messages.account_id, chat_messages.type, chat_messages.global_id, chat_messages.clan_id, chat_messages.message, DATE_FORMAT(chat_messages.send_time, '{0}') AS send_time, accounts.name, accounts.chat_color FROM chat_messages LEFT JOIN accounts ON chat_messages.account_id = accounts.id WHERE chat_messages.global_id = {1} AND chat_messages.type = {2}", Data.mysqlDateTimeFormat, global_id, (int)type);
                }
            }

            if (!string.IsNullOrEmpty(query))
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                Data.CharMessage message = new Data.CharMessage();
                                long.TryParse(reader["id"].ToString(), out message.id);
                                long.TryParse(reader["account_id"].ToString(), out message.accountID);
                                int t = 0;
                                int.TryParse(reader["type"].ToString(), out t);
                                message.type = (Data.ChatType)t;
                                long.TryParse(reader["global_id"].ToString(), out message.globalID);
                                long.TryParse(reader["clan_id"].ToString(), out message.clanID);
                                message.message = reader["message"].ToString();
                                message.name = reader["name"].ToString();
                                message.color = reader["chat_color"].ToString();
                                message.time = reader["send_time"].ToString();
                                messages.Add(message);
                            }
                        }
                    }
                }
            }
            return messages;
        }

        public async static void SendChatMessage(int id, string message, Data.ChatType type, long target)
        {
            long account_id = Server.clients[id].account;
            int response = await SendChatMessageAsync(account_id, message, type, target);
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.SENDCHAT);
            packet.Write(response);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> SendChatMessageAsync(long account_id, string message, Data.ChatType type, long target)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _SendChatMessageAsync(account_id, message, type, target), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _SendChatMessageAsync(long account_id, string message, Data.ChatType type, long target)
        {
            int response = 0;
            if (!string.IsNullOrEmpty(message) && Data.IsMessageGoodToSend(message))
            {
                using (MySqlConnection connection = GetMysqlConnection())
                {
                    long clan_id = 0;
                    int global_chat_blocked = 0;
                    int clan_chat_blocked = 0;
                    bool timeOk = false;
                    string query = String.Format("SELECT clan_id, global_chat_blocked, clan_chat_blocked FROM accounts WHERE id = {0} AND last_chat <= NOW() - INTERVAL 1 SECOND;", account_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    long.TryParse(reader["clan_id"].ToString(), out clan_id);
                                    int.TryParse(reader["global_chat_blocked"].ToString(), out global_chat_blocked);
                                    int.TryParse(reader["clan_chat_blocked"].ToString(), out clan_chat_blocked);
                                    timeOk = true;
                                }
                            }
                        }
                    }
                    if (timeOk)
                    {
                        if (global_chat_blocked > 0 || (clan_chat_blocked > 0 && clan_id > 0 && type == Data.ChatType.clan))
                        {
                            // Banned from sending message
                            response = 2;
                        }
                        else
                        {
                            bool sent = false;
                            if (type == Data.ChatType.global)
                            {
                                query = String.Format("INSERT INTO chat_messages (account_id, type, global_id, clan_id, message) VALUES({0}, {1}, {2}, {3}, '{4}');", account_id, (int)type, target, clan_id, message);
                                using (MySqlCommand command = new MySqlCommand(query, connection))
                                {
                                    command.ExecuteNonQuery();
                                }
                                query = String.Format("DELETE FROM chat_messages WHERE type = {0} AND global_id = {1} AND send_time <= (SELECT send_time FROM (SELECT send_time FROM chat_messages WHERE type = {0} AND global_id = {1} ORDER BY send_time DESC LIMIT 1 OFFSET {2}) messages);", (int)type, target, Data.globalChatArchiveMaxMessages);
                                using (MySqlCommand command = new MySqlCommand(query, connection))
                                {
                                    command.ExecuteNonQuery();
                                }
                                sent = true;
                            }
                            else
                            {
                                if (clan_id == target)
                                {
                                    query = String.Format("INSERT INTO chat_messages (account_id, type, global_id, clan_id, message) VALUES({0}, {1}, {2}, {3}, '{4}');", account_id, (int)type, 0, clan_id, message);
                                    using (MySqlCommand command = new MySqlCommand(query, connection))
                                    {
                                        command.ExecuteNonQuery();
                                    }
                                    query = String.Format("DELETE FROM chat_messages WHERE type = {0} AND clan_id = {1} AND send_time <= (SELECT send_time FROM (SELECT send_time FROM chat_messages WHERE type = {0} AND clan_id = {1} ORDER BY send_time DESC LIMIT 1 OFFSET {2}) messages);", (int)type, clan_id, Data.clanChatArchiveMaxMessages);
                                    using (MySqlCommand command = new MySqlCommand(query, connection))
                                    {
                                        command.ExecuteNonQuery();
                                    }
                                    sent = true;
                                }
                            }
                            if (sent)
                            {
                                query = String.Format("UPDATE accounts SET last_chat = NOW() WHERE id = {0};", account_id);
                                using (MySqlCommand command = new MySqlCommand(query, connection))
                                {
                                    command.ExecuteNonQuery();
                                }
                                response = 1;
                            }
                        }
                    }
                    connection.Close();
                }
            }
            return response;
        }
        
        #endregion

        #region Spell

        private static List<Data.ServerSpell> GetServerSpells(MySqlConnection connection)
        {
            List<Data.ServerSpell> units = new List<Data.ServerSpell>();
            string query = String.Format("SELECT id, global_id, level, req_gold, req_elixir, req_gem, req_dark_elixir, brew_time, housing, radius, pulses_count, pulses_duration, pulses_value, pulses_value_2, research_time, research_gold, research_elixir, research_dark_elixir, research_gems FROM server_spells;");
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            Data.ServerSpell spell = new Data.ServerSpell();
                            long.TryParse(reader["id"].ToString(), out spell.databaseID);
                            spell.id = (Data.SpellID)Enum.Parse(typeof(Data.SpellID), reader["global_id"].ToString());
                            int.TryParse(reader["level"].ToString(), out spell.level);
                            int.TryParse(reader["req_gold"].ToString(), out spell.requiredGold);
                            int.TryParse(reader["req_elixir"].ToString(), out spell.requiredElixir);
                            int.TryParse(reader["req_gem"].ToString(), out spell.requiredGems);
                            int.TryParse(reader["req_dark_elixir"].ToString(), out spell.requiredDarkElixir);
                            int.TryParse(reader["brew_time"].ToString(), out spell.brewTime);
                            int.TryParse(reader["housing"].ToString(), out spell.housing);



                            float.TryParse(reader["radius"].ToString(), out spell.radius);
                            int.TryParse(reader["pulses_count"].ToString(), out spell.pulsesCount);
                            float.TryParse(reader["pulses_duration"].ToString(), out spell.pulsesDuration);
                            float.TryParse(reader["pulses_value"].ToString(), out spell.pulsesValue);
                            float.TryParse(reader["pulses_value_2"].ToString(), out spell.pulsesValue2);
                            float.TryParse(reader["research_time"].ToString(), out spell.researchTime);
                            int.TryParse(reader["research_gold"].ToString(), out spell.researchGold);
                            int.TryParse(reader["research_elixir"].ToString(), out spell.researchElixir);
                            int.TryParse(reader["research_dark_elixir"].ToString(), out spell.researchDarkElixir);
                            int.TryParse(reader["research_gems"].ToString(), out spell.researchGems);


                            units.Add(spell);
                        }
                    }
                }
            }
            return units;
        }

        public async static void BrewSpell(int id, string globalID)
        {
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.BREW);
            long account_id = Server.clients[id].account;
            int res = await BrewSpellAsync(account_id, 1, globalID);
            packet.Write(res);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> BrewSpellAsync(long account_id, int level, string globalID)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _BrewSpellAsync(account_id, level, globalID), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static int _BrewSpellAsync(long account_id, int level, string globalID)
        {
            int response = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                Data.ServerSpell spell = GetServerSpell(connection, globalID, level);
                if (spell != null)
                {
                    int capacity = 0;
                    List<Data.Building> spellFactory = GetBuildingsByGlobalID(Data.BuildingID.spellfactory.ToString(), account_id, connection);
                    for (int i = 0; i < spellFactory.Count; i++)
                    {
                        capacity += spellFactory[i].capacity;
                    }

                    int occupied = 999;
                    string query = String.Format("SELECT SUM(server_spells.housing) AS occupied FROM spells LEFT JOIN server_spells ON spells.global_id = server_spells.global_id AND spells.level = server_spells.level WHERE spells.account_id = {0} AND ready <= 0;", account_id);
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    int.TryParse(reader["occupied"].ToString(), out occupied);
                                }
                            }
                        }
                    }

                    if (capacity - occupied >= spell.housing)
                    {
                        if (SpendResources(connection, account_id, spell.requiredGold, spell.requiredElixir, spell.requiredGems, spell.requiredDarkElixir))
                        {
                            query = String.Format("INSERT INTO spells (global_id, level, account_id) VALUES('{0}', {1}, {2})", globalID, level, account_id);
                            using (MySqlCommand command = new MySqlCommand(query, connection))
                            {
                                command.ExecuteNonQuery();
                                response = 1;
                            }
                        }
                        else
                        {
                            response = 2;
                        }
                    }
                    else
                    {
                        response = 4;
                    }
                }
                else
                {
                    response = 3;
                }
                connection.Close();
            }
            return response;
        }

        private static Data.ServerSpell GetServerSpell(MySqlConnection connection, string id, int level)
        {
            Data.ServerSpell spell = null;
            string query = String.Format("SELECT id, global_id, level, req_gold, req_elixir, req_gem, req_dark_elixir, brew_time, housing, radius, pulses_count, pulses_duration, pulses_value, pulses_value_2, research_time, research_gold, research_elixir, research_dark_elixir, research_gems FROM server_spells WHERE global_id = '{0}' AND level = {1};", id, level);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            spell = new Data.ServerSpell();
                            long.TryParse(reader["id"].ToString(), out spell.databaseID);
                            spell.id = (Data.SpellID)Enum.Parse(typeof(Data.SpellID), reader["global_id"].ToString());
                            int.TryParse(reader["level"].ToString(), out spell.level);
                            int.TryParse(reader["req_gold"].ToString(), out spell.requiredGold);
                            int.TryParse(reader["req_elixir"].ToString(), out spell.requiredElixir);
                            int.TryParse(reader["req_gem"].ToString(), out spell.requiredGems);
                            int.TryParse(reader["req_dark_elixir"].ToString(), out spell.requiredDarkElixir);
                            int.TryParse(reader["brew_time"].ToString(), out spell.brewTime);
                            int.TryParse(reader["housing"].ToString(), out spell.housing);
                            float.TryParse(reader["radius"].ToString(), out spell.radius);
                            int.TryParse(reader["pulses_count"].ToString(), out spell.pulsesCount);
                            float.TryParse(reader["pulses_duration"].ToString(), out spell.pulsesDuration);
                            float.TryParse(reader["pulses_value"].ToString(), out spell.pulsesValue);
                            float.TryParse(reader["pulses_value_2"].ToString(), out spell.pulsesValue2);
                            float.TryParse(reader["research_time"].ToString(), out spell.researchTime);
                            int.TryParse(reader["research_gold"].ToString(), out spell.researchGold);
                            int.TryParse(reader["research_elixir"].ToString(), out spell.researchElixir);
                            int.TryParse(reader["research_dark_elixir"].ToString(), out spell.researchDarkElixir);
                            int.TryParse(reader["research_gems"].ToString(), out spell.researchGems);
                        }
                    }
                }
            }
            return spell;
        }
    
        public async static void CancelBrewSpell(int id, long databaseID)
        {
            Packet packet = new Packet();
            packet.Write((int)Terminal.RequestsID.CANCELBREW);
            long account_id = Server.clients[id].account;
            int res = await CancelBrewSpellAsync(account_id, databaseID);
            packet.Write(res);
            Sender.TCP_Send(id, packet);
        }

        private async static Task<int> CancelBrewSpellAsync(long account_id, long databaseID)
        {
            Task<int> task = Task.Run(() =>
            {
                return Retry.Do(() => _CancelBrewSpellAsync(account_id, databaseID), TimeSpan.FromSeconds(0.1), 10, false);
            });
            return await task;
        }

        private static int _CancelBrewSpellAsync(long account_id, long databaseID)
        {
            int id = 0;
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = String.Format("DELETE FROM spells WHERE id = {0} AND account_id = {1} AND ready <= 0", databaseID, account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                    id = 1;
                }
                connection.Close();
            }
            return id;
        }

        private async static Task<List<Data.Spell>> GetSpellsAsync(long account_id)
        {
            Task<List<Data.Spell>> task = Task.Run(() =>
            {
                return Retry.Do(() => _GetSpellsAsync(account_id), TimeSpan.FromSeconds(0.1), 1, false);
            });
            return await task;
        }

        private static List<Data.Spell> _GetSpellsAsync(long account_id)
        {
            List<Data.Spell> spells = new List<Data.Spell>();
            using (MySqlConnection connection = GetMysqlConnection())
            {
                string query = String.Format("SELECT spells.id, spells.global_id, spells.level, spells.brewed, spells.ready, spells.brewed_time, server_spells.brew_time, server_spells.housing FROM spells LEFT JOIN server_spells ON spells.global_id = server_spells.global_id AND spells.level = server_spells.level WHERE spells.account_id = {0};", account_id);
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                Data.Spell spell = new Data.Spell();
                                spell.id = (Data.SpellID)Enum.Parse(typeof(Data.SpellID), reader["global_id"].ToString());
                                long.TryParse(reader["id"].ToString(), out spell.databaseID);
                                int.TryParse(reader["level"].ToString(), out spell.level);
                                int.TryParse(reader["housing"].ToString(), out spell.hosing);
                                int.TryParse(reader["brew_time"].ToString(), out spell.brewTime);
                                float.TryParse(reader["brewed_time"].ToString(), out spell.brewedTime);

                                int isTrue = 0;
                                int.TryParse(reader["brewed"].ToString(), out isTrue);
                                spell.brewed = isTrue > 0;

                                isTrue = 0;
                                int.TryParse(reader["ready"].ToString(), out isTrue);
                                spell.ready = isTrue > 0;
                                spells.Add(spell);
                            }
                        }
                    }
                }
                connection.Close();
            }
            return spells;
        }

        private static Data.Spell GetSpell(MySqlConnection connection, long database_id, long account_id, bool get_server = false)
        {
            Data.Spell spell = null;
            string query = String.Format("SELECT spells.id, spells.global_id, spells.level, spells.brewed, spells.ready, spells.brewed_time, server_spells.brew_time, server_spells.housing FROM spells LEFT JOIN server_spells ON spells.global_id = server_spells.global_id AND spells.level = server_spells.level WHERE spells.id = {0} AND spells.account_id = {1};", database_id, account_id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            spell = new Data.Spell();
                            spell.id = (Data.SpellID)Enum.Parse(typeof(Data.SpellID), reader["global_id"].ToString());
                            long.TryParse(reader["id"].ToString(), out spell.databaseID);
                            int.TryParse(reader["level"].ToString(), out spell.level);
                            int.TryParse(reader["housing"].ToString(), out spell.hosing);
                            int.TryParse(reader["brew_time"].ToString(), out spell.brewTime);
                            float.TryParse(reader["brewed_time"].ToString(), out spell.brewedTime);

                            int isTrue = 0;
                            int.TryParse(reader["brewed"].ToString(), out isTrue);
                            spell.brewed = isTrue > 0;

                            isTrue = 0;
                            int.TryParse(reader["ready"].ToString(), out isTrue);
                            spell.ready = isTrue > 0;
                        }
                    }
                }
            }
            if (spell != null && get_server)
            {
                spell.server = GetServerSpell(connection, spell.id.ToString(), spell.level);
            }
            return spell;
        }

        public static void DeleteSpell(long id, MySqlConnection connection)
        {
            string query = String.Format("DELETE FROM spells WHERE id = {0};", id);
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }
        }

        #endregion

    }
}