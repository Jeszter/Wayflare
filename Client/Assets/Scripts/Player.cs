namespace DevelopersHub.ClashOfWhatecer
{
    using System.Collections.Generic;
    using UnityEngine;
    using DevelopersHub.RealtimeNetworking.Client;
    using UnityEngine.SceneManagement;

    public class Player : MonoBehaviour
    {
        public Data.Player data = new Data.Player();
        private static Player _instance = null; public static Player instanse { get { return _instance; } }
        public Data.InitializationData initializationData = new Data.InitializationData();
        private bool _inBattle = false; public static bool inBattle { get { return instanse._inBattle; } set { instanse._inBattle = value; } }
        private bool _msgBoxClosing = false;

        public enum RequestsID
        {
            AUTH = 1, SYNC = 2, BUILD = 3, REPLACE = 4, COLLECT = 5, PREUPGRADE = 6, UPGRADE = 7, INSTANTBUILD = 8,
            TRAIN = 9, CANCELTRAIN = 10, BATTLEFIND = 11, BATTLESTART = 12, BATTLEFRAME = 13, BATTLEEND = 14,
            OPENCLAN = 15, GETCLANS = 16, JOINCLAN = 17, LEAVECLAN = 18, EDITCLAN = 19, CREATECLAN = 20,
            OPENWAR = 21, STARTWAR = 22, CANCELWAR = 23, WARSTARTED = 24, WARATTACK = 25, WARREPORTLIST = 26,
            WARREPORT = 27, JOINREQUESTS = 28, JOINRESPONSE = 29, GETCHATS = 30, SENDCHAT = 31,
            SENDCODE = 32, CONFIRMCODE = 33, EMAILCODE = 34, EMAILCONFIRM = 35, LOGOUT = 36,
            KICKMEMBER = 37, BREW = 38, CANCELBREW = 39, MAPCOLLECT = 40
        }

        public static readonly string username_key = "username";
        public static readonly string password_key = "password";

        private void Awake()
        {
            _instance = this;
        }

        private void Start()
        {
            RealtimeNetworking.OnPacketReceived += ReceivedPaket;
            RealtimeNetworking.OnDisconnectedFromServer += DisconnectedFromServer;

            string device = SystemInfo.deviceUniqueIdentifier;
            string password = PlayerPrefs.HasKey(password_key) ? PlayerPrefs.GetString(password_key) : "";
            string username = PlayerPrefs.HasKey(username_key) ? PlayerPrefs.GetString(username_key) : "";

            Packet packet = new Packet();
            packet.Write((int)RequestsID.AUTH);
            packet.Write(device);
            packet.Write(password);
            packet.Write(username);
            Sender.TCP_Send(packet);
        }

        private bool connected = false;
        private float timer = 0;
        private bool updating = false;
        private float syncTime = 5;

        private void Update()
        {
            if (connected)
            {
                if (!_inBattle)
                {
                    if (timer <= 0)
                    {
                        if (!updating)
                        {
                            updating = true;
                            timer = syncTime;
                            SendSyncRequest();
                        }
                    }
                    else
                    {
                        timer -= Time.deltaTime;
                    }
                }
                data.nowTime = data.nowTime.AddSeconds(Time.deltaTime);
            }
        }

        private void ReceivedPaket(Packet packet)
        {
            try
            {
                int id = packet.ReadInt();
                long databaseID = 0;
                int response = 0;

                switch ((RequestsID)id)
                {
                    case RequestsID.AUTH:
                        connected = true;
                        updating = true;
                        timer = 0;
                        int bytesLength = packet.ReadInt();
                        byte[] bytes = packet.ReadBytes(bytesLength);
                        string authData = Data.Decompress(bytes);
                        initializationData = Data.Desrialize<Data.InitializationData>(authData);
                        PlayerPrefs.SetString(password_key, initializationData.password);
                        SendSyncRequest();
                        break;

                    case RequestsID.SYNC:
                        string playerData = packet.ReadString();
                        Data.Player playerSyncData = Data.Desrialize<Data.Player>(playerData);
                        SyncData(playerSyncData);
                        updating = false;
                        break;

                    case RequestsID.BUILD:
                        response = packet.ReadInt();
                        switch (response)
                        {
                            case 1:
                                RushSyncRequest();
                                break;
                            case 2:
                                ShowNotEnoughResources();
                                break;
                        }
                        break;

                    case RequestsID.REPLACE:
                        int replaceResponse = packet.ReadInt();
                        int replaceX = packet.ReadInt();
                        int replaceY = packet.ReadInt();
                        long replaceID = packet.ReadLong();

                        for (int i = 0; i < UI_Main.instanse._grid.buildings.Count; i++)
                        {
                            if (UI_Main.instanse._grid.buildings[i].databaseID == replaceID)
                            {
                                if (replaceResponse == 1)
                                {
                                    UI_Main.instanse._grid.buildings[i].PlacedOnGrid(replaceX, replaceY);
                                    RushSyncRequest();
                                }
                                UI_Main.instanse._grid.buildings[i].waitingReplaceResponse = false;
                                break;
                            }
                        }
                        break;

                    case RequestsID.COLLECT:
                        long db = packet.ReadLong();
                        int collected = packet.ReadInt();

                        for (int i = 0; i < UI_Main.instanse._grid.buildings.Count; i++)
                        {
                            if (db == UI_Main.instanse._grid.buildings[i].data.databaseID)
                            {
                                var b = UI_Main.instanse._grid.buildings[i];
                                b.collecting = false;

                                switch (b.id)
                                {
                                    case Data.BuildingID.goldmine:
                                        b.data.goldStorage -= collected;
                                        if (b.data.goldStorage < 0) b.data.goldStorage = 0;
                                        gold += collected;
                                        if (gold > maxGold) gold = maxGold;
                                        break;

                                    case Data.BuildingID.elixirmine:
                                        b.data.elixirStorage -= collected;
                                        if (b.data.elixirStorage < 0) b.data.elixirStorage = 0;
                                        elixir += collected;
                                        if (elixir > maxElixir) elixir = maxElixir;
                                        break;

                                    case Data.BuildingID.darkelixirmine:
                                        b.data.darkStorage -= collected;
                                        if (b.data.darkStorage < 0) b.data.darkStorage = 0;
                                        break;
                                }

                                b.AdjustUI();
                                break;
                            }
                        }

                        RefreshResourcesUI();
                        break;

                    case RequestsID.MAPCOLLECT:
                        int addGold = packet.ReadInt();
                        int addElixir = packet.ReadInt();
                        int addGems = packet.ReadInt();

                        gold += addGold;
                        elixir += addElixir;

                        if (gold > maxGold) gold = maxGold;
                        if (elixir > maxElixir) elixir = maxElixir;

                        if (UI_Main.instanse != null)
                        {
                            UI_Main.instanse._goldText.text = gold + "/" + maxGold;
                            UI_Main.instanse._elixirText.text = elixir + "/" + maxElixir;
                            UI_Main.instanse._gemsText.text = addGems > 0
                                ? (int.Parse(UI_Main.instanse._gemsText.text) + addGems).ToString()
                                : UI_Main.instanse._gemsText.text;
                        }
                        break;

                    case RequestsID.PREUPGRADE:
                        databaseID = packet.ReadLong();
                        string dataString = packet.ReadString();
                        Data.ServerBuilding sb = Data.Desrialize<Data.ServerBuilding>(dataString);
                        UI_BuildingUpgrade.instanse.Open(sb, databaseID);
                        break;

                    case RequestsID.UPGRADE:
                        response = packet.ReadInt();
                        switch (response)
                        {
                            case 1:
                                RushSyncRequest();
                                break;
                            case 2:
                                ShowNotEnoughResources();
                                break;
                        }
                        break;

                    case RequestsID.INSTANTBUILD:
                        response = packet.ReadInt();
                        if (response == 1)
                        {
                            RushSyncRequest();
                        }
                        break;

                    case RequestsID.TRAIN:
                        response = packet.ReadInt();
                        if (response == 2)
                        {
                            ShowNotEnoughResources();
                        }
                        else if (response == 1)
                        {
                            RushSyncRequest();
                        }
                        break;

                    case RequestsID.CANCELTRAIN:
                        response = packet.ReadInt();
                        if (response == 1)
                        {
                            RushSyncRequest();
                        }
                        break;
                }
            }
            catch (System.Exception ex)
            {
                Debug.Log(ex.Message);
            }
        }

        private void ShowNotEnoughResources()
        {
            MessageBox.Open(
                0,
                0.8f,
                false,
                (layoutIndex, buttonIndex) =>
                {
                    if (_msgBoxClosing) return;
                    _msgBoxClosing = true;
                    MessageBox.Close();
                    _msgBoxClosing = false;
                },
                new string[] { "Not enough resources" },
                new string[] { "OK" }
            );
        }

        public void SendSyncRequest()
        {
            Packet p = new Packet();
            p.Write((int)RequestsID.SYNC);
            p.Write(SystemInfo.deviceUniqueIdentifier);
            Sender.TCP_Send(p);
        }

        public void SendMapResources(int gold, int elixir, int gems)
        {
            Packet p = new Packet();
            p.Write((int)RequestsID.MAPCOLLECT);
            p.Write(gold);
            p.Write(elixir);
            p.Write(gems);
            Sender.TCP_Send(p);
        }

        [HideInInspector] public int gold = 0;
        [HideInInspector] public int maxGold = 0;
        [HideInInspector] public int elixir = 0;
        [HideInInspector] public int maxElixir = 0;
        [HideInInspector] public int darkElixir = 0;
        [HideInInspector] public int maxDarkElixir = 0;

        public void SyncData(Data.Player player)
        {
            data = player;

            gold = 0; maxGold = 0;
            elixir = 0; maxElixir = 0;
            darkElixir = 0; maxDarkElixir = 0;

            int gems = player.gems;

            if (player.buildings != null && player.buildings.Count > 0)
            {
                for (int i = 0; i < player.buildings.Count; i++)
                {
                    switch (player.buildings[i].id)
                    {
                        case Data.BuildingID.townhall:
                            maxGold += player.buildings[i].goldCapacity;
                            gold += player.buildings[i].goldStorage;
                            maxElixir += player.buildings[i].elixirCapacity;
                            elixir += player.buildings[i].elixirStorage;
                            maxDarkElixir += player.buildings[i].darkCapacity;
                            darkElixir += player.buildings[i].darkStorage;
                            break;
                        case Data.BuildingID.goldstorage:
                            maxGold += player.buildings[i].goldCapacity;
                            gold += player.buildings[i].goldStorage;
                            break;
                        case Data.BuildingID.elixirstorage:
                            maxElixir += player.buildings[i].elixirCapacity;
                            elixir += player.buildings[i].elixirStorage;
                            break;
                        case Data.BuildingID.darkelixirstorage:
                            maxDarkElixir += player.buildings[i].darkCapacity;
                            darkElixir += player.buildings[i].darkStorage;
                            break;
                    }
                }
            }

            UI_Main.instanse._goldText.text = gold + "/" + maxGold;
            UI_Main.instanse._elixirText.text = elixir + "/" + maxElixir;
            UI_Main.instanse._gemsText.text = gems.ToString();

            UI_Main.instanse._usernameText.text = data.name;
            UI_Main.instanse._trophiesText.text = data.trophies.ToString();
            UI_Main.instanse._levelText.text = data.level.ToString();

            if (UI_Main.instanse.isActive && !UI_WarLayout.instanse.isActive)
            {
                UI_Main.instanse.DataSynced();
            }
            else if (UI_WarLayout.instanse.isActive)
            {
                UI_WarLayout.instanse.DataSynced();
            }
            else if (UI_Train.instanse.isOpen)
            {
                UI_Train.instanse.Sync();
            }
        }

        private void RefreshResourcesUI()
        {
            if (UI_Main.instanse != null)
            {
                UI_Main.instanse._goldText.text = gold + "/" + maxGold;
                UI_Main.instanse._elixirText.text = elixir + "/" + maxElixir;
            }
        }

        public void RushSyncRequest()
        {
            timer = 0;
        }

        private void DisconnectedFromServer()
        {
            ThreadDispatcher.instance.Enqueue(() => Desconnected());
        }

        private void Desconnected()
        {
            connected = false;
            RealtimeNetworking.OnDisconnectedFromServer -= DisconnectedFromServer;
            MessageBox.Open(0, 0.8f, false, MessageResponded, new string[] { "Failed to connect to server. Please check you internet connection and try again." }, new string[] { "Try Again" });
        }

        private void MessageResponded(int layoutIndex, int buttonIndex)
        {
            if (layoutIndex == 0)
            {
                RestartGame();
            }
        }

        public void AssignServerSpell(ref Data.Spell spell)
        {
            if (spell != null)
            {
                for (int i = 0; i < initializationData.serverSpells.Count; i++)
                {
                    if (initializationData.serverSpells[i].id == spell.id && initializationData.serverSpells[i].level == spell.level)
                    {
                        spell.server = initializationData.serverSpells[i];
                        break;
                    }
                }
            }
        }

        public static void RestartGame()
        {
            if (_instance != null)
            {
                RealtimeNetworking.OnDisconnectedFromServer -= _instance.DisconnectedFromServer;
                RealtimeNetworking.OnPacketReceived -= _instance.ReceivedPaket;
            }
            Destroy(RealtimeNetworking.instance.gameObject);
            SceneManager.LoadScene(0);
        }
    }
}
