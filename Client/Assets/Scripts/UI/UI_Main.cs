namespace DevelopersHub.ClashOfWhatecer
{
    using System.Collections;
    using System.Collections.Generic;
    using UnityEngine;
    using TMPro;
    using UnityEngine.UI;
    using Mapbox.Examples;

    public class UI_Main : MonoBehaviour
    {

        [SerializeField] public GameObject _elements = null;
        [SerializeField] public TextMeshProUGUI _goldText = null;
        [SerializeField] public TextMeshProUGUI _elixirText = null;
        [SerializeField] public TextMeshProUGUI _gemsText = null;
        [SerializeField] private Button _shopButton = null;
        [SerializeField] private Button _switchButton = null;
        [SerializeField] private Button _settingsButton = null;
        [SerializeField] public TextMeshProUGUI _usernameText = null;
        [SerializeField] public TextMeshProUGUI _trophiesText = null;
        [SerializeField] public TextMeshProUGUI _levelText = null;

        [SerializeField] public BuildGrid _grid = null;
        [SerializeField] public Building[] _buildingPrefabs = null;

        [Header("Buttons")]
        public Transform buttonsParent = null;
        public UI_Button buttonCollectGold = null;
        public UI_Button buttonCollectElixir = null;
        public UI_Button buttonCollectDarkElixir = null;
        public UI_Bar barBuild = null;


        [SerializeField] bool game_on = false;
        [SerializeField] GameObject gps = null;
        [SerializeField] GameObject game = null;
        private static UI_Main _instance = null; public static UI_Main instanse { get { return _instance; } }

        private bool _active = true; public bool isActive { get { return _active; } }


        [SerializeField] GameObject inRange;
        [SerializeField] GameObject notInRange;
        bool active = false;
        int tempEvent;

        [SerializeField] LocationStatus canva;
        [SerializeField] EventManager eventManager;



        private void Awake()
        {
            _instance = this;
            _elements.SetActive(true);
        }

        private void Start()
        {
            _shopButton.onClick.AddListener(ShopButtonClicked);
            _switchButton.onClick.AddListener(SwitchButtonClicked);
            _settingsButton.onClick.AddListener(SettingsButtonClicked);
        }

        private void SettingsButtonClicked()
        {
            UI_Settings.instanse.Open();
        }


        private void SwitchButtonClicked()
        {
            game_on = !game_on;
            game.SetActive(game_on);
            gps.SetActive(!game_on);
            canva.load();
        }


        private void ShopButtonClicked()
        {
            UI_Shop.instanse.SetStatus(true);
            SetStatus(false);
        }

        private void BattleButtonClicked()
        {
            UI_Search.instanse.SetStatus(true);
            SetStatus(false);
        }

        private void OnLeave()
        {
            UI_Build.instanse.Cancel();
        }

        public void SetStatus(bool status)
        {
            if (!status)
            {
                OnLeave();
            }
            else
            {
                Player.instanse.RushSyncRequest();
            }
            _active = status;
            _elements.SetActive(status);
        }

        public Building GetBuildingPrefab(Data.BuildingID id)
        {
            for (int i = 0; i < _buildingPrefabs.Length; i++)
            {
                if (_buildingPrefabs[i].id == id)
                {
                    return _buildingPrefabs[i];
                }
            }
            return null;
        }

        public void DataSynced()
        {
            if (Player.instanse.data.buildings != null && Player.instanse.data.buildings.Count > 0)
            {
                for (int i = 0; i < Player.instanse.data.buildings.Count; i++)
                {
                    Building building = _grid.GetBuilding(Player.instanse.data.buildings[i].databaseID);
                    if (building != null)
                    {

                    }
                    else
                    {
                        Building prefab = GetBuildingPrefab(Player.instanse.data.buildings[i].id);
                        if (prefab)
                        {
                            building = Instantiate(prefab, Vector3.zero, Quaternion.identity);
                            building.databaseID = Player.instanse.data.buildings[i].databaseID;
                            building.PlacedOnGrid(Player.instanse.data.buildings[i].x, Player.instanse.data.buildings[i].y);
                            building._baseArea.gameObject.SetActive(false);

                            _grid.buildings.Add(building);
                        }
                    }

                    if (building.buildBar == null)
                    {
                        building.buildBar = Instantiate(barBuild, buttonsParent);
                        building.buildBar.gameObject.SetActive(false);
                    }

                    building.data = Player.instanse.data.buildings[i];
                    switch (building.id)
                    {
                        case Data.BuildingID.goldmine:
                            if (building.collectButton == null)
                            {
                                building.collectButton = Instantiate(buttonCollectGold, buttonsParent);
                                building.collectButton.button.onClick.AddListener(building.Collect);
                                building.collectButton.gameObject.SetActive(false);
                            }
                            break;
                        case Data.BuildingID.elixirmine:
                            if (building.collectButton == null)
                            {
                                building.collectButton = Instantiate(buttonCollectElixir, buttonsParent);
                                building.collectButton.button.onClick.AddListener(building.Collect);
                                building.collectButton.gameObject.SetActive(false);
                            }
                            break;
                        case Data.BuildingID.darkelixirmine:
                            if (building.collectButton == null)
                            {
                                building.collectButton = Instantiate(buttonCollectDarkElixir, buttonsParent);
                                building.collectButton.button.onClick.AddListener(building.Collect);
                                building.collectButton.gameObject.SetActive(false);
                            }
                            break;
                    }
                    building.AdjustUI();
                }
            }
        }
        public void DisplayInRangeEvent(int id)
        {
            if (!active)
            {   
                Debug.Log("Displaying in-range event for ID: " + id);
                tempEvent = id;
                inRange.SetActive(true);
                active = true;
            }
        }

        public void DisplayNotInRange()
        {
            if (!active)
            {
                notInRange.SetActive(true);
                active = true;
            }
        }
        
        public void JoinButtonClick()
        {   
            Debug.Log("Joining Event: " + tempEvent);
            eventManager.ActivateEvent(tempEvent);
            active = false;
            inRange.SetActive(false);
        }

        public void CloseButtonClick()
        {   
            inRange.SetActive(false);
            notInRange.SetActive(false);
            active = false;
        }

    }
}