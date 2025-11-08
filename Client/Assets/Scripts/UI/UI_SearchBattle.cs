namespace DevelopersHub.ClashOfWhatecer
{
    using DevelopersHub.RealtimeNetworking.Client;
    using System.Collections;
    using System.Collections.Generic;
    using UnityEngine;
    using UnityEngine.UI;
using UnityEngine;
using UnityEngine.SceneManagement;

    public class UI_SearchBattle : MonoBehaviour
    {

        [SerializeField] private GameObject _elements = null;
        [SerializeField] private Button _closeButton = null;
        [SerializeField] private Button _findButton = null;
        [SerializeField] GameObject gps = null;
        [SerializeField] GameObject game = null;

        private static UI_SearchBattle _instance = null; public static UI_SearchBattle instanse { get { return _instance; } }
        private bool _active = true; public bool isActive { get { return _active; } }
        private long lastTarget = 0;

        private void Awake()
        {
            _instance = this;
            _elements.SetActive(false);
        }

        private void Start()
        {
            _closeButton.onClick.AddListener(Close);
            _findButton.onClick.AddListener(Find);
        }

        public void SetStatus(bool status)
        {
            if (status)
            {
                lastTarget = 0;
            }
            _active = status;
            _elements.SetActive(status);
        }

        private void Close()
        {
            SetStatus(false);
            UI_Main.instanse.SetStatus(true);
        }

        public void Find()
        {
            _active = false;
            _elements.SetActive(false);
            game.SetActive(true);
            gps.SetActive(false);
           // SceneManager.LoadScene("GPSScene");
        }


        public void FindResponded(long target, Data.OpponentData opponent)
        {
            if(target > 0 && opponent != null && target != lastTarget)
            {
                SetStatus(false);
                bool attack = UI_Battle.instanse.Display(opponent.buildings, target, Data.BattleType.normal);
                if (attack)
                {
                    lastTarget = target;
                }
                else
                {
                    UI_Main.instanse.SetStatus(true);
                }
            }
            else
            {
                UI_Battle.instanse.NoTarget();
                Debug.Log("No target found.");
            }
        }

    }
}